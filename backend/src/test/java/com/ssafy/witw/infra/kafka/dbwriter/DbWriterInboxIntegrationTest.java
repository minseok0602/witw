package com.ssafy.witw.infra.kafka.dbwriter;

import com.ssafy.witw.support.TestContainerSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import tools.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

@SpringBootTest
@ActiveProfiles("test")
class DbWriterInboxIntegrationTest extends TestContainerSupport {
    @Autowired DbWriterInboxService inbox;
    @Autowired JdbcTemplate jdbc;
    @Autowired ObjectMapper mapper;
    @MockitoBean DbWriterService writer;

    @BeforeEach
    void clearInbox() {
        jdbc.execute("TRUNCATE pipeline_event_inbox");
    }

    private DbWriterEvent event(String id) {
        return new DbWriterEvent(id, "ISSUE_CREATE", java.time.OffsetDateTime.now(),
                mapper.createObjectNode().put("theme_id", 1));
    }

    @Test
    void reorderedEventIsDurableAndAppliedAfterDependenciesArrive() {
        var event = event("reordered");
        doThrow(new MissingEventDependencyException("article not stored yet"))
                .doNothing().when(writer).handle(event);
        assertThat(inbox.receive(event)).isEqualTo(DbWriterInboxService.Result.DEFERRED);
        assertThat(inbox.pendingEventIds()).containsExactly("reordered");
        assertThat(inbox.retryPending("reordered")).isEqualTo(DbWriterInboxService.Result.APPLIED);
        assertThat(inbox.pendingEventIds()).isEmpty();
        assertThat(inbox.receive(event)).isEqualTo(DbWriterInboxService.Result.DUPLICATE);
        verify(writer, times(2)).handle(event);
    }

    @Test
    void retryWithNewKafkaTimeOrGeneratedNameDoesNotReapplySameEventId() {
        assertThat(inbox.receive(event("stable-cluster"))).isEqualTo(DbWriterInboxService.Result.APPLIED);
        assertThat(inbox.receive(event("stable-cluster"))).isEqualTo(DbWriterInboxService.Result.DUPLICATE);
        verify(writer, times(1)).handle(any());
    }

    @Test
    void domainFailureRollsBackReceiptSoKafkaCanRetry() {
        var event = event("rollback");
        doThrow(new IllegalStateException("write failed")).when(writer).handle(event);
        assertThatThrownBy(() -> inbox.receive(event)).isInstanceOf(IllegalStateException.class);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM pipeline_event_inbox", Long.class)).isZero();
        doNothing().when(writer).handle(event);
        assertThat(inbox.receive(event)).isEqualTo(DbWriterInboxService.Result.APPLIED);
    }

    @Test
    void concurrentDeliveryAppliesTheDomainChangeOnlyOnce() throws Exception {
        var event = event("concurrent");
        var start = new java.util.concurrent.CountDownLatch(1);
        try (var workers = java.util.concurrent.Executors.newFixedThreadPool(2)) {
            java.util.concurrent.Callable<DbWriterInboxService.Result> receive = () -> {
                start.await();
                return inbox.receive(event);
            };
            var first = workers.submit(receive);
            var second = workers.submit(receive);
            start.countDown();
            assertThat(java.util.List.of(first.get(10, java.util.concurrent.TimeUnit.SECONDS),
                    second.get(10, java.util.concurrent.TimeUnit.SECONDS)))
                    .containsExactlyInAnyOrder(DbWriterInboxService.Result.APPLIED,
                            DbWriterInboxService.Result.DUPLICATE);
        }
        verify(writer, times(1)).handle(event);
    }

    @Test
    void legacyReplayIgnoresRetryTimeForIdenticalPayload() {
        var original = event("unused");
        var first = new DbWriterEvent(original.eventType(), original.occurredAt(), original.payload());
        var replay = new DbWriterEvent(original.eventType(), original.occurredAt().plusDays(1), original.payload());
        assertThat(inbox.receive(first)).isEqualTo(DbWriterInboxService.Result.APPLIED);
        assertThat(inbox.receive(replay)).isEqualTo(DbWriterInboxService.Result.DUPLICATE);
        verify(writer, times(1)).handle(first);
    }
}
