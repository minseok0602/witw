package com.ssafy.witw.infra.kafka.dbwriter;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Profile("prod")
@RequiredArgsConstructor
public class DbWriterPendingScheduler {
    private final DbWriterInboxService inbox;
    private final DbWriterPipelineMetrics metrics;
    private final JdbcTemplate jdbcTemplate;

    @Scheduled(fixedDelayString = "${pipeline.dbwriter.pending-delay-ms:5000}")
    public void retryPending() {
        for (String id : inbox.pendingEventIds()) {
            try {
                var result = inbox.retryPending(id);
                if (result == DbWriterInboxService.Result.APPLIED) metrics.record("applied");
            } catch (Exception exception) {
                metrics.record("pending_retry_failed");
                log.error("Deferred pipeline event failed: eventId={}", id, exception);
            }
        }
        jdbcTemplate.query("""
                SELECT COUNT(*) AS pending,
                       COALESCE(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP - MIN(received_at)), 0) AS age
                FROM pipeline_event_inbox WHERE status = 'PENDING'
                """, rs -> {
            metrics.pendingSnapshot(rs.getLong("pending"), Math.max(0, rs.getLong("age")));
        });
    }
}
