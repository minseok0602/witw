package com.ssafy.witw.infra.kafka.dbwriter;

import com.ssafy.witw.support.TestContainerSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import tools.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
@ActiveProfiles("test")
class DbWriterOrderingIntegrationTest extends TestContainerSupport {
    @Autowired DbWriterInboxService inbox;
    @Autowired JdbcTemplate jdbc;
    @Autowired ObjectMapper mapper;

    @BeforeEach
    void prepareData() {
        jdbc.update("DELETE FROM pipeline_event_inbox WHERE event_id LIKE 'ordering-%'");
        jdbc.update("DELETE FROM article WHERE id = 'ordering-article'");
        jdbc.update("DELETE FROM issue WHERE name = 'Pipeline ordering test'");
        jdbc.update("INSERT INTO field(id, code, name, description) VALUES (900001, 'PIPELINE', "
                + "'Pipeline ordering test', 'test') ON CONFLICT (id) DO NOTHING");
        jdbc.update("INSERT INTO theme(id, field_id, code, name, description) VALUES (900001, 900001, "
                + "'PIPELINE', 'Pipeline ordering test', 'test') ON CONFLICT (id) DO NOTHING");
    }

    private DbWriterEvent article(int tone) {
        return mapper.readValue("""
                {"event_id":"ordering-article","event_type":"ARTICLE_UPSERT",
                 "occurred_at":"2026-09-03T06:00:00Z","payload":{
                 "article_id":"ordering-article","publisher_country_code":"KR","theme_id":900001,
                 "theme_relevance_score":0.9,"publisher":"test","url":"https://example.com/article",
                 "original_title":"test","crawled_at":"2026-09-03T05:00:00Z",
                 "tone_score":%d,"published_at":"2026-09-03T04:00:00Z"}}
                """.formatted(tone), DbWriterEvent.class);
    }

    @Test
    void issueBeforeArticleIsAppliedLaterWithoutPartialIssueOrDuplicateAssignment() {
        var event = mapper.readValue("""
                {"event_id":"ordering-cluster","event_type":"ISSUE_CREATE",
                 "occurred_at":"2026-09-03T06:10:00Z","payload":{
                 "theme_id":900001,"name":"Pipeline ordering test","embedding":[1,0],
                 "article_ids":["ordering-article"],"first_seen_at":"2026-09-03T04:00:00Z"}}
                """, DbWriterEvent.class);
        assertThat(inbox.receive(event)).isEqualTo(DbWriterInboxService.Result.DEFERRED);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM issue WHERE name = 'Pipeline ordering test'",
                Long.class)).isZero();
        assertThat(inbox.receive(article(50))).isEqualTo(DbWriterInboxService.Result.APPLIED);
        assertThat(inbox.retryPending("ordering-cluster")).isEqualTo(DbWriterInboxService.Result.APPLIED);
        var assignedAt = jdbc.queryForObject("SELECT issue_assigned_at FROM article WHERE id = 'ordering-article'",
                java.sql.Timestamp.class);
        assertThat(assignedAt).isNotNull();
        var replay = new DbWriterEvent(event.eventId(), event.eventType(), event.occurredAt().plusDays(1),
                event.payload());
        assertThat(inbox.receive(replay)).isEqualTo(DbWriterInboxService.Result.DUPLICATE);
        assertThat(jdbc.queryForObject("SELECT issue_assigned_at FROM article WHERE id = 'ordering-article'",
                java.sql.Timestamp.class)).isEqualTo(assignedAt);
    }

    @Test
    void invalidDomainPayloadDoesNotCommitInboxOrArticle() {
        assertThatThrownBy(() -> inbox.receive(article(101))).isInstanceOf(IllegalArgumentException.class);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM pipeline_event_inbox "
                + "WHERE event_id = 'ordering-article'", Long.class)).isZero();
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM article WHERE id = 'ordering-article'",
                Long.class)).isZero();
        assertThat(inbox.receive(article(50))).isEqualTo(DbWriterInboxService.Result.APPLIED);
    }
}
