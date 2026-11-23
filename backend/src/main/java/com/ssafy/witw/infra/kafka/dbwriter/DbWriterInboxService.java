package com.ssafy.witw.infra.kafka.dbwriter;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;

@Service
@RequiredArgsConstructor
public class DbWriterInboxService {
    public enum Result { APPLIED, DUPLICATE, DEFERRED }

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final DbWriterService dbWriterService;

    @Transactional
    public Result receive(DbWriterEvent event) {
        if (event == null || event.eventType() == null || event.occurredAt() == null
                || event.payload() == null) {
            throw new IllegalArgumentException("Invalid pipeline event envelope");
        }
        String json = objectMapper.writeValueAsString(event);
        String id = event.eventId();
        if (id == null) {
            // Legacy producers remain accepted; explicit producer IDs are preferred.
            id = legacyId(event.eventType() + ":" + objectMapper.writeValueAsString(event.payload()));
        } else if (id.isBlank() || id.length() > 255) {
            throw new IllegalArgumentException("Invalid pipeline event_id");
        }
        jdbcTemplate.update("""
                INSERT INTO pipeline_event_inbox(event_id, event_type, event_json)
                VALUES (?, ?, ?) ON CONFLICT (event_id) DO NOTHING
                """, id, event.eventType(), json);
        return applyLocked(id);
    }

    public List<String> pendingEventIds() {
        return jdbcTemplate.queryForList("""
                SELECT event_id FROM pipeline_event_inbox WHERE status = 'PENDING'
                ORDER BY last_attempt_at NULLS FIRST, received_at, event_id LIMIT 100
                """, String.class);
    }

    @Transactional
    public Result retryPending(String id) {
        return applyLocked(id);
    }

    private Result applyLocked(String id) {
        // SKIP LOCKED also lets multiple backend instances retry safely.
        var rows = jdbcTemplate.query("""
                SELECT status, event_json FROM pipeline_event_inbox
                WHERE event_id = ? FOR UPDATE SKIP LOCKED
                """, (rs, row) -> new String[]{rs.getString("status"), rs.getString("event_json")}, id);
        if (rows.isEmpty()) return Result.DEFERRED;
        if ("APPLIED".equals(rows.getFirst()[0])) return Result.DUPLICATE;
        DbWriterEvent stored = objectMapper.readValue(rows.getFirst()[1], DbWriterEvent.class);
        try {
            dbWriterService.handle(stored);
        } catch (MissingEventDependencyException exception) {
            jdbcTemplate.update("UPDATE pipeline_event_inbox SET last_attempt_at = CURRENT_TIMESTAMP "
                    + "WHERE event_id = ?", id);
            return Result.DEFERRED;
        }
        jdbcTemplate.update("""
                UPDATE pipeline_event_inbox SET status = 'APPLIED', applied_at = CURRENT_TIMESTAMP,
                    event_json = '{}'
                WHERE event_id = ?
                """, id);
        return Result.APPLIED;
    }

    private static String legacyId(String json) {
        try {
            return "legacy:" + HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(json.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException(exception);
        }
    }
}
