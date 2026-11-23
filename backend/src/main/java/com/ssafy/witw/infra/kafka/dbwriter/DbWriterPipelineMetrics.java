package com.ssafy.witw.infra.kafka.dbwriter;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;
import java.time.Duration;
import java.time.OffsetDateTime;

@Component
@Profile("prod")
public class DbWriterPipelineMetrics {
    private final MeterRegistry registry;
    private final AtomicLong lastApplied = new AtomicLong();
    private final AtomicLong pending = new AtomicLong();
    private final AtomicLong oldestPendingAge = new AtomicLong();
    private final AtomicLong snapshotTimestamp = new AtomicLong();
    private final Timer articleDelivery;

    public DbWriterPipelineMetrics(MeterRegistry registry) {
        this.registry = registry;
        articleDelivery = Timer.builder("witw.dbwriter.article.delivery")
                .description("Time from collector discovery to committed article persistence")
                .serviceLevelObjectives(Duration.ofMinutes(1), Duration.ofMinutes(5),
                        Duration.ofMinutes(15), Duration.ofHours(1), Duration.ofDays(1))
                .register(registry);
        Gauge.builder("witw.dbwriter.last.applied.timestamp.seconds", lastApplied, AtomicLong::get)
                .register(registry);
        Gauge.builder("witw.dbwriter.pending.events", pending, AtomicLong::get).register(registry);
        Gauge.builder("witw.dbwriter.pending.oldest.age.seconds", oldestPendingAge, AtomicLong::get)
                .register(registry);
        Gauge.builder("witw.dbwriter.pending.snapshot.timestamp.seconds", snapshotTimestamp, AtomicLong::get)
                .register(registry);
    }

    public void record(String outcome) {
        registry.counter("witw.dbwriter.events", "outcome", outcome).increment();
        if ("applied".equals(outcome)) lastApplied.set(System.currentTimeMillis() / 1000);
    }

    public void pendingSnapshot(long count, long oldestAge) {
        pending.set(count);
        oldestPendingAge.set(oldestAge);
        snapshotTimestamp.set(System.currentTimeMillis() / 1000);
    }

    public void articleApplied(DbWriterEvent event) {
        if ("ARTICLE_UPSERT".equals(event.eventType())) {
            Duration delay = Duration.between(OffsetDateTime.parse(event.payload().get("crawled_at").asText()),
                    OffsetDateTime.now());
            articleDelivery.record(delay.isNegative() ? Duration.ZERO : delay);
        }
    }
}
