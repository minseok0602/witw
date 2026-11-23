package com.ssafy.witw.infra.kafka.dbwriter;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

@Slf4j
@Component
@Profile("prod")
@RequiredArgsConstructor
public class DbWriterKafkaConsumer {

    private final ObjectMapper objectMapper;
    private final DbWriterInboxService inbox;
    private final DbWriterPipelineMetrics metrics;

    // groupId는 프로파일별 application-*.yml의 spring.kafka.consumer.group-id를 따른다.
    // 애너테이션 속성은 yml을 덮어쓰므로, 여기에 하드코딩하면 환경별 분리가 불가능하다.
    @KafkaListener(topics = "news.event.v1")
    public void consume(ConsumerRecord<String, String> record) {
        try {
            DbWriterEvent event = deserialize(record.value());
            var result = inbox.receive(event);
            metrics.record(result.name().toLowerCase(java.util.Locale.ROOT));
            if (result == DbWriterInboxService.Result.APPLIED) metrics.articleApplied(event);
            if (result == DbWriterInboxService.Result.DEFERRED) {
                log.warn("Pipeline event waiting for dependencies: topic={} partition={} offset={} eventId={}",
                        record.topic(), record.partition(), record.offset(), event.eventId());
            }
        } catch (RuntimeException exception) {
            metrics.record("failed");
            throw exception;
        }
    }

    private DbWriterEvent deserialize(String value) {
        try {
            return objectMapper.readValue(value, DbWriterEvent.class);
        } catch (JacksonException exception) {
            throw new IllegalArgumentException("Invalid news.event.v1 message", exception);
        }
    }
}
