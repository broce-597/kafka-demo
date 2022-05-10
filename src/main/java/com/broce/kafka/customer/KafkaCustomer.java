package com.broce.kafka.customer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import java.util.Objects;

/**
 * @author Blade
 * @since 2021/6/16 9:29
 */
@Slf4j
public class KafkaCustomer {
    @KafkaListener(topics = {"topic"})
    private void listen(ConsumerRecord<?, String> record) {
        log.info("kafka—customer");
        try {
            String value = record.value();
            if (Objects.isNull(value)) {
                return;
            }
        //Json解析
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
