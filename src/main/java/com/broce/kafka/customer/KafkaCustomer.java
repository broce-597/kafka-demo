package com.broce.kafka.customer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springblade.core.tool.utils.StringUtil;

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
            if (StringUtil.isEmpty(value)) {
                return;
            }
        //Json解析
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
