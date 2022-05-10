package com.broce.kafka.produce;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Properties;

/**
 * @author broce
 * @since 2021/6/16 9:24
 */


@Component
@Slf4j
public class KafkaProduce {

    @Value("${spring.kafka.bootstrap-servers}")
    private String host;

    /**
     * @Param [topic, key, msg]
     * @Author broce
     * @Return int
     **/
    public int sendMsg(String topic, String key, JSONObject jsonObject) {
        HashMap<String, Integer> map = new HashMap();
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, host);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        //acks=0如果设置为0，那么生产者将不会等待任何来自服务器的确认。记录将立即添加到套接字缓冲区，并认为已发送。在这种情况下，不能保证服务器已经收到了记录，重试配置将不会生效(因为客户端通常不会知道任何失败)。为每条记录返回的偏移量将始终设置为-1。
        //acks=1这将意味着leader将记录写入其本地日志，但将在没有等待所有follower完全确认的情况下进行响应。在这种情况下，如果leader在确认记录后立即失败，但在follower复制它之前，那么记录将丢失。
        //acks=all表示leader将等待所有同步副本确认该记录。这可以保证只要至少有一个同步副本仍然存在，记录就不会丢失。这是最有力的保证。这相当于acks=-1设置。
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(p);

        try {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, jsonObject.toJSONString());
            // 异步回调
            kafkaProducer.send(record, (recordMetadata, e) -> {
                if (e != null) {
                    map.put("code", 0);
                    log.info(e.getMessage());
                    log.info("消息发送失败");
                } else {
                    map.put("code", 1);
                    log.info("消息发送成功");
                }
            });
        } finally {
            kafkaProducer.close();
        }
        return map.get("code");
    }

    public int sendMsg(String topic, JSONObject jsonObject) {
        HashMap<String, Integer> map = new HashMap();
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, host);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(p);

        try {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, jsonObject.toJSONString());
            // 异步回调
            kafkaProducer.send(record, (recordMetadata, e) -> {
                if (e != null) {
                    map.put("code", 0);
                    log.info(e.getMessage());
                    log.info("消息发送失败");
                } else {
                    map.put("code", 1);
                    log.info("消息发送成功");
                }
            });
        } finally {
            kafkaProducer.close();
        }
        return map.get("code");
    }


}
