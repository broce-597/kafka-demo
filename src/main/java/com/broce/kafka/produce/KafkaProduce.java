package com.broce.kafka.produce;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.HashMap;
import java.util.Properties;

/**
 * @author broce
 * @since 2021/6/16 9:24
 */


@Component
@Slf4j
@RefreshScope
public class KafkaProduce {

    @Value("${spring.kafka.bootstrap-servers}")
    private String host;

    @Autowired
    private KafkaTemplate kafkaTemplate;

    /**
     * @Description 发送消息
     * @Date 2020/10/20 16:57
     * @Param [topic, key, msg]
     * @Author  hcb
     * @Return int
     **/
    public int sendMsg(String topic, String key,JSONObject jsonObject){
        HashMap<String,Integer> map = new HashMap();
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, host);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(p);

        try {
            ProducerRecord<String,String> record = new ProducerRecord<String, String>(topic,key,jsonObject.toJSONString());
            // 异步回调
            kafkaProducer.send(record, (recordMetadata, e) -> {
                if (e != null) {
                    map.put("code",0);
                    log.info(e.getMessage());
                    log.info("消息发送失败");
                } else {
                    map.put("code",1);
                    log.info("消息发送成功");
                }
            });
        } finally {
            kafkaProducer.close();
        }
        return map.get("code");
    }

    public int sendMsg(String topic,JSONObject jsonObject){
        HashMap<String,Integer> map = new HashMap();
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, host);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(p);

        try {
            ProducerRecord<String,String> record = new ProducerRecord<String, String>(topic,jsonObject.toJSONString());
            // 异步回调
            kafkaProducer.send(record, (recordMetadata, e) -> {
                if (e != null) {
                    map.put("code",0);
                    log.info(e.getMessage());
                    log.info("消息发送失败");
                } else {
                    map.put("code",1);
                    log.info("消息发送成功");
                }
            });
        } finally {
            kafkaProducer.close();
        }
        return map.get("code");
    }


    public void sendMsgTemplate(String topic, String key,JSONObject jsonObject) {

        ListenableFuture t = kafkaTemplate.send(topic, key, jsonObject);

    }

}
