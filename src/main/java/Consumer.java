
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
public class Consumer {

    public static void main(String[] args) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.18.0.69:9092");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "mytest");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(p);
        kafkaConsumer.subscribe(Collections.singletonList(Producer.topic));// 订阅消息

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll( Duration.ofMillis(1000) );
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(String.format("topic:%s,offset:%d,消息:%s", //
                        record.topic(), record.offset(), record.value()));
            }
        }
    }
}
