package simple;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

public class BasicConsumerExample {

    private static final Logger logger = LoggerFactory.getLogger(BasicProducerExample.class);

    public static void main(String[] args) {

        String topic = "Country";

        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.deserializer", StringDeserializer.class);
        kafkaProps.put("value.deserializer", StringDeserializer.class);
        kafkaProps.put("group.id", "countries");

        KafkaConsumer<String,String> consumers = new KafkaConsumer<>(kafkaProps);
        consumers.subscribe(Collections.singletonList(topic));

        while (true) {
            for (ConsumerRecord<String,String> records : consumers.poll(1000)) {
                System.out.println(String.format("Received Message topic =%s, partition =%s, offset = %d, key = %s, value = %s",
                        records.topic(), records.partition(), records.offset(), records.key(), records.value()));
            }

        }

    }
}
