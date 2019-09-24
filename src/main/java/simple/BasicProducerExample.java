package simple;

import javafx.util.Pair;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

public class BasicProducerExample {

    private static final Logger logger = LoggerFactory.getLogger(BasicProducerExample.class);

    public static void main(String[] args) {

        Map<String,String> countries = new HashMap<>();
        countries.put("France", "PSG");
        countries.put("Russia", "Spartak");
        countries.put("Italia", "Inter");
        countries.put("Austria", "Sturm");
        String topic = "ChampionsLeague";

        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", StringSerializer.class);
        kafkaProps.put("value.serializer", StringSerializer.class);
        kafkaProps.put("acks", "all");
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(kafkaProps);

        for(Map.Entry<String,String> elem : countries.entrySet()) {
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>("Country",elem.getKey(), elem.getValue()));
            try {
                future.get();
            } catch (Exception e) {
                System.out.println(String.format("Error while producing event for topic : {}", topic, e));
            }
        }




    }
}
