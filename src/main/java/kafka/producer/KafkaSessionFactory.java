package kafka.producer;

import kafka.consumer.AcqKafkaConsumer;
import org.apache.kafka.clients.producer.Partitioner;

public class KafkaSessionFactory {

    final static String kafkaServer = "localhost:9092";

    public AcqKafkaProducer createProducer() {
        return new AcqKafkaProducer(kafkaServer, null);
    }

    public AcqKafkaProducer createProducer(String kafkaServer) {

        return new AcqKafkaProducer(kafkaServer, null);
    }

    public AcqKafkaProducer createProducer(Class<? extends Partitioner> partitioner) {
        return new AcqKafkaProducer(kafkaServer, partitioner);
    }

    public AcqKafkaConsumer createConsumer() {
        return new AcqKafkaConsumer(kafkaServer);
    }

    public AcqKafkaConsumer createConsumer(String kafkaServer) {
        return new AcqKafkaConsumer(kafkaServer);
    }
}