package kafka.producer;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.Future;

public class SimpleProducer<K, V> {

    Logger logger = LoggerFactory.getLogger(SimpleProducer.class);

    private KafkaProducer<byte[], byte[]> producer;

    private boolean isAsync;

    private volatile boolean shutDown = false;

    public SimpleProducer(Properties properties) {
        this (properties, true);
    }

    public SimpleProducer(Properties properties, boolean isAsync) {
        this.isAsync = isAsync;
        this.producer = new KafkaProducer<byte[], byte[]>(properties);
        logger.info("Started kafka.producer! isAsync :{}",isAsync);
    }

    public void send(String topic, V value) {
        send(topic, -1,null, value, new ProducerCallBack());
    }

    public void send(String topic, K key, V value) {
        send(topic, -1,key, value, new ProducerCallBack());
    }

    public void send(String topic, int partition, V value) {
        send(topic, partition, null, value, new ProducerCallBack());
    }

    public void send(String topic, int partition, K key, V value) {
        send(topic, partition, key, value, new ProducerCallBack());
    }

    public void send(String topic, int partition, K key, V value, Callback callback) {

        if (BooleanUtils.isTrue(shutDown)) {
            throw new RuntimeException("Producer is closed!");
        }

        ProducerRecord record;

        try {
            if (partition < 0) {
                record = new ProducerRecord(topic, key, value);
            } else {
                record = new ProducerRecord(topic, partition, key, value);
            }

            if (BooleanUtils.isTrue(isAsync)) {
                producer.send(record, callback);
            } else {
                Future<RecordMetadata> future = producer.send(record);
                future.get();
            }
        } catch (Exception ex) {
            logger.error("Error while producing event to topic {}", topic, ex);
        }
    }

    private class ProducerCallBack implements Callback {

        public void onCompletion(RecordMetadata record, Exception ex) {
            if (ex != null) {
                logger.error("Error while producing to topic : {}", record.topic(), ex);
            } else {
                logger.debug("sent message to topic: {}, partition: {}, offset: {}",
                        record.topic(), record.partition(), record.offset());
            }
        }
    }

    public void close() {
        shutDown = true;
        try {
            producer.close();
        } catch (Exception ex) {
            logger.error("Exception occured while stoppping the kafka.producer", ex);
        }
    }

}