package kafka.producer;


import com.google.common.collect.Maps;
import kafka.serializations.KryoObjectSerializer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class AcqKafkaProducer implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(AcqKafkaProducer.class);

    private static final String TM_PRODUCER_CLIENT = "TM-kafka.producer-";
    private static final int RETENTION_24_HOURS_MS = 1000 * 60 * 60 * 24;
    private static final AtomicInteger ID = new AtomicInteger();

    private final String kafkaServer;
    private final Class<? extends Partitioner> partitioner;
    private KafkaProducer<String, Object> producer;

    private static KryoObjectSerializer objectSerializer = new KryoObjectSerializer();

    public AcqKafkaProducer(String kafkaServer, Class<? extends Partitioner> partitioner) {
        this.partitioner = partitioner;
        this.kafkaServer = kafkaServer;
        start(configurate());
    }

    private Properties configurate() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaServer);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG,TM_PRODUCER_CLIENT+ID.getAndDecrement());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.RETRIES_CONFIG, 5);
        if (Objects.nonNull(partitioner)) {
            properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, partitioner);
        }
        return properties;
    }

    private void start(Properties properties) {
        producer = new KafkaProducer<>(properties);
    }

    public Future<RecordMetadata> send(String topic, int partition, Object value) {
        return send(topic, partition, null, value);
    }

    public Future<RecordMetadata> send(String topic, String key, Object value) {
        return send(topic, -1, key, value);
    }

    public Future<RecordMetadata> send(String topic, int partition, String key, Object data) {
        try {
            final byte[] value = data instanceof byte[] ? (byte[]) data : objectSerializer.serialize(data);

            ProducerRecord producerRecord;

            if (partition <= 0) {
                producerRecord = new ProducerRecord<>(topic, key, value);
            } else {
                producerRecord = new ProducerRecord<>(topic, partition, key, value);
            }
            return producer.send(producerRecord);
        } catch (RuntimeException ex) {
            logger.error(String.format("Error send to topic [%s]", topic), ex);
            throw new RuntimeException("Error send kafka messages");
        }
    }

    public Map<String, String> configTopic() {
        Map<String, String> configs = Maps.newHashMap();
        configs.put(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(RETENTION_24_HOURS_MS));
        return configs;
    }

    public Map<String, String> configTopicWithCompressed() {
        Map<String, String> configs = Maps.newHashMap();
        configs.put(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(RETENTION_24_HOURS_MS));
        configs.put(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        return configs;
    }

    public void createTopic(Map<String, String> configs, String topic, int partitionCnt, int replicationFactory) {
        try (AdminClient client = createAdminClient()) {
            final NewTopic newTopic = new NewTopic(topic, partitionCnt, (short) replicationFactory);
            CreateTopicsResult topicsResult = client.createTopics(Collections.singletonList(newTopic.configs(configs)));
            try {
                topicsResult.all().get();
            } catch (Exception ex) {
                final Throwable cause = ex.getCause();
                if (cause instanceof TopicExistsException) {
                    throw (TopicExistsException) cause;
                }
                String errorMessage = String.format("Error create topic = %s", topic);
                logger.error(errorMessage, ex);
                throw new RuntimeException(errorMessage, ex);
            }
        }
    }

    private AdminClient createAdminClient() {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        return AdminClient.create(config);
    }

    public void close() {
        producer.flush();
        producer.close();
    }

    public void flush() {
        producer.flush();
    }

}