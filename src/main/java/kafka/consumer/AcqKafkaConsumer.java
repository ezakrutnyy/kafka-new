package kafka.consumer;

import com.google.common.collect.Lists;
import kafka.serializations.KryoObjectSerializer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class AcqKafkaConsumer implements AutoCloseable{

    private static final Logger logger = LoggerFactory.getLogger(AcqKafkaConsumer.class);

    private static final String ASSIGNED_CONSUMER_GROUP_ID = "TM-AssignedConsumer";
    private static final String TM_CONSUMER_CLIENT = "TM-consumer-";
    private static final AtomicInteger ID = new AtomicInteger();

    private final String kafkaServer;
    private KafkaConsumer<String, byte[]> consumer;

    private static KryoObjectSerializer objectSerializer = new KryoObjectSerializer();

    public AcqKafkaConsumer(String kafkaServer) {
        this.kafkaServer = kafkaServer;
        start(configurate());
    }

    private Properties configurate() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaServer);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG,TM_CONSUMER_CLIENT+ID.getAndDecrement());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, ASSIGNED_CONSUMER_GROUP_ID);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }

    private void start(Properties properties) {
        consumer = new KafkaConsumer<>(properties);
    }

    public <T> List<T> getMessages(String topic, int partition, Class<T> itemClass) {
        logger.debug("Start read topic = {}, partition = {}", topic, partition);

        List<T> messages = Lists.newArrayList();
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        List<TopicPartition> partitionList = Collections.singletonList(topicPartition);

        checkAssignment();
        try {
            consumer.assign(partitionList);
            consumer.seekToBeginning(partitionList);
            long currentOffset, endOffset;
            do {
                final ConsumerRecords<String, byte[]> records = consumer.poll(1000);
                records.forEach(record -> {
                    final T dataObj = (itemClass == byte[].class)
                            ? (T) record.value() : objectSerializer.deserialize(record.value(), itemClass);
                    messages.add(dataObj);
                });
                endOffset = consumer.endOffsets(partitionList).get(topicPartition);
                currentOffset = consumer.position(topicPartition);
            } while(currentOffset < endOffset);
        } catch (Exception ex) {
            logger.error(String.format("Error occured while read messages from topic[%s], partition[%s]", topic,partition),ex);
        } finally {
            consumer.unsubscribe();
        }
        logger.debug("Read done, count={}", messages.size());
        return messages;
    }

    public <T> List<T> getMessages(String topic, Class<T> itemClass) {
        logger.debug("Start read topic = {}", topic);
        List<T> messages = Lists.newArrayList();
        for (PartitionInfo partitionInfo : consumer.partitionsFor(topic)) {
            TopicPartition partition = new TopicPartition(topic, partitionInfo.partition());
            List<TopicPartition> partitions = Collections.singletonList(partition);
            try {
                consumer.assign(partitions);
                consumer.seekToBeginning(partitions);
                long currentOffset, endOffset;
                do {
                    final ConsumerRecords<String, byte[]> records = consumer.poll(1000);
                    records.forEach(record -> {
                        final T dataObj = (itemClass == byte[].class)
                                ? (T) record.value() : objectSerializer.deserialize(record.value(), itemClass);
                        messages.add(dataObj);
                    });
                    endOffset = consumer.endOffsets(partitions).get(partition);
                    currentOffset = consumer.position(partition);
                } while (currentOffset < endOffset);
            } catch (Exception ex) {
                logger.error(String.format("Error occured while read messages from topic[%s], partition[%s]", topic, partition), ex);
            } finally {
                consumer.unsubscribe();
            }
        }

        logger.debug("Read done, count={}", messages.size());
        return messages;
    }

    public void deleteTopic(String topic) {
        if (Objects.nonNull(topic))
            deleteTopics(Collections.singleton(topic));
    }

    private void deleteTopics(Set<String> topicNames) {
        try (AdminClient client = getAdminClient()) {
            while (true) {
                try {
                    DeleteTopicsResult r = client.deleteTopics(topicNames);
                    KafkaFuture<Void> future = r.all();
                    future.get();
                } catch (Exception ex){
                    logger.error("Error!!!",ex);
                }

                Set<String> aliveTopics = listTopics().stream()
                        .filter(topicNames::contains)
                        .collect(Collectors.toSet());
                if (CollectionUtils.isEmpty(aliveTopics)) {
                    break;
                }

                logger.debug("Kafka delete topics. Start new cycle. topics ={}",topicNames);
            }
        } catch (Exception ex) {
            logger.error("[Kafka] Error on delete topics", ex);
        }

    }

    private Set<String> listTopics() {
        return consumer.listTopics().keySet();
    }


    private void checkAssignment() {
        Set<TopicPartition> partitions = consumer.assignment();
        if (CollectionUtils.isNotEmpty(partitions))
            throw new IllegalStateException("Consumer already assigned to " + partitions);
    }

    @Override
    public void close(){
        consumer.close();
    }

    private AdminClient getAdminClient() {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        return AdminClient.create(config);
    }

}