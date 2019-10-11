package kafka.partitions;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;

public class StringPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        int totalPartitionsNumber = cluster.partitionCountForTopic(topic);

        if ( (keyBytes == null) || (!(key instanceof String)))
            throw  new InvalidRecordException("Key must be Strings");

        return Utils.toPositive(Utils.murmur2(keyBytes)) % totalPartitionsNumber;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}