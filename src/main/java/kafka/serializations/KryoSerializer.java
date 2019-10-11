package kafka.serializations;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class KryoSerializer implements Serializer<Object> {

    public void configure(Map<String, ?> configs, boolean isKey) { }

    public byte[] serialize(String topic, Object data) {
        return KryoUtils.serialize(data);
    }

    public void close() { }
}