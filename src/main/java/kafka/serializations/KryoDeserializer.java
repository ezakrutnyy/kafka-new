package kafka.serializations;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class KryoDeserializer implements Deserializer<Object> {

    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    public Object deserialize(String topic, byte[] data) {
        return KryoUtils.deserialize(data);
    }

    public void close() {

    }
}