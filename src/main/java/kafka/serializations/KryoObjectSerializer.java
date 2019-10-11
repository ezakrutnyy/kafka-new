package kafka.serializations;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoPool;

import java.util.TreeMap;

public class KryoObjectSerializer implements ObjectSerializer {

    private final KryoPool pool;
    private final int maxBufferSize;

    static TreeMap<Integer, String> registrations = new TreeMap<>();
    static {
        registrations.put(101, "kafka.value.Employee");
    }

    public KryoObjectSerializer() {
        maxBufferSize = -1;
        this.pool = KryoPoolFactory.build(registrations);
    }

    @Override
    public byte[] serialize(Object object) {
        Kryo kryo = pool.borrow();
        try {
            Output output = new Output(Math.min(4096, maxBufferSize == -1 ? 4096 : maxBufferSize), maxBufferSize);
            kryo.writeObject(output, object);
            output.flush();
            return output.toBytes();
        } finally {
            pool.release(kryo);
        }
    }

    @Override
    public <T> T deserialize(byte[] data, Class<T> type) {
        Kryo kryo = pool.borrow();
        try {
            return kryo.readObject(new Input(data), type);
        } finally {
            pool.release(kryo);
        }
    }
}