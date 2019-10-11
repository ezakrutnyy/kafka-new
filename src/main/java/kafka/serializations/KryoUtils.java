package kafka.serializations;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoCallback;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import kafka.value.Employee;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.ByteArrayOutputStream;

public class KryoUtils {

    private static final Logger logger = LoggerFactory.getLogger(KryoUtils.class);

    private static final KryoFactory factory = new KryoFactory() {
        public Kryo create() {
            Kryo kryo = new Kryo();
            try {
                /*
                 * Register пока хз на что влияет
                 * */
                kryo.register(Employee.class,100);
                /*
                 * InstantiatorStrategy - Стратегия создания объектов
                 * */
                kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
            } catch (Exception ex) {
                logger.error("Exception occurred", ex);
            }
            return kryo;
        }
    };

    private static final KryoPool pool = new KryoPool.Builder(factory).softReferences().build();




    public static byte[] serialize(final Object data) {
        return pool.run(new KryoCallback<byte[]>() {
            public byte[] execute(Kryo kryo) {
                ByteArrayOutputStream stream = new ByteArrayOutputStream();
                Output output = new Output(stream);
                kryo.writeClassAndObject(output, data);
                output.close();
                return stream.toByteArray();
            }
        });
    }

    public static <V> V deserialize(final byte[] data) {
        return pool.run(new KryoCallback<V>() {
            public V execute(Kryo kryo) {
                Input input = new Input(data);
                return (V) kryo.readClassAndObject(input);
            }
        });
    }

}