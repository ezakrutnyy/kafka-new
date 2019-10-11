package kafka.serializations;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.pool.KryoPool;
import com.esotericsoftware.kryo.util.DefaultClassResolver;
import com.esotericsoftware.kryo.util.DefaultStreamFactory;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;

public class KryoPoolFactory {

    public static KryoPool build(TreeMap<Integer, String> registration) {
        return new KryoPool.Builder(
                () -> {
                    Kryo kryo = new Kryo(new DefaultClassResolver(), null, new DefaultStreamFactory());
                    registration.forEach((k, v) -> {
                        try {
                            kryo.register(Class.forName(v), k);
                        } catch (ClassNotFoundException ex) {
                            throw new RuntimeException("Class " + v + " not found in classpath");
                        }
                    });
                    kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
                    return kryo;
                })
                .queue(new ArrayBlockingQueue<>(1000))
                .softReferences()
                .build();
    }

}