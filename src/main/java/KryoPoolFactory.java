import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.pool.KryoPool;
import com.esotericsoftware.kryo.util.DefaultClassResolver;
import com.esotericsoftware.kryo.util.DefaultStreamFactory;
import java.util.concurrent.ArrayBlockingQueue;


public class KryoPoolFactory {

    public static KryoPool build() {
        return new KryoPool.Builder(
                () -> {
                    Kryo kryo = new Kryo(new DefaultClassResolver(), null, new DefaultStreamFactory());
                    return kryo;
                })
                .queue(new ArrayBlockingQueue<>(1000))
                .softReferences()
                .build();
    }
}
