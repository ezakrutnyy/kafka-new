import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import kafka.utils.json.JsonObject;
import org.objenesis.strategy.StdInstantiatorStrategy;

public enum KryoPoolFactory {

    INSTANCE;


    private KryoPool kryoPool;

    KryoPoolFactory() {
        KryoFactory kryoFactory = () -> {
            Kryo kryo = new Kryo();
            kryo.setReferences(false);
            kryo.register(Employee.class);
            kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
            return kryo;
        };
        kryoPool = new KryoPool.Builder(kryoFactory).softReferences().build();
    }


    public KryoPool getPool() {
        return kryoPool;
    }
}