package kafka.serializations;

public interface ObjectSerializer {

    /**
     * serialize object
     *
     * @param object source
     * @return value as byte array
     */
    byte[] serialize(Object object);

    /**
     * deserialize object
     *
     * @param data value as byte array
     * @param type class of object
     * @param <T>  type of object
     * @return object or exception
     */
    <T> T deserialize(byte[] data, Class<T> type);
}