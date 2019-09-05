import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 *  [bootstrap.servers] параметр для первоначального соединения с кластером кафки
 *  [key.serializer] - класс сериализации ключа
 *  [value.serializer] - класс сериализации значения
 *  [acks] - число поступивших сообщение реплик
 *      0 - отправка без ожидания ответа реплик
 *      1 - отправка, с подтверждением только ведущей репликой
 *      all - отправка с подтверждением всех реплик
 *  [buffer.memory] - объем памяти используемый производителем для буферизаци сообщений ожидающих отправку
 *  [compression.type] - применение к данным алгоритмов сжатия, по умолчанию сообщения не сжимаются.
 *      Например значения: snappy, gzip, lz4
 *  [retries] - кол-во потворных попыток отправок сообщений в случае ошибки сервера
 *  [retry.backoff.ms] - По умолчанию значение 100, это время через которое будет потоврная попытка отправки сообщения
 *  [batch.size] - определяет объем в байтах для каждого пакета, при заполнение которого, все входищие
 *      в него сообщения отправляются. Но может и раньше.
 *  [linger.ms] - управляет временем ожидания дополнительных сообщений перед отправкой текущего пакета.
 *  [client.id] - идентификатор производителя для брокера
 *  [max.in.flight.requests.per.connection] - управляет кол-вом элементов который может послать производитель
 *      серверу, не дожидаясь ответа. Значение - 1 гаранитирует порядок сообщений в брокере в порядке их отиправки
 *      даже в случае потворной отправки
 *  [timeout.ms] [request.timeout.ms] [metadata.fetch.timeout.ms] - параметры управляют длительностью ожидания
 *      производителем ответа от сервера при отправки перед повторной отправкой или генерировании ошибки
 *  [max.block.ms] - управляет временем блокировки при вызове метода send  или partitionsFor() при заполнении буфера
 *  отправки производителем либо недоступности метаданных. По истечению генерируется ошибка!
 *  [max.request.size] - задает максимальный размер отровляемого производителем запроса
 *  [partitioner.class] - объект партиций для компоновки по разделам
 *
 *
 */


public class MessageSender {

    private final static Logger LOGGER = LoggerFactory.getLogger(MessageSender.class);

    private final KafkaProducer<String, byte[]> kafkaProducer;

    public MessageSender(String kafkaServers) {
        this(kafkaServers, null);
    }

    public MessageSender(String kafkaServers, Class<? extends Partitioner> partitioner) {
        final Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", kafkaServers);
        kafkaProps.put("acks", "all");
        kafkaProps.put("key.serializer", StringSerializer.class);
        kafkaProps.put("value.serializer", ByteArraySerializer.class);
        kafkaProps.put("max.request.size", "5000000");
        kafkaProps.put("batch.size", "32000");
        kafkaProps.put("linger.ms", "1000");
        if (Objects.nonNull(partitioner)) {
            kafkaProps.put("partitioner.class", partitioner);
        }
        kafkaProducer = new KafkaProducer<>(kafkaProps);
    }

    public Future<RecordMetadata> sendMessage(String topic,
                                              String key,
                                              byte[] value) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(String.format("Send new message to kafka (Type: %s, Key: %s, Value: %s)",
                    value, topic, new String(value)));
        }
        return kafkaProducer.send(new ProducerRecord<>(topic, key, value));
    }

    public Future<RecordMetadata> sendMessage(String topic, int partition, byte[] data) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(String.format("Send new message to kafka (Type: %s, Partition: %s, Value: %s)",
                    partition, topic, new String(data)));
        }
        return kafkaProducer.send(new ProducerRecord<>(topic, partition, null, data));
    }
}
