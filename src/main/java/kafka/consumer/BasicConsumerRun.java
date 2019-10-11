package kafka.consumer;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collections;
import java.util.Properties;
import static net.sourceforge.argparse4j.impl.Arguments.store;


public class BasicConsumerRun {

    private static final Logger logger = LoggerFactory.getLogger(BasicConsumerRun.class);

    public static void main(String[] args)  {

        logger.info("Started BasicConsumerRun!");

        ArgumentParser parser = getArgs();

        try {
            /* input params */
            final Namespace params = parser.parseArgs(args);
            final String servers = params.getString("bootstrap.servers");
            final String topic = params.getString("topic");
            Properties consumerConfig = new Properties();
            consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-z1");
            consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(consumerConfig);
            consumer.subscribe(Collections.singletonList(topic));
            while (true) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(100);
                logger.debug("records size()"+records.count());
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    logger.info(String.format("Read record: topic[%s], partition[%s], offset[%s], key[%s], value[%s]",
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            deserialize(record.key()),
                            deserialize(record.value())));
                }
                consumer.commitSync();
            }
        } catch (ArgumentParserException ex) {
            if (args.length == 0) {
                logger.info("Args is null!");
                parser.printHelp();
                System.exit(0);
            } else {
                logger.info("UndefinedError!", ex);
                parser.handleError(ex);
                System.exit(1);
            }
        }catch (Exception ex){
            logger.error("Undefined Error!!!", ex);
        } finally {
            logger.info("Ended BasicConsumerRun!");
        }
    }

    private static <V> V deserialize(byte[] obj) {
        return SerializationUtils.deserialize(obj);
    }

    private static ArgumentParser getArgs() {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("kafka-release")
                .defaultHelp(true)
                .description("This example is to demonstrate kafka-release capabilities");

        parser.addArgument("--bootstrap.servers").action(store())
                .required(true)
                .type(String.class)
                .metavar("BROKER-LIST")
                .help("comma separated broker list");

        parser.addArgument("--topic").action(store())
                .required(true)
                .type(String.class)
                .metavar("TOPIC")
                .help("produce messages to this topic");

        return parser;
    }
}