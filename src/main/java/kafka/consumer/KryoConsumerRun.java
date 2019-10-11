package kafka.consumer;

import kafka.serializations.KryoDeserializer;
import kafka.value.Employee;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

import static net.sourceforge.argparse4j.impl.Arguments.store;

public class KryoConsumerRun {

    private static final Logger logger = LoggerFactory.getLogger(KryoConsumerRun.class);

    public static void main(String[] args) {

        logger.info("Started KryoConsumerRun!");

        ArgumentParser parser = getArgs();

        try {
            /* input params */
            final Namespace params = parser.parseArgs(args);
            final String servers = params.getString("bootstrap.servers");
            final String topic = params.getString("topic");
            Properties consumerConfig = new Properties();
            consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-z1");
            consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KryoDeserializer.class);
            consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KryoDeserializer.class);
            consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            KafkaConsumer<String, Employee> consumer = new KafkaConsumer<String, Employee>(consumerConfig);
            consumer.subscribe(Collections.singletonList(topic));
            while (true) {
                ConsumerRecords<String, Employee> records = consumer.poll(100);
                logger.debug("records size()"+records.count());
                for (ConsumerRecord<String, Employee> record : records) {
                    logger.info(String.format("Read record: topic[%s], partition[%s], offset[%s], key[%s], value[%s]",
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            record.key(),
                            record.value()));
                }
                consumer.commitSync();
            }
        } catch(ArgumentParserException ex) {
            if (args.length == 0) {
                logger.info("Args is null!");
                parser.printHelp();
                System.exit(0);
            } else {
                logger.info("UndefinedError!", ex);
                parser.handleError(ex);
                System.exit(1);
            }
        } catch(Exception ex) {
            logger.error("Undefined Error!!!", ex);
        } finally {
            logger.info("Ended BasicConsumerRun!");
        }


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