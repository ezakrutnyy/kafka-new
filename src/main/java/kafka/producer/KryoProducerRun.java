package kafka.producer;

import kafka.serializations.KryoSerializer;
import kafka.value.Employee;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static net.sourceforge.argparse4j.impl.Arguments.store;

public class KryoProducerRun {

    private static final Logger logger = LoggerFactory.getLogger(KryoProducerRun.class);

    public static void main(String[] args) {

        logger.info("Started KryoProducerRun!");

        ArgumentParser parser = getArgs();

        try {

            Namespace params = parser.parseArgs(args);

            /* input params */
            String brokerList = params.getString("bootstrap.servers");
            String topic = params.getString("topic");
            Boolean isAsync = params.getBoolean("isAsync");

            String login = params.getString("login");
            String country = params.getString("country");
            Integer age = params.getInt("age");

            Properties producerConfig = new Properties();
            producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
            producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-kryo-id");
            producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
            producerConfig.put(ProducerConfig.RETRIES_CONFIG, 3);
            producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KryoSerializer.class);
            producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KryoSerializer.class);

            SimpleProducer<String, Employee> producer = null;
            try {
                producer = new SimpleProducer(producerConfig, isAsync);
                producer.send(topic, country, new Employee(login,country,age));
            } catch (Exception ex) {
                logger.error("Error while process send for topic{}", topic, ex);
            } finally {
                producer.close();
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
        } finally {
            logger.info("Ended KryoProducerRun!");
        }
    }

    private static ArgumentParser getArgs() {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("kafka-release")
                .defaultHelp(true)
                .description("This example is to demonstrate kafka-release project");

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

        parser.addArgument("--isAsync").action(store())
                .required(false)
                .type(Boolean.class)
                .setDefault(false)
                .metavar("TRUE/FALSE")
                .help("sync/async send of message");

        parser.addArgument("--login").action(store())
                .required(true)
                .type(String.class)
                .setDefault("Mister N")
                .metavar("Login")
                .help("Employee login!");

        parser.addArgument("--age").action(store())
                .required(false)
                .setDefault(0)
                .type(Integer.class)
                .metavar("Age")
                .help("Employee login!");

        parser.addArgument("--country").action(store())
                .required(false)
                .setDefault("Russia")
                .type(String.class)
                .metavar("Country")
                .help("Country employee");

        return parser;
    }
}