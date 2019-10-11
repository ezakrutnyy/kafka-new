package kafka.producer;

import kafka.value.LinkVisited;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Properties;

import static net.sourceforge.argparse4j.impl.Arguments.store;

public class BasicProducerRun {

    private static final Logger logger = LoggerFactory.getLogger(BasicProducerRun.class);

    public static void main(String[] args) {

        logger.info("Started BasicProducerRun!");

        ArgumentParser parser = getArgs();

        try {
            Namespace params = parser.parseArgs(args);
            /* input params */
            String brokerList = params.getString("bootstrap.servers");
            String topic = params.getString("topic");
            Boolean isAsync = params.getBoolean("isAsync");
            Long range = params.getLong("range");
            String url = params.getString("url");
            String login = params.getString("login");
            Properties producerConfig = new Properties();
            producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
            producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "basic-kafka.producer");
            producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
            producerConfig.put(ProducerConfig.RETRIES_CONFIG, 3);
            producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
            producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
            SimpleProducer<byte[], byte[]> producer = null;
            try {
                producer = new SimpleProducer(producerConfig, isAsync);;
                producer.send(topic, serialize(url), serialize(new LinkVisited(range,url,login)));
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
            logger.info("Ended BasicProducerRun!");
        }
    }

    private static byte[] serialize(Object obj) {
        return SerializationUtils.serialize((Serializable) obj);
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

        parser.addArgument("--range").action(store())
                .required(false)
                .type(Long.class)
                .setDefault(0L)
                .metavar("RANGE")
                .help("Time on visited url!");

        parser.addArgument("--url").action(store())
                .required(false)
                .setDefault("blank")
                .type(String.class)
                .metavar("URL")
                .help("Url visited");

        parser.addArgument("--login").action(store())
                .required(false)
                .setDefault("anonimus")
                .type(String.class)
                .metavar("LOGIN")
                .help("Login visited");

        return parser;
    }
}