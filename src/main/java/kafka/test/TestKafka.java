package kafka.test;

import kafka.consumer.AcqKafkaConsumer;
import kafka.partitions.StringPartitioner;
import kafka.producer.AcqKafkaProducer;
import kafka.producer.KafkaSessionFactory;
import kafka.value.Employee;
import org.apache.kafka.clients.producer.Partitioner;
import service.SXSSFExportExcelService;

import java.io.IOException;
import java.util.List;

public class TestKafka {

    final private static String topic = "country-33";

    public static void main(String[] args) throws IOException {
        // [1] create producer and create topic
        final AcqKafkaProducer producer = new KafkaSessionFactory().createProducer();
        // producer with custom partitioner class
        //final AcqKafkaProducer producer = new KafkaSessionFactory().createProducer(StringPartitioner.class);
        producer.createTopic(producer.configTopicWithCompressed(), topic, 5, 1);

        // [2] send messages and close producer
        for (Employee employee : Employee.fillTest()) {
            //producer.send(topic, 0, employee); - В конкретную партицию
            producer.send(topic, employee.getCountry(), employee);
        }
//        producer.close();

        // [3] create consumer and  poll messages
        final AcqKafkaConsumer consumer = new KafkaSessionFactory().createConsumer();
        //final List<Employee> employees = consumer.getMessages(topic, 0, Employee.class); - Из конкретной партиции
        final List<Employee> employees = consumer.getMessages(topic, Employee.class);

        // [4] delete topics and close consumer
//        consumer.deleteTopic(topic);
//        consumer.close();

        // [5] Generate report
        final SXSSFExportExcelService serviceExport = new SXSSFExportExcelService();
        serviceExport.export(Employee.titles(), employees);
    }
}