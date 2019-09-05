import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;

public class KafkaProducerDemo {
    public static void main(String[] args) {

//        Employee employee = new Employee();
//        employee.setCountry("Russia");
//        employee.setName("Petrov Petr  Petrovich");
//        employee.setYear(35);

//        Employee employee = new Employee();
//        employee.setCountry("Russia");
//        employee.setName("Ivanov Ivan Ivanovich");
//        employee.setYear(45);

//        Employee employee = new Employee();
//        employee.setCountry("USA");
//        employee.setName("Joch Smith");
//        employee.setYear(31);

        Employee employee = new Employee();
        employee.setCountry("Serbia");
        employee.setName("Chester Varvil");
        employee.setYear(12);

        KryoObjectSerializer kryo = new KryoObjectSerializer();
        byte[] data = kryo.serialize(employee);
        System.out.println(data);


        String severs = "localhost:9092";

        try {
            MessageSender sender = new MessageSender(severs);
            Future<RecordMetadata> rs = sender.sendMessage("Country", employee.getCountry(), data);
            rs.get();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }
}
