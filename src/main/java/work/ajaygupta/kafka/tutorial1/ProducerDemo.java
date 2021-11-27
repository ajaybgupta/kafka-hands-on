package work.ajaygupta.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        String bootStrapServers = "127.0.0.1:9092";
        // Create Producer Properties
        // We can get configuration from this website
        // https://kafka.apache.org/20/documentation.html#producerconfigs
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        // Create Producer Record
        ProducerRecord<String,String> record =
                new ProducerRecord<String,String>("first-topic","Auto hello world");

        // Send Data - Asynchronous
        producer.send(record);

        // Flush Data
        producer.flush();

        // Flush and Close Data
        producer.close();
    }
}
