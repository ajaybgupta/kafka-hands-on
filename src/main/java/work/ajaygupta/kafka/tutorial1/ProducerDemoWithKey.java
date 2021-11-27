package work.ajaygupta.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKey {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String bootStrapServers = "127.0.0.1:9092";
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithKey.class);

        // Create Producer Properties
        // We can get configuration from this website
        // https://kafka.apache.org/20/documentation.html#producerconfigs
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        String topic = "first-topic";

        for (int i = 0; i < 100; i++) {
            String key = "key_" + Integer.toString(i);
            String value = "Hello World! " + Integer.toString(i);
            logger.info("Key being used " + key);

            // Create Producer Record
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic, key, value);

            // Callback
            Callback callback = new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // Executes Every Time when Record is Sent or An Exception is Thrown
                    if (e == null) {
                        // record was successfully sent
                        logger.info("\n" +
                                "Received New Meta Data. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp()
                        );
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            };
            // Send Data - Asynchronous with Callback
            // Making it Synchronous by forcing .get
            // Do not do this in production
            producer.send(record, callback).get();
        }

        // Flush Data
        producer.flush();

        // Flush and Close Data
        producer.close();
    }
}
