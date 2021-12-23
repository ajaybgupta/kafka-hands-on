package tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    List<String> terms = Lists.newArrayList("bitcoin");

    public TwitterProducer() {
    }

    public static void main(String[] args) {
        String fileName = "kafka-producer-twitter/src/main/resources/twitter.config";
        TwitterProducer twitterProducer = new TwitterProducer();
        TwitterCredentials twitterCredentials = twitterProducer.getTwitterCredentials(fileName);
        twitterProducer.run(twitterCredentials);
    }

    public TwitterCredentials getTwitterCredentials(String fileName) {
        Properties properties = new Properties();
        try (FileInputStream fis = new FileInputStream(fileName)) {
            properties.load(fis);
        } catch (IOException ex) {
            logger.error(ex.getMessage());
        }
        String consumerKey = properties.getProperty("consumerKey");
        String consumerSecret = properties.getProperty("consumerSecret");
        String token = properties.getProperty("token");
        String secret = properties.getProperty("secret");
        return new TwitterCredentials(consumerKey, consumerSecret, token, secret);
    }

    public void run(TwitterCredentials twitterCredentials) {
        logger.info("Run started!");
        /* Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);

        // Create Twitter Client
        Client client = createTwitterClient(msgQueue, twitterCredentials);
        // Attempts to establish a connection
        client.connect();

        // Create Kafka Producer
        KafkaProducer<String, String> kafkaProducer = createKafkaProducer();

        // Add a Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Application");
            logger.info("Shutting down Client");
            client.stop();
            logger.info("Shutting down Kafka Producer");
            kafkaProducer.close();
            logger.info("Done.");
        }));

        // Loop to send Kafka Tweets
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
                logger.error(e.getMessage());
            }

            if (msg != null) {
                logger.info(msg);

                // Create Producer Record
                ProducerRecord<String, String> record =
                        new ProducerRecord<String, String>("twitter_tweets", null, msg);

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

                kafkaProducer.send(record, callback);
            }
        }
        logger.info("End of application!");
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue, TwitterCredentials twitterCredentials) {

        /* Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        hosebirdEndpoint.trackTerms(terms);

        String consumerKey = twitterCredentials.getConsumerKey();
        String consumerSecret = twitterCredentials.getConsumerSecret();
        String token = twitterCredentials.getToken();
        String secret = twitterCredentials.getSecret();

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
        return builder.build();
    }

    public KafkaProducer<String, String> createKafkaProducer() {
        String bootStrapServers = "127.0.0.1:9092";
        // Create Producer Properties
        // We can get configuration from this website
        // https://kafka.apache.org/20/documentation.html#producerconfigs
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Safe Producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
//        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
//        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
//        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        // Kafka 2.0 >= 1.1 so we can use 5, use 1 otherwise
//
        // High Throughput Producer (At bit of expense of a bit of latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); // 32 KB

        // Create Producer
        return new KafkaProducer<String, String>(properties);
    }
}
