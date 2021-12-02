package work.ajaygupta.kafka.tutorial2;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    public TwitterProducer() {
    }

    public static void main(String[] args) {
        String fileName = "src/main/resources/work/ajaygupta/kafka/tutorial2/twitter.config";
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
            }
        }
        logger.info("End of application!");
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue, TwitterCredentials twitterCredentials) {

        /* Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("india");
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
}
