package tutorial4;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StreamsFilterTweets {

    Logger logger = LoggerFactory.getLogger(StreamsFilterTweets.class.getName());

    public static void main(String[] args) {
        // Create Properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        // Ser Des as we are Serializing and Deserializing
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // Create Topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // Create Topic
        KStream<String, String> inputTopic = streamsBuilder.stream("twitter_tweets");
        KStream<String, String> filteredStream = inputTopic.filter(

                // Filter for tweets which have user of 10000 followers
                (k, jsonTweet) -> extractUserFollowerFromTweet(jsonTweet) > 10000
        );
        filteredStream.to("important_tweets");

        // Build Topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        // Start Stream Application
        kafkaStreams.start();
    }

    private static JsonParser jsonParser = new JsonParser();

    public static Integer extractUserFollowerFromTweet(String tweetJSON) {
        System.out.println("Tweet " + tweetJSON);
        // GSON library
        try {
            int followersCount = jsonParser
                    .parse(tweetJSON)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
            System.out.println("Followers Count" + followersCount);
            return followersCount;
        } catch (NullPointerException e) {
            return 0;
        }
    }
}
