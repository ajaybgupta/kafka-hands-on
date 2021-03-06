package tutorial3;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ElasticsearchConsumer {
    public static void main(String[] args) throws IOException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ElasticsearchConsumer.class.getName());
        // Elasticsearch Client
        RestHighLevelClient client = createClient();

        // Kafka Consumer where the
        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord record : records) {

                //   Non idempotent - Auto Generated Id
//                IndexRequest indexRequest = new IndexRequest(
//                        "twitter",
//                        "tweets"
//                ).source(record.value().toString(), XContentType.JSON);

                // To make our consumer producer idempotent
                // We will be leveraging Twitter feed specific id
                String id = extractIdFromTweet(record.value().toString());

                // Where we insert data into Elasticsearch
                IndexRequest indexRequest = new IndexRequest(
                        "twitter",
                        "tweets",
                        id // id to make it idempotent
                ).source(record.value().toString(), XContentType.JSON);
                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                String responseId = indexResponse.getId();
                logger.info("Id:" + responseId);
                Thread.sleep(1000);
            }
        }
    }

    private static JsonParser jsonParser = new JsonParser();

    public static String extractIdFromTweet(String tweetJSON) {
        // GSON library
        return jsonParser
                .parse(tweetJSON)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

    public static RestHighLevelClient createClient() {
        String hostname = "127.0.0.1";
        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 9200, "http"));
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }


    public static KafkaConsumer<String, String> createConsumer(String topic) {
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        // Subscribe Consumer to our Topics
        return consumer;
    }
}
