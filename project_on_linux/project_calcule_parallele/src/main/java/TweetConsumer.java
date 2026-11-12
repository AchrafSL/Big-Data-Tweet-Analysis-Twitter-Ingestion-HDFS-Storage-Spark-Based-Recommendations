
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class TweetConsumer {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                  "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "tweet-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                  StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                  StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        ObjectMapper mapper = new ObjectMapper();

        try (KafkaConsumer<String, String> consumer =
                 new KafkaConsumer<>(props)) {

            consumer.subscribe(Collections.singletonList("tweets"));

            System.out.println("Waiting for tweets...\n");

            while (true) {
                ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                        JsonNode tweet;

                        try {
                        tweet = mapper.readTree(record.value());
                        } catch (Exception e) {
                        System.out.println("Skipping invalid message: " + record.value());
                        continue;
                        }

                        if (tweet == null || !tweet.isObject()) {
                        System.out.println("Skipping non-JSON message: " + record.value());
                        continue;
                        }

                        if (!tweet.has("search_term") ||
                        !tweet.has("text") ||
                        !tweet.has("date")) {
                        System.out.println("Skipping message with missing fields");
                        continue;
}
                    System.out.println("Search term: "
                        + tweet.path("search_term").asText());
                    System.out.println("Text: "
                        + tweet.path("text").asText());
                    System.out.println("Date: "
                        + tweet.path("date").asText());
                    System.out.println("--------------------------------");
                }
            }
        }
    }
}
