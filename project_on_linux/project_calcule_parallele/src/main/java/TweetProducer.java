
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Properties;

public class TweetProducer {

    private static final String API_URL =
        "https://datasets-server.huggingface.co/rows";

    private static final String DATASET =
        "NLP-UniBW/tweets_dataset_jan_feb_big_deduplicated";

    private static final String TOPIC = "tweets";
    private static final String BROKER = "localhost:9092";

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final HttpClient client = HttpClient.newHttpClient();

    // 1. Fetch one page of tweets from Hugging Face

    private static JsonNode fetchTweets(int offset, int limit)
            throws Exception {

        String url = API_URL
            + "?dataset=" + DATASET.replace("/", "%2F")
            + "&config=default"
            + "&split=train"
            + "&offset=" + offset
            + "&length=" + limit;

        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(60))
                    .header("User-Agent", "TweetProducer/1.0")
                    .GET()
                    .build();

                HttpResponse<String> response = client.send(
                    request, HttpResponse.BodyHandlers.ofString()
                );

                if (response.statusCode() == 200) {
                    return mapper.readTree(response.body()).path("rows");
                }

                System.err.println(
                    "API returned HTTP " + response.statusCode()
                    + " (attempt " + attempt + "/3)"
                );

            } catch (java.io.IOException e) {
                System.err.println(
                    "API connection failed (attempt " + attempt + "/3): "
                    + e.getMessage()
                );
            }

            if (attempt < 3) {
                Thread.sleep(2000L * attempt);
            }
        }

        throw new RuntimeException(
            "Failed to fetch tweets after 3 attempts. Offset: " + offset
        );
    }


    // Send tweets to Kafka
    private static void startProducer(
            int totalTweets, int pageSize, int delayMs)
            throws Exception {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                  StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                  StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer =
                 new KafkaProducer<>(props)) {

            int sent = 0;

            for (int offset = 0;
                 offset < totalTweets;
                 offset += pageSize) {

                int limit = Math.min(pageSize, totalTweets - offset);
                JsonNode rows = fetchTweets(offset, limit);

                if (!rows.isArray() || rows.isEmpty()) {
                    System.out.println("No more tweets available.");
                    break;
                }

                for (JsonNode item : rows) {
                    JsonNode tweet = item.path("row");

                    // Keep only the three required fields
                    JsonNode message = mapper.createObjectNode()
                        .put("id", tweet.path("id").asText(""))
                        .put("search_term", tweet.path("search_term").asText(""))
                        .put("text", tweet.path("text").asText(""))
                        .set("date", tweet.path("date"));

                    String value = mapper.writeValueAsString(message);

                    producer.send(
                        new ProducerRecord<>(TOPIC, value)
                    ).get();

                    sent++;
                    System.out.println("Published: " + sent);

                    if (delayMs > 0) {
                        Thread.sleep(delayMs);
                    }
                }
            }

            producer.flush();
            System.out.println("Finished. Total sent: " + sent);
        }
    }

    // Configure and run
    public static void main(String[] args) throws Exception {
        int totalTweets = args.length > 0 ? Integer.parseInt(args[0]) : 1000;

        int pageSize = args.length > 1 ? Integer.parseInt(args[1]) : 100;

        int delayMs = args.length > 2 ? Integer.parseInt(args[2]) : 200;

        if (totalTweets <= 0 || pageSize <= 0 || delayMs < 0) {
            throw new IllegalArgumentException(
                "Tweets and page size must be positive; delay cannot be negative."
            );
        }

        startProducer(totalTweets, pageSize, delayMs);
    }
}
