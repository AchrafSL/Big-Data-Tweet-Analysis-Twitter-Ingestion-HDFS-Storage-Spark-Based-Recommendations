import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import org.apache.spark.sql.streaming.Trigger;  

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.get_json_object;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;


import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.HashMap;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;


import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class TweetTopicExtractor {


    // Pipeline metrics
    private static final AtomicInteger batchesProcessed = new AtomicInteger(0);
    private static final AtomicInteger validTweetsProcessed =new AtomicInteger(0);
    private static final AtomicInteger topicsExtracted = new AtomicInteger(0);
    private static final AtomicInteger topicsNormalized = new AtomicInteger(0);
    private static final AtomicInteger recordsSavedToHDFS = new AtomicInteger(0);


    private static SparkSession createSparkSession() {
        SparkSession spark = SparkSession.builder()
            .appName("TweetTopicExtractor")
            .master("local[*]")  // use all cores for parallelisme
            .getOrCreate(); // create session

        return spark;
    }


    




    private static Dataset<Row> readTweetsFromKafka(SparkSession spark) {
        // use spark streaming to read a stream
        // each batch contain 10 tweets read from kafka broker by 

        Dataset<Row> messages = spark.readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092") // broker adress
            .option("subscribe", "tweets")  // subscribe to the tweets topic
            .option("maxOffsetsPerTrigger", 10) // limit reading 10 tweets per micro batch
            .option("startingOffsets", "latest") // reads the new messages not old ones
            .load();

        return messages;
    }






    private static Dataset<Row> extractTweetFields(Dataset<Row> messages) {
        /*
            kafka sent messages in this format:
            key, value, topic, ... : value contains the json of {text, date, search_term}
            
            Spark built-in functions suports distributed execution and benifit from query optimisation
            reducing complex functions to parse fields.
        */
    
        // decode binary to str (desearialisation)
        Dataset<Row> jsonMessages = messages.selectExpr("CAST(value AS STRING) AS json");
        // this returns a Dataset with only one column named json containing string repr of value


        // extract search_term, text and date, id
        Dataset<Row> extractedDataset = jsonMessages.select(
            get_json_object(col("json"), "$.id").alias("id"),
            get_json_object(col("json"), "$.search_term").alias("search_term"),
            get_json_object(col("json"), "$.text").alias("text"),
            get_json_object(col("json"), "$.date").alias("date")
        );
        return extractedDataset;

    }



    private static void printStreamTest(Dataset<Row> tweets) throws Exception {
        StreamingQuery query = tweets.writeStream()
            .format("console")  // print in console
            .outputMode("append") // print new messages
            .option("truncate", false) // print full txt
            .start();

        query.awaitTermination();
    }





    private static void processBatch(Dataset<Row> batch, Long batchId) {
        
        batchesProcessed.incrementAndGet();


        System.out.println("Processing batch: " + batchId);

        JavaRDD<Row> tweetRDD = batch.javaRDD();

        // Filter out empty tweet text
        JavaRDD<Row> filteredRDD = tweetRDD.filter(tweet -> {
            String text = tweet.getAs("text");
            return text != null && !text.trim().isEmpty();
        });

        // Collect this small batch on the Spark driver
        List<Row> tweets = filteredRDD.collect();

        validTweetsProcessed.addAndGet(tweets.size());


        System.out.println("Non-empty tweets: " + tweets.size());

        if (tweets.isEmpty()) {
            System.out.println("Skipping empty batch.");
            return;
        }

        try {
            // One LLM request for the entire batch
            String llmResponse = callOllama(tweets);

            System.out.println("Gemma response:");
            System.out.println(llmResponse);
  
            Map<String, String> topics = parseTopics(llmResponse);

            topicsExtracted.addAndGet(topics.size());


            System.out.println("Topics parsed: " + topics.size());

            Map<String, String> canonicalTopics = normalizeTopics(topics);

            topicsNormalized.addAndGet(canonicalTopics.size());


            System.out.println("Topics normalized: " + canonicalTopics.size());

            saveBatchToHDFS(batchId, tweets, topics, canonicalTopics);


        } catch (Exception e) {
            System.err.println(
                "LLM processing failed: " + e.getMessage()
            );
            e.printStackTrace();
        } finally {
            printMetrics();
        }
    }
    





    private static void startBatchProcessing(Dataset<Row> tweets) throws Exception {

        StreamingQuery query = tweets.writeStream()
            .outputMode("append") // processes newly appended rows in the broker
            .trigger(Trigger.ProcessingTime("3 seconds")) // 3s delay between batch retrieved
            .foreachBatch((Dataset<Row> batch, Long batchId) -> processBatch(batch, batchId))  // sends each micro-batch to your processBatch() function instead of writing directly to a file.
            .start();

        query.awaitTermination();
    }






    private static String callOllama(List<Row> tweets) throws Exception {

        ObjectMapper mapper = new ObjectMapper();

        // Prepare only the ID and text for the LLM
        List<Map<String, String>> inputTweets = new ArrayList<>();

        for (Row tweet : tweets) {
            Map<String, String> item = new LinkedHashMap<>();

            item.put("id", (String) tweet.getAs("id"));
            item.put("text", (String) tweet.getAs("text"));
            inputTweets.add(item);
        }

        
        String prompt = """
            You are an expert tweet topic extraction system.

            For each tweet, identify its main subject as precisely as the text allows.

            
            RULES:
                1. Identify WHAT the tweet is specifically about, not just its general category.

                2. For political criticism, identify the target and the specific
                issue being criticized, such as a politician, government,
                policy, decision, election, or event, whenever the text supports it.

                3. For wars, identify the countries, conflict, or specific event
                involved whenever the text provides enough information.

                4. For economic topics, identify the specific issue, such as
                inflation, unemployment, a financial crisis, or a business deal.

                5. Identify the central claim, action, or event, not merely the
                general context in which it occurs.

                6. Avoid vague labels such as "Politics", "News", "Economics",
                "Social Issues", "Political Criticism", or "Political Campaigns"
                when a more specific subject can be identified from the tweet.

                7. Never invent names, events, causes, or details absent from the text.

                8. If the tweet lacks sufficient context, use the most specific
                topic that the available text supports.

                9. Use a concise, informative topic label, usually 2-8 words.

                10. Preserve each original ID exactly as provided.

                11. Return exactly one topic per tweet.

                12. Return ONLY a valid JSON array, without Markdown.


            Required format:
            [
            {"id": "original_id", "topic": "specific topic"}
            ]

            Tweets to classify:
            """ + mapper.writeValueAsString(inputTweets);


        // Construct the HTTP request body
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("model", "gemma3:4b");
        body.put("prompt", prompt);
        body.put("stream", false);

        String requestBody = mapper.writeValueAsString(body);

        HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

        String ollamaUrl = System.getenv("OLLAMA_API_URL");
        if (ollamaUrl == null || ollamaUrl.trim().isEmpty()) {
            ollamaUrl = "http://<OLLAMA_HOST>:11434/api/generate";
        }

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(ollamaUrl))
            .timeout(Duration.ofMinutes(3))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(requestBody))
            .build();

        HttpResponse<String> response = client.send(
            request,
            HttpResponse.BodyHandlers.ofString()
        );

        if (response.statusCode() != 200) {
            throw new RuntimeException(
                "Ollama HTTP error: " + response.statusCode()
                    + " - " + response.body()
            );
        }

        JsonNode responseJson = mapper.readTree(response.body());

        return responseJson.path("response").asText();
    }




    private static Map<String, String> parseTopics(String response) throws Exception {

        ObjectMapper mapper = new ObjectMapper();

        response = response.trim()
            .replaceFirst("(?s)^```(?:json)?\\s*", "")
            .replaceFirst("\\s*```\\s*$", "");

        JsonNode results = mapper.readTree(response);
        Map<String, String> topics = new HashMap<>();

        for (JsonNode item : results) {
            String id = item.path("id").asText();
            String topic = item.path("topic").asText();

            if (!id.isBlank() && !topic.isBlank()) {
                topics.put(id, topic);
            }
        }

        return topics;
    }





    private static Map<String, String> normalizeTopics(Map<String, String> topics) throws Exception {

        ObjectMapper mapper = new ObjectMapper();
        List<Map<String, String>> input = new ArrayList<>();

        for (Map.Entry<String, String> entry : topics.entrySet()) {
            Map<String, String> item = new LinkedHashMap<>();
            item.put("id", entry.getKey());
            item.put("topic", entry.getValue());
            input.add(item);
        }

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("topics", input);

        String requestBody = mapper.writeValueAsString(body);

        HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:8000/normalize"))
            .timeout(Duration.ofSeconds(60))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(requestBody))
            .build();

        HttpResponse<String> response = client.send(
            request,
            HttpResponse.BodyHandlers.ofString()
        );

        if (response.statusCode() != 200) {
            throw new RuntimeException(
                "Topic normalization HTTP error: "
                    + response.statusCode() + " - " + response.body()
            );
        }

        JsonNode results = mapper.readTree(response.body()).path("results");
        Map<String, String> canonicalTopics = new HashMap<>();

        for (JsonNode item : results) {
            String id = item.path("id").asText();
            String topic = item.path("canonical_topic").asText();

            if (!id.isBlank() && !topic.isBlank()) {
                canonicalTopics.put(id, topic);
            }
        }

        return canonicalTopics;
    }




    private static void saveBatchToHDFS(long batchId, List<Row> tweets, Map<String, 
        String> gemmaTopics,Map<String, String> normalizedTopics) throws Exception {

        Configuration conf = new Configuration();
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));

        FileSystem fs = FileSystem.get(
            new URI("hdfs://master:9000"), conf
        );

        Path directory = new Path("/hadoop/tweet_topics");
        fs.mkdirs(directory);

        Path file = new Path(directory, "batch-" + batchId + ".csv");

        int savedCount = 0;

        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(
                    fs.create(file, false),
                    StandardCharsets.UTF_8))) {

            writer.write("id,date,gemma_topic,normalized_topic");
            writer.newLine();

            for (Row tweet : tweets) {
                String id = tweet.getAs("id");
                String date = tweet.getAs("date");
                String gemmaTopic = gemmaTopics.get(id);
                String normalizedTopic = normalizedTopics.get(id);

                if (gemmaTopic == null || gemmaTopic.isBlank()
                        || normalizedTopic == null || normalizedTopic.isBlank()) {
                    System.err.println("Missing topic for ID: " + id);
                    continue;
                }

                writer.write(csvEscape(id) + ","
                    + csvEscape(date) + ","
                    + csvEscape(gemmaTopic) + ","
                    + csvEscape(normalizedTopic));
                writer.newLine();

                savedCount++;
            }
        }

        recordsSavedToHDFS.addAndGet(savedCount);
        System.out.println("Saved to HDFS: " + file);
    }



    private static String csvEscape(String value) {
        if (value == null) {
            return "\"\"";
        }

        return "\"" + value.replace("\"", "\"\"") + "\"";
    }




    private static void printMetrics() {
        System.out.println("\n PIPELINE METRICS :\n");
        System.out.println("Batches processed: " + batchesProcessed.get());
        System.out.println("Valid tweets processed: " + validTweetsProcessed.get());
        System.out.println("Topics extracted: " + topicsExtracted.get());
        System.out.println("Topics normalized: " + topicsNormalized.get());
        System.out.println("Records saved to HDFS: " + recordsSavedToHDFS.get());
        System.out.println("======================================\n");
    }






    public static void main(String[] args) throws Exception {
        System.out.println("Starting Tweet Topic Extractor...");

        SparkSession spark = createSparkSession();

        System.out.println("[SUCCESS] Spark session created");

        spark.sparkContext().setLogLevel("WARN");  // because [info] messages are too much

        Dataset<Row> messages = readTweetsFromKafka(spark);
        System.out.println(messages.schema().treeString());


        Dataset<Row> tweets = extractTweetFields(messages);
        System.out.println(tweets.schema().treeString());

        // printStreamTest(tweets);

        startBatchProcessing(tweets);

        spark.stop();
    }
}