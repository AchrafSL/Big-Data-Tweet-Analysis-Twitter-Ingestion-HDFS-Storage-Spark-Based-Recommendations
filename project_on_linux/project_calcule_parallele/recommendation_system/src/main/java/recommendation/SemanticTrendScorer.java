
package recommendation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Paths;
import java.util.*;

public class SemanticTrendScorer {

    private static final ObjectMapper JSON = new ObjectMapper();

    public static Map<Integer, Double> calculate(
            Map<String, Double> trends) throws Exception {

        JsonNode registry = JSON.readTree(
            Paths.get("../topic_service/topic_registry.json").toFile());

        JsonNode products = JSON.readTree(
            Paths.get("data/product_embeddings.json").toFile());

        Map<String, double[]> topicVectors = new HashMap<>();

        for (JsonNode topic : registry.path("topics")) {
            String label = topic.path("label").asText().trim().toLowerCase();

            if (trends.containsKey(label)) {
                topicVectors.put(label, toVector(topic.path("embedding")));
            }
        }

        Map<Integer, Double> scores = new HashMap<>();
        double totalWeight = topicVectors.keySet().stream()
            .mapToDouble(trends::get).sum();

        Iterator<Map.Entry<String, JsonNode>> iterator =
            products.fields();

        while (iterator.hasNext()) {
            Map.Entry<String, JsonNode> product = iterator.next();
            double[] productVector = toVector(product.getValue());

            double weightedSimilarity = 0.0;

            for (Map.Entry<String, double[]> topic : topicVectors.entrySet()) {
                double similarity = cosine(productVector, topic.getValue());
                double weight = trends.get(topic.getKey());

                weightedSimilarity += weight * Math.max(0.0, similarity);
            }

            double score = totalWeight > 0
                ? weightedSimilarity / totalWeight : 0.0;

            scores.put(Integer.parseInt(product.getKey()), score);
        }

        System.out.println("[SemanticTrendScorer] Scored "
            + scores.size() + " products using "
            + topicVectors.size() + " trending topics.");

        return scores;
    }

    private static double[] toVector(JsonNode array) {
        double[] vector = new double[array.size()];

        for (int i = 0; i < array.size(); i++) {
            vector[i] = array.get(i).asDouble();
        }

        return vector;
    }

    private static double cosine(double[] a, double[] b) {
        // both vectors already normalised so the norms are equale to zero
        double dot = 0.0;

        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
        }

        return dot;
    }
}
