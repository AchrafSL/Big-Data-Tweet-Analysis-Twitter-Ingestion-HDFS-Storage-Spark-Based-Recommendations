package recommendation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;

public class TrendsLoader {

    /**
     * Lit un CSV "topic,frequency" et retourne les topK tendances
     * sous la forme {mot-clé -> poids entre 0 et 1}.
     *
     * À l'intégration : il suffit de passer Config.HDFS_TRENDS_PATH comme chemin
     * (à condition que Personne 2 écrive bien "topic,frequency" avec en-tête).
     */
    public static Map<String, Double> load(SparkSession spark, String path, int topK) {

        Dataset<Row> trends = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(path)
                .orderBy(col("frequency").desc())
                .limit(topK);

        List<Row> rows = trends.collectAsList();

        double maxFreq = 0.0;
        for (Row r : rows) {
            maxFreq = Math.max(maxFreq, ((Number) r.get(1)).doubleValue());
        }
        if (maxFreq == 0.0) maxFreq = 1.0; // évite une division par zéro

        Map<String, Double> weights = new LinkedHashMap<>();
        for (Row r : rows) {
            String keyword = r.getString(0).trim().toLowerCase();
            double freq = ((Number) r.get(1)).doubleValue();
            weights.put(keyword, freq / maxFreq);
        }
        return weights;
    }
}