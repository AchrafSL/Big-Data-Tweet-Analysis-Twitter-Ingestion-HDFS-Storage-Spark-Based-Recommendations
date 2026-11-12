package recommendation;

import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.round;
import static org.apache.spark.sql.functions.row_number;


import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class HybridRecommender {

    /** Lancement isolé : ne marche que si un modèle a été sauvegardé (mode intégré). */
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkFactory.create("Hybrid-Recommender");
        try {
            run(spark, null);
        } finally {
            spark.stop();
        }
    }

    /**
     * @param model modèle ALS déjà entraîné (mode local). Si null, il est chargé depuis Config.MODEL_PATH.
     */
    public static void run(SparkSession spark, ALSModel model) throws Exception {

        // ---------- 1. Chargement ----------
        if (model == null) {
            model = ALSModel.load(Config.MODEL_PATH);
        }

        Dataset<Row> products = readCsv(spark, Config.PRODUCTS_PATH);
        Dataset<Row> ratings  = readCsv(spark, Config.RATINGS_PATH).select("userId", "productId");

        Map<String, Double> trends =
                TrendsLoader.load(spark, Config.TRENDS_PATH, Config.TRENDS_TOP_K);
        System.out.println("[Hybrid] Tendances prises en compte (mot -> poids) : " + trends);

        // ---------- 2. Score ALS de tous les produits pour chaque utilisateur ----------
        int nbProducts = (int) products.count();

        Dataset<Row> alsRecs = model.recommendForAllUsers(nbProducts)
                .withColumn("rec", explode(col("recommendations")))
                .select(
                        col("userId"),
                        col("rec.productId").alias("productId"),
                        col("rec.rating").cast("double").alias("alsScore"));

        // ---------- 3. On retire les produits déjà notés ----------
        Dataset<Row> candidates = alsRecs
                .join(ratings,
                        alsRecs.col("userId").equalTo(ratings.col("userId"))
                                .and(alsRecs.col("productId").equalTo(ratings.col("productId"))),
                        "left_anti")
                .join(products, "productId");

        // ---------- 4. Normalisation min-max du score ALS ----------
        Row minMax = candidates.agg(min("alsScore"), max("alsScore")).first();

        double minAls = 0.0;
        double maxAls = 0.0;

        if (!minMax.isNullAt(0) && !minMax.isNullAt(1)) {
        minAls = minMax.getDouble(0);
        maxAls = minMax.getDouble(1);
        }

        double range = (maxAls - minAls) == 0 ? 1.0 : (maxAls - minAls);


        // ---------- 5. Score de tendance sémantique ----------
        Map<Integer, Double> semanticScores =
                SemanticTrendScorer.calculate(trends);

        List<Row> scoreRows = new java.util.ArrayList<>();

        for (Map.Entry<Integer, Double> entry : semanticScores.entrySet()) {
            scoreRows.add(RowFactory.create(
                    entry.getKey(),
                    entry.getValue()
            ));
        }

        StructType scoreSchema = new StructType()
                .add("productId", DataTypes.IntegerType, false)
                .add("trendScore", DataTypes.DoubleType, false);

        Dataset<Row> semanticScoreDF =
                spark.createDataFrame(scoreRows, scoreSchema);

        // ---------- 6. Score final hybride ----------
        Dataset<Row> scored = candidates
                .join(semanticScoreDF, "productId")
                .withColumn(
                        "alsNorm",
                        col("alsScore").minus(minAls).divide(range)
                )
                .withColumn(
                        "finalScore",
                        col("alsNorm").multiply(Config.ALPHA)
                                .plus(col("trendScore").multiply(Config.BETA))
                );


        // ---------- 7. Top N par utilisateur ----------
        WindowSpec byUser = Window.partitionBy("userId")
                .orderBy(col("finalScore").desc(), col("productId").asc());

        Dataset<Row> top = scored
                .withColumn("rank", row_number().over(byUser))
                .filter(col("rank").leq(Config.TOP_N))
                .select(
                        col("userId"),
                        col("rank"),
                        col("productId"),
                        col("name"),
                        round(col("alsScore"), 2).alias("alsScore"),
                        round(col("alsNorm"), 3).alias("alsNorm"),
                        round(col("trendScore"), 3).alias("trendScore"),
                        round(col("finalScore"), 3).alias("finalScore"))
                .orderBy("userId", "rank")
                .cache();

        // ---------- 8. Sauvegarde ----------
        saveResults(top);

        // ---------- 9. Affichage pour la démo ----------
        printDemo(spark, scored, top);
    }

    /** Mode intégré : Spark écrit dans HDFS. Mode local : écriture en Java pur (pas de winutils). */
    
    /** Sauvegarde locale des recommandations. */
    private static void saveResults(Dataset<Row> top) throws IOException {
        List<Row> rows = top.collectAsList();

        Path file = Paths.get(Config.LOCAL_OUTPUT_FILE);
        if (file.getParent() != null) {
            Files.createDirectories(file.getParent());
        }

        try (PrintWriter out = new PrintWriter(
                Files.newBufferedWriter(file, StandardCharsets.UTF_8))) {
            out.println("userId,rank,productId,name,alsScore,alsNorm,trendScore,finalScore");

            for (Row r : rows) {
                out.println(r.get(0) + "," + r.get(1) + "," + r.get(2) + ","
                        + r.get(3) + "," + r.get(4) + "," + r.get(5) + ","
                        + r.get(6) + "," + r.get(7));
            }
        }

        System.out.println("[Hybrid] " + rows.size()
                + " lignes écrites dans " + Config.LOCAL_OUTPUT_FILE);
    }


    
        private static void printDemo(SparkSession spark, Dataset<Row> scored, Dataset<Row> top) {
        int uid = Config.DEMO_USER_ID;

        List<Row> profiles = readCsv(spark, Config.USERS_PATH)
                .filter(col("userId").equalTo(uid))
                .limit(1)
                .collectAsList();

        if (profiles.isEmpty()) {
                System.out.println("[Hybrid] Demo user " + uid
                        + " not found. Skipping demo display.");
                return;
        }

        Row profile = profiles.get(0);

        System.out.println("\n========== DEMO: user " + uid + " ==========");
        System.out.println("Profile: likes "
                + profile.getString(1) + " + " + profile.getString(2));

        Dataset<Row> demoScored = scored.filter(col("userId").equalTo(uid));
        Dataset<Row> demoTop = top.filter(col("userId").equalTo(uid));

        System.out.println("\n--- ALS only ---");
        if (demoScored.limit(1).count() > 0) {
                demoScored.orderBy(col("alsScore").desc())
                        .limit(Config.TOP_N)
                        .select(col("name"),
                                round(col("alsScore"), 2).alias("alsScore"))
                        .show(false);
        } else {
                System.out.println("No ALS recommendations for this user.");
        }

        System.out.println("--- Hybrid recommendations ---");
        if (demoTop.limit(1).count() > 0) {
                demoTop.select("rank", "name", "alsNorm",
                        "trendScore", "finalScore").show(false);
        } else {
                System.out.println("No hybrid recommendations for this user.");
        }
        }


    private static Dataset<Row> readCsv(SparkSession spark, String path) {
        return spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(path);
    }
}