package recommendation;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.ml.recommendation.ALSModel;

import java.nio.file.Files;
import java.nio.file.Paths;

public class Main {

    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0] : "serve";

        System.out.println("Java: " + System.getProperty("java.version")
                + " | " + System.getProperty("java.home")
                + " | Mode: " + mode);

        // Do not generate synthetic data here.
        SparkSession spark = SparkFactory.create("Recommendation-Pipeline");

        try {
            ALSModel model;

            if ("refresh".equalsIgnoreCase(mode)) {
                System.out.println("[Main] Training a fresh ALS model...");
                model = AlsTrainer.run(spark);
                System.out.println("[Main] Fresh ALS model saved.");

            } else if ("serve".equalsIgnoreCase(mode)) {
                if (!Files.exists(Paths.get(Config.MODEL_PATH, "metadata"))) {
                    throw new IllegalStateException(
                            "No saved ALS model. Run refresh first.");
                }

                System.out.println("[Main] Loading existing ALS model...");
                model = ALSModel.load(Config.MODEL_PATH);

            } else {
                throw new IllegalArgumentException(
                        "Unknown mode: " + mode
                                + ". Use 'refresh' or 'serve'.");
            }

            HybridRecommender.run(spark, model);

        } finally {
            spark.stop();
        }
    }
}
