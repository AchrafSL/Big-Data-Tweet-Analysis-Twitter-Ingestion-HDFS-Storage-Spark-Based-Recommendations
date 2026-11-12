package recommendation;

import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.pow;
import static org.apache.spark.sql.functions.sqrt;

public class AlsTrainer {

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkFactory.create("ALS-Trainer");
        try {
            run(spark);
        } finally {
            spark.stop();
        }
    }

    public static ALSModel run(SparkSession spark) throws Exception {

        // 1. Chargement des interactions
        Dataset<Row> ratings = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(Config.RATINGS_PATH)
                .cache();
        System.out.println("[AlsTrainer] Nombre d'interactions : " + ratings.count());

        // 2. Séparation entraînement (80 %) / test (20 %)
        Dataset<Row>[] splits = ratings.randomSplit(new double[]{0.8, 0.2}, Config.SEED);
        Dataset<Row> training = splits[0];
        Dataset<Row> test = splits[1];

        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setMetricName("rmse")
                .setLabelCol("rating")
                .setPredictionCol("prediction");

        // 3. Référence : prédire toujours la note moyenne
        double meanRating = training.agg(avg("rating")).first().getDouble(0);
        double baselineRmse = test
                .select(sqrt(avg(pow(col("rating").minus(meanRating), 2.0))).alias("rmse"))
                .first().getDouble(0);

        // 4. Recherche automatique de rank et regParam (validation croisée, 3 plis)
        ALS als = buildAls(Config.ALS_RANK, Config.ALS_REG);
        ParamMap[] grid = new ParamGridBuilder()
                .addGrid(als.rank(), new int[]{2, 4, 8})
                .addGrid(als.regParam(), new double[]{0.1, 0.3, 1.0})
                .build();

        CrossValidator cv = new CrossValidator()
                .setEstimator(als)
                .setEstimatorParamMaps(grid)
                .setEvaluator(evaluator)
                .setNumFolds(3)
                .setSeed(Config.SEED);

        System.out.println("[AlsTrainer] Recherche des hyperparamètres (9 combinaisons x 3 plis)...");
        CrossValidatorModel cvModel = cv.fit(training);

        ALSModel best = (ALSModel) cvModel.bestModel();
        int bestRank = best.rank();
        double bestReg = ((ALS) best.parent()).getRegParam();

        // 5. RMSE du meilleur modèle sur les 20 % jamais vus
        double rmse = evaluator.evaluate(best.transform(test));
        System.out.printf("[AlsTrainer] Meilleurs paramètres : rank=%d, regParam=%.2f%n", bestRank, bestReg);
        System.out.printf("[AlsTrainer] RMSE ALS            = %.3f%n", rmse);
        System.out.printf("[AlsTrainer] RMSE note moyenne   = %.3f (référence à battre)%n", baselineRmse);

        // 6. Modèle final : mêmes paramètres, entraîné sur 100 % des données
        ALSModel finalModel = buildAls(bestRank, bestReg).fit(ratings);

        // 7. Sauvegarde : uniquement en mode intégré (HDFS). En local, le modèle reste en mémoire.
        finalModel.write().overwrite().save(Config.MODEL_PATH);
        System.out.println("[AlsTrainer] Modèle sauvegardé dans " + Config.MODEL_PATH);
    

        finalModel.recommendForAllUsers(5).show(5, false);
        return finalModel;
    }

    private static ALS buildAls(int rank, double regParam) {
        return new ALS()
                .setMaxIter(Config.ALS_MAX_ITER)
                .setRank(rank)
                .setRegParam(regParam)
                .setUserCol("userId")
                .setItemCol("productId")
                .setRatingCol("rating")
                .setSeed(Config.SEED)
                .setColdStartStrategy("drop");
    }
}