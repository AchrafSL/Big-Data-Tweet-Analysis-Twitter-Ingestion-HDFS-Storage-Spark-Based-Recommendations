
package recommendation;

/**
 * Configuration centralisee du systeme de recommandation.
 * Toutes les donnees sont gerees localement sur la VM.
 */
public final class Config {

    private Config() {}

    // ---------- Donnees locales ----------
    public static final String DATA_DIR = "data";
    public static final String RATINGS_PATH = "data/ratings.csv";
    public static final String PRODUCTS_PATH = "data/products.csv";
    public static final String USERS_PATH = "data/users.csv";

    // Tendances quotidiennes exportees par TopicTrendAnalyzer
    public static final String TRENDS_PATH = "data/daily_trends.csv";

    // ---------- Modele et resultats ----------
    public static final String MODEL_PATH = "models/recommendation_model";
    public static final String OUTPUT_PATH = "output/recommendations";
    public static final String LOCAL_OUTPUT_FILE = "output/recommendations.csv";

    // ---------- Donnees synthetiques ----------
    public static final int NB_USERS = 1000;
    public static final int RATINGS_PER_USER = 8;
    public static final long SEED = 42L;

    // ---------- Hyperparametres ALS ----------
    public static final int ALS_RANK = 10;
    public static final int ALS_MAX_ITER = 10;
    public static final double ALS_REG = 0.1;

    // ---------- Score hybride ----------
    public static final double ALPHA = 0.7;
    public static final double BETA = 0.3;

    // ---------- Sorties ----------
    public static final int TOP_N = 5;

    // Nombre de tendances utilisees par le moteur.
    public static final int TRENDS_TOP_K = 200;

    public static final int DEMO_USER_ID = 1;
}
