package recommendation;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Génère des données synthétiques réalistes :
 * chaque utilisateur a un thème et une couleur préférés (cachés au modèle),
 * et ses notes en dépendent. ALS doit retrouver ces goûts à partir des notes seules.
 */
public class DataGenerator {

    // id, name, category, color, theme
    private static final String[][] PRODUCTS = {
            {"1",  "Football T-shirt",             "sport",   "white", "football"},
            {"2",  "Red Morocco Football T-shirt", "sport",   "red",   "football"},
            {"3",  "Red Football Jersey",          "sport",   "red",   "football"},
            {"4",  "Green Morocco Jersey",         "sport",   "green", "football"},
            {"5",  "Sports T-shirt",               "sport",   "blue",  "sport"},
            {"6",  "Running Dry-Fit T-shirt",      "sport",   "black", "running"},
            {"7",  "Gaming T-shirt",               "gaming",  "black", "gaming"},
            {"8",  "Retro Pixel Gaming T-shirt",   "gaming",  "green", "gaming"},
            {"9",  "Black Minimalist T-shirt",     "casual",  "black", "minimal"},
            {"10", "White Minimalist T-shirt",     "casual",  "white", "minimal"},
            {"11", "Beige Basic T-shirt",          "casual",  "beige", "minimal"},
            {"12", "Oversized Streetwear T-shirt", "fashion", "black", "streetwear"},
            {"13", "Red Streetwear T-shirt",       "fashion", "red",   "streetwear"},
            {"14", "Zellige Print T-shirt",        "fashion", "blue",  "moroccan"},
            {"15", "Moroccan Flag T-shirt",        "fashion", "red",   "moroccan"},
            {"16", "Music Band T-shirt",           "fashion", "black", "music"}
    };

    private static final String[] THEMES = {
            "football", "sport", "running", "gaming",
            "minimal", "streetwear", "moroccan", "music"
    };

    private static final String[] COLORS = {
            "red", "green", "blue", "black", "white", "beige"
    };

    public static void main(String[] args) throws IOException {
        run();
    }

        
    
    public static void run() throws IOException {
        Files.createDirectories(Paths.get(Config.DATA_DIR));

        Random rnd = new Random(Config.SEED);

        if (!Files.exists(Paths.get(Config.PRODUCTS_PATH))
                || !Files.exists(Paths.get(Config.USERS_PATH))
                || !Files.exists(Paths.get(Config.RATINGS_PATH))) {

            System.out.println("[DataGenerator] Missing data files. Generating data...");
            writeProducts();
            writeUsersAndRatings(rnd);
        } else {
            System.out.println("[DataGenerator] Existing products, users and ratings preserved.");
        }
    }



    private static void writeProducts() throws IOException {
        try (PrintWriter out = writer(Config.PRODUCTS_PATH)) {
            out.println("productId,name,category,color,theme");
            for (String[] p : PRODUCTS) {
                out.println(String.join(",", p));
            }
        }
    }

    private static void writeUsersAndRatings(Random rnd) throws IOException {
        try (PrintWriter users = writer(Config.USERS_PATH);
             PrintWriter ratings = writer(Config.RATINGS_PATH)) {

            users.println("userId,favTheme,favColor");
            ratings.println("userId,productId,rating");

            for (int u = 1; u <= Config.NB_USERS; u++) {

                // L'utilisateur 1 est fixé : il aime le foot et le rouge (exemple du plan)
                String favTheme = (u == 1) ? "football" : THEMES[rnd.nextInt(THEMES.length)];
                String favColor = (u == 1) ? "red"      : COLORS[rnd.nextInt(COLORS.length)];
                users.println(u + "," + favTheme + "," + favColor);

                // On choisit au hasard les produits que cet utilisateur a notés
                List<Integer> indexes = new ArrayList<>();
                for (int i = 0; i < PRODUCTS.length; i++) indexes.add(i);
                Collections.shuffle(indexes, rnd);

                for (int k = 0; k < Config.RATINGS_PER_USER; k++) {
                    String[] p = PRODUCTS[indexes.get(k)];
                    String productId = p[0];
                    String color     = p[3];
                    String theme     = p[4];

                    // Note de base + bonus d'affinité + bruit
                    double score = 2.0;
                    if (theme.equals(favTheme)) score += 2.0;
                    if (color.equals(favColor)) score += 1.0;
                    score += rnd.nextGaussian() * 0.4;

                    long rounded = Math.round(score);
                    int rating = (int) Math.max(1L, Math.min(5L, rounded)); // borné à [1, 5]

                    ratings.println(u + "," + productId + "," + rating);
                }
            }
        }
    }

    // Tendances fictives, même format que celui attendu de Personne 2 : topic,frequency
    private static void writeTrendsMock() throws IOException {
        try (PrintWriter out = writer(Config.TRENDS_PATH)) {
            out.println("topic,frequency");
            out.println("football,150");
            out.println("morocco,120");
            out.println("red,95");
            out.println("fashion,80");
        }
    }

    private static PrintWriter writer(String path) throws IOException {
        return new PrintWriter(Files.newBufferedWriter(Paths.get(path), StandardCharsets.UTF_8));
    }
}