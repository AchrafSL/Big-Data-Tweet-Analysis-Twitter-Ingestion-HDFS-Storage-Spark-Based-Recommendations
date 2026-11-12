# Analyse Big Data de Tweets & Système de Recommandation Hybride

**Version Française** | [English Version](README.md)

Projet académique de fin de module en **Calcul Parallèle et Distribué**, réalisé au sein du Master **Big Data, Intelligence Artificielle et Applications Avancées (BDIAA)** à la Faculté des Sciences de Kénitra, Université Ibn Tofaïl.

---

## Informations Académiques

- **Établissement** : Université Ibn Tofaïl (UIT), Faculté des Sciences de Kénitra
- **Formation** : Master Big Data, Intelligence Artificielle et Applications Avancées (BDIAA)
- **Année Universitaire** : 2025 / 2026
- **Module** : Calcul Parallèle & Distribué
- **Encadrant** : **Pr. Ayoub Allali**
- **Auteurs du Projet** :
  - **Salimi Achraf**
  - **El Mehdi Terech**
  - **Nabil Belaliaa**
  - **Hamza El Kabraoui**

---

## Sommaire

1. [Contexte et Problématique](#contexte-et-problématique)
2. [Objectifs du Projet](#objectifs-du-projet)
3. [Architecture Globale du Système](#architecture-globale-du-système)
4. [Technologies Utilisées](#technologies-utilisées)
5. [Détail des Composants du Pipeline](#détail-des-composants-du-pipeline)
   - [1. Ingestion de Flux avec Kafka (`TweetProducer`)](#1-ingestion-de-flux-avec-kafka-tweetproducer)
   - [2. Traitement Streaming & IA Locale (`TweetTopicExtractor`)](#2-traitement-streaming--ia-locale-tweettopicextractor)
   - [3. Normalisation Sémantique des Thèmes (`topic_service`)](#3-normalisation-sémantique-des-thèmes-topic_service)
   - [4. Stockage Distribué HDFS](#4-stockage-distribué-hdfs)
   - [5. Analyse Batch des Tendances (`TopicTrendAnalyzer`)](#5-analyse-batch-des-tendances-topictrendanalyzer)
   - [6. Moteur de Recommandation Hybride (`recommendation_system`)](#6-moteur-de-recommandation-hybride-recommendation_system)
   - [7. Plateforme Web E-Commerce & Visualisation](#7-plateforme-web-e-commerce--visualisation)
6. [Structure du Dépôt](#structure-du-dépôt)
7. [Résultats Expérimentaux & Évaluation](#résultats-expérimentaux--évaluation)
8. [Guide d'Installation et d'Exécution](#guide-dinstallation-et-dexécution)

---

## Contexte et Problématique

### Contexte
Les réseaux sociaux, et Twitter (X) en particulier, constituent une source ininterrompue de données textuelles reflétant les centres d'intérêt, opinions et engouements mondiaux en temps réel. Pour les acteurs du e-commerce (par exemple les créateurs et vendeurs de produits et services numériques), capter ces tendances à la volée représente un levier stratégique majeur pour aligner leur catalogue sur la demande réelle des consommateurs.

### Problématique
> **Comment concevoir et déployer une architecture Big Data distribuée capable d'ingérer un flux continu de tweets, d'en extraire et normaliser les thématiques via l'IA locale, de stocker ces données dans un cluster distribué, et de combiner ces tendances en temps réel avec un modèle de filtrage collaboratif afin d'alimenter un moteur de recommandation de produits personnalisé ?**

---

## Objectifs du Projet

1. **Ingestion de flux en continu** : Récupérer des tweets volumineux depuis un jeu de données réel et les diffuser via un courtier de messages distribué (**Apache Kafka**).
2. **Traitement distribué en temps réel** : Traiter le flux par micro-lots avec **Apache Spark Structured Streaming**.
3. **Extraction sémantique par LLM local** : Identifier le sujet clé de chaque tweet à l'aide d'un modèle de langage compact (**Gemma 3:4b**) exécuté localement sous **Ollama** (garantissant confidentialité et absence de coûts d'API cloud).
4. **Normalisation vectorielle des thématiques** : Agréger les synonymes et variantes lexicales grâce aux plongements de texte (*embeddings* `all-MiniLM-L6-v2`) et à la similarité cosinus via une API **FastAPI**.
5. **Persistance distribuée** : Sauvegarder les flux traités et thématiques enrichies dans le système de fichiers distribué **Hadoop HDFS**.
6. **Analyse Batch multidimensionnelle** : Calculer et classer les fréquences des tendances selon trois granularités (globale, quotidienne, horaire) avec **Spark Core / SQL**.
7. **Recommandation Hybride personnalisée** : Fusionner le filtrage collaboratif (**Spark MLlib ALS**) et la pertinence sémantique des tendances du moment.
8. **Restitution utilisateur** : Offrir une interface web interactive (**Flask**) pour les acheteurs, les producteurs et les administrateurs du système.

---

## Architecture Globale du Système

Le système est articulé autour d'un pipeline complet allant de la collecte des flux jusqu'à l'interface utilisateur finale :

```text
┌────────────────────────────────────────────────────────────────────────┐
│                        1. SOURCE & INGESTION                           │
│  Dataset Hugging Face (NLP-UniBW/tweets) ──> TweetProducer.java        │
│                                                   │                    │
│                                                   ▼                    │
│                                        Apache Kafka (Topic: tweets)    │
└───────────────────────────────────────────────┬────────────────────────┘
                                                │ Micro-lots (3s)
                                                ▼
┌────────────────────────────────────────────────────────────────────────┐
│                   2. STREAMING, IA LOCALE & HDFS                       │
│                       TweetTopicExtractor.java                         │
│                                   │                                    │
│       ┌───────────────────────────┴───────────────────────────┐        │
│       ▼                                                       ▼        │
│  Ollama Server                                         FastAPI :8000   │
│  (Modèle Gemma 3:4b)                                   all-MiniLM-L6-v2│
│  Extraction du sujet brut                              Normalisation   │
│       │                                                       │        │
│       └───────────────────────────┬───────────────────────────┘        │
│                                   ▼                                    │
│                    Stockage HDFS (/hadoop/tweet_topics/)               │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                    3. ANALYSE BATCH DES TENDANCES                      │
│     TopicTrendAnalyzer.java (Spark Batch : Global / Daily / Hourly)    │
│                                   │                                    │
│                                   ▼                                    │
│                 Trend API Flask (:5001) ──> CSV Exports                │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                   4. MOTEUR DE RECOMMANDATION HYBRIDE                  │
│  Ratings Clients (ratings.csv)      Daily Trends + Embeddings Produits │
│             │                                       │                  │
│             ▼                                       ▼                  │
│     Spark MLlib (ALS)                      SemanticTrendScorer         │
│     Score ALS normalisé                    Score sémantique tendance   │
│             │                                       │                  │
│             └───────────────────┬───────────────────┘                  │
│                                 ▼                                      │
│               HybridRecommender : 0.7*ALS + 0.3*Trend                  │
│                                 │                                      │
│                                 ▼                                      │
│                    Recommendation API Flask (:5002)                    │
└─────────────────────────────────┬──────────────────────────────────────┘
                                  │
                                  ▼
┌────────────────────────────────────────────────────────────────────────┐
│                 5. APPLICATION WEB & RESTITUTION (:5000)               │
│               DamsoStream Web (Flask + Jinja2 + SQLite)                │
│                                                                        │
│   • Page Tendances (/trends)       : Tendances globales, jour, heure   │
│   • Page Recommandations (/ForYou) : Suggestions hybrides adaptées     │
│   • Studio Producteur (/producer)  : KPI, CRUD offres & notes 1-5★     │
│   • Export CSV / Admin (/admin)    : Synchronisation avec Spark        │
└────────────────────────────────────────────────────────────────────────┘
```

---

## Technologies Utilisées

| Domaine | Technologie | Rôle dans le Projet |
|---|---|---|
| **Message Broker** | Apache Kafka | Ingestion et mise en mémoire tampon des flux de tweets en temps réel. |
| **Calcul Distribué** | Apache Spark (Streaming & Batch) | Traitement continu par micro-batches (3s) et agrégations distribuées. |
| **Machine Learning** | Apache Spark MLlib | Entraînement et inférence du modèle de filtrage collaboratif ALS. |
| **Stockage Distribué** | Hadoop HDFS | Persistance distribuée des tweets et des thématiques extraites. |
| **IA & LLM Local** | Ollama & Gemma 3:4b | Extraction thématique locale zéro-shot des tweets en flux continu. |
| **Plongements Vectoriels** | `all-MiniLM-L6-v2` | Encodage des produits et thèmes pour la similarité cosinus sémantique. |
| **Microservices Web** | FastAPI & Flask (Python) | Normalisation thématique (8000), API tendances (5001) et API recommandations (5002). |
| **Langages** | Java 11/17 & Python 3.10+ | Développement du cœur distribué Spark/Kafka et des microservices IA. |
| **Application Web** | Flask, Jinja2, SQLite, CSS3 | Plateforme e-commerce interactive avec tableaux de bord et visualisations. |

---

## Détail des Composants du Pipeline

### 1. Ingestion de Flux avec Kafka (`TweetProducer`)
- **Source** : Dataset Hugging Face `NLP-UniBW/tweets_dataset_jan_feb_big_deduplicated`.
- **Fonctionnement** : La classe Java `TweetProducer` interroge l'API REST de Hugging Face Datasets Server avec pagination (requêtes HTTP GET et reprise automatique en cas d'erreur réseau jusqu'à 3 tentatives).
- **Champs extraits** : `id`, `search_term`, `text` et `date`.
- **Publication** : Les messages sont sérialisés en chaînes UTF-8 et publiés sur le topic Kafka `tweets` (broker `localhost:9092`) avec un délai paramétrable (200 ms par défaut).

### 2. Traitement Streaming & IA Locale (`TweetTopicExtractor`)
- **Moteur de streaming** : Spark Structured Streaming en mode local multicoeur (`local[*]`).
- **Configuration** :
  - `startingOffsets = latest`
  - `maxOffsetsPerTrigger = 10`
  - Fréquence de déclenchement : **toutes les 3 secondes** via `foreachBatch`.
- **Transformation RDD** : Chaque micro-lot est converti en `JavaRDD<Row>`, puis les tweets vides ou nuls sont éliminés par filtrage.
- **Inférence LLM Locale** : Appel HTTP vers le serveur local **Ollama** avec le modèle **Gemma 3:4b** (`callOllama()`), avec une consigne demandant l'attribution d'un thème concis sous forme d'un tableau JSON.
- **Métriques en direct** : Compteurs atomiques thread-safe (`AtomicInteger`) pour suivre le nombre de micro-lots, de tweets valides, de thèmes extraits et de lignes enregistrées.

### 3. Normalisation Sémantique des Thèmes (`topic_service`)
Pour regrouper différentes formulations désignant un même sujet (par ex. *« World Cup »*, *« FIFA WC »*, *« Coupe du monde »*), un microservice FastAPI expose l'endpoint `/normalize` (port 8000) :
- Les thèmes extraits sont vectorisés à l'aide du modèle `all-MiniLM-L6-v2`.
- La similarité cosinus est calculée par rapport à un registre persistant `topic_registry.json`.
- **Règle de décision** :
  - Si **similarité >= 0.80** : le thème est rattaché au thème canonique existant.
  - Si **similarité < 0.80** : un nouveau thème canonique est créé et indexé dans le registre.

### 4. Stockage Distribué HDFS
- Les tweets enrichis (identifiant, terme de recherche, texte, date, thème brut et thème canonique) sont convertis en CSV UTF-8 avec gestion rigoureuse des caractères d'échappement.
- Les fichiers sont sauvegardés dans l'arborescence Hadoop HDFS sous `/hadoop/tweet_topics/`.

### 5. Analyse Batch des Tendances (`TopicTrendAnalyzer`)
- Charge les données depuis HDFS avec Apache Spark.
- Supprime les doublons sur l'identifiant des tweets et élimine les dates invalides.
- Calcule la fréquence d'apparition des thèmes canoniques selon 3 niveaux temporels :
  - **Global** : Tendances générales consolidées sur l'ensemble des données.
  - **Quotidien (Daily)** : Tendances groupées par date (`YYYY-MM-DD`).
  - **Horaire (Hourly)** : Tendances groupées par heure (`YYYY-MM-DD HH:00`).
- Export en CSV et mise à disposition par l'API Flask `trendAPISender` (port 5001) via les routes :
  - `GET /api/trends/global`
  - `GET /api/trends/daily?date=YYYY-MM-DD`
  - `GET /api/trends/hourly?date=YYYY-MM-DD&hour=HH`

### 6. Moteur de Recommandation Hybride (`recommendation_system`)

Le moteur de recommandation concilie les historiques d'évaluation des clients et la fraîcheur des tendances détectées sur les réseaux sociaux :

```text
  [Notes Clients (ratings.csv)]                 [Tendances (daily_trends.csv)]
               │                                               │
               ▼                                               ▼
  [Modèle ALS (Spark MLlib)]                      [Pondération des tendances w_t]
               │                                               │
               ▼                                               ▼
  [Score ALS Prédit (0 à 1)]                      [Embeddings Produits & Thèmes]
  (Normalisation Min-Max)                                      │
               │                                               ▼
               │                                 [Score Sémantique Tendance]
               │                                 (Similarité Cosinus pondérée)
               │                                               │
               └───────────────────────┬───────────────────────┘
                                       ▼
                       [Score Hybride Final du Produit]
                     Score = (0.7 × ALS) + (0.3 × Trend)
                                       │
                                       ▼
                          [Classement & Top-N Produits]
```

#### A. Filtrage Collaboratif ALS
- Algorithme : **Alternating Least Squares (ALS)** de Spark MLlib.
- Découpage : 80% entraînement / 20% test (`seed = 42`).
- Validation croisée à 3 plis évaluant 9 combinaisons de paramètres :
  - `rank` dans {2, 4, 8} (dimension des facteurs latents)
  - `regParam` dans {0.1, 0.3, 1.0} (régularisation)
  - `maxIter = 10`
- **Résultats de validation** :
  - Meilleure configuration : `rank = 2`, `regParam = 0.30`.
  - RMSE du modèle ALS sur l'ensemble de test : **0.975** (contre **0.877** pour la baseline basée sur la moyenne).
- Normalisation Min-Max du score prédit :
  `ALS_norm(u, p) = (Score - Min) / (Max - Min)`

#### B. Scoring Sémantique des Tendances
- Poids de chaque tendance normalisé par la fréquence maximale :
  `w_t = f_t / f_max`
- Plongement vectoriel composite pour chaque produit (nom + catégorie + thème + couleur) généré via `generate_product_embeddings.py`.
- Calcul du score sémantique par similarité cosinus pondérée :
  `TrendScore(p) = Σ [ w_t × max(0, sim(p, t)) ] / Σ w_t`

#### C. Score Hybride Final
`Score Final(u, p) = (0.7 × ALS_norm(u, p)) + (0.3 × TrendScore(p))`

Les résultats sont sauvegardés dans `output/recommendations.csv` et exposés par `recommendation_api.py` (port 5002).

### 7. Plateforme Web E-Commerce & Visualisation

L'application web (`presentation_website/UIT_Final_Project-SynchroWork`, port 5000) fournit l'interface utilisateur pour consulter et interagir avec les résultats :
- **Page Tendances (`/trends`)** : Affichage interactif des tendances extraites des tweets (vues globale, journalière et horaire) avec distinction entre données simulées et résultats réels Spark/Kafka.
- **Page Recommandations personnalisées (`/recommendations` ou "For You")** : Suggestions de produits adaptées aux préférences de l'utilisateur (thème, couleur) et boostées par les tendances Twitter.
- **Studio Producteur (`/producer/dashboard` & `/producer/ratings`)** : Interface de gestion du catalogue pour les producteurs avec suivi des métriques de vente et répartition détaillée des avis de 1 à 5 étoiles.
- **Export CSV / Administration (`/admin/csv`)** : Studio d'exportation atomique des jeux de données (`products.csv`, `users.csv`, `ratings.csv`) pour réentraîner le modèle Spark.

---

## Structure du Dépôt

```text
calcule parallele/projet/
│
├── Systeme_de_Recommandation_Tweets.docx  # Rapport académique officiel du projet
├── README.md                              # Documentation générale du projet
│
├── project_on_linux/                      # Pipeline Big Data & Calcul Distribué
│   └── project_calcule_parallele/
│       ├── pom.xml                        # Configuration Maven (Spark, Kafka, Jackson)
│       │
│       ├── src/main/java/                 # Modules de streaming et de traitement batch
│       │   ├── TweetProducer.java         # Ingestion depuis Hugging Face vers Kafka
│       │   ├── TweetConsumer.java         # Consommateur Kafka de test et vérification
│       │   ├── TweetTopicExtractor.java   # Spark Streaming + Inférence Ollama + HDFS
│       │   └── TopicTrendAnalyzer.java    # Analyse Batch Spark des tendances (HDFS -> CSV)
│       │
│       ├── topic_service/                 # Service de normalisation sémantique
│       │   ├── app.py                     # API FastAPI (/normalize sur le port 8000)
│       │   └── topic_registry.json        # Registre des thèmes canoniques indexés
│       │
│       ├── trendAPISender/                # Passerelle REST des tendances Spark
│       │   └── trend_api.py               # API Flask (port 5001) déclenchant TopicTrendAnalyzer
│       │
│       ├── recommendation_system/         # Moteur de recommandation Spark MLlib
│       │   ├── pom.xml                    # Dépendances Maven (Spark MLlib, Core)
│       │   ├── generate_product_embeddings.py # Calcul des vecteurs produits all-MiniLM
│       │   ├── recommendation_api.py      # API Flask (port 5002) pour les recommandations
│       │   ├── data/                      # Données d'entrée (users, products, ratings)
│       │   ├── models/                    # Modèle ALS sauvegardé au format Parquet
│       │   ├── output/                    # Sorties des recommandations (recommendations.csv)
│       │   └── src/main/java/recommendation/
│       │       ├── AlsTrainer.java        # Entraînement et validation croisée de l'ALS
│       │       ├── TrendsLoader.java      # Normalisation des tendances journalières
│       │       ├── SemanticTrendScorer.java # Calcul de similarité cosinus pondérée
│       │       ├── HybridRecommender.java # Fusion hybride (0.7 ALS + 0.3 Trends)
│       │       ├── Config.java            # Constantes et chemins de configuration
│       │       └── SparkFactory.java      # Initialisation de SparkSession
│       │
│       └── exports/                       # Résultats exportés par Spark (global, daily, hourly)
│
└── presentation_website/                  # Interface Web E-Commerce & Démonstrateur
    └── UIT_Final_Project-SynchroWork/
        ├── app.py                         # Serveur Flask principal (port 5000)
        ├── db_init.py                     # Initialisation du schéma SQLite damsostream.db
        ├── csv_generator.py               # Exportateur atomique de CSV pour Spark
        ├── migrate_users.py               # Seeder des comptes utilisateurs
        ├── damsostream.db                 # Base de données SQLite locale
        ├── requirements.txt               # Dépendances Python de l'application web
        │
        ├── csv_exports/                   # Données synchronisées avec Spark
        │   ├── products.csv
        │   ├── users.csv
        │   ├── ratings.csv
        │   └── global_trends.csv
        │
        ├── templates/                     # Vues Jinja2 (Trends, Recommendations, Studio, etc.)
        └── static/                        # Feuilles de style (CSS), JavaScript et images
```

---

## Résultats Expérimentaux & Évaluation

### 1. Évaluation du Filtrage Collaboratif ALS
L'évaluation sur l'échantillon test (20% des évaluations) donne les résultats suivants :

| Paramètre / Métrique | Valeur |
|---|---|
| Dimension latente optimale (`rank`) | **2** |
| Paramètre de régularisation (`regParam`) | **0.30** |
| Nombre d'itérations | **10** |
| Plis de validation croisée | **3 plis** |
| **RMSE Modèle ALS** | **0.975** |
| **RMSE Baseline (moyenne globale)** | **0.877** |

> *Observation* : En raison du volume réduit et de la sparsité du jeu de test initial, la baseline fondée sur la note moyenne obtient une RMSE légèrement inférieure à celle de l'ALS. Le modèle ALS apporte néanmoins la personnalisation indispensable pour différencier les goûts individuels des utilisateurs.

### 2. Exemple de Recommandation Produit Top-5
Pour un profil utilisateur donné, les scores prédits bruts par ALS se présentent comme suit avant combinaison sémantique :

| Rang | ID Produit | Score ALS Prédit |
|:---:|:---:|:---:|
| 1 | `3` | 2.8379 |
| 2 | `2` | 2.8177 |
| 3 | `1` | 2.8140 |
| 4 | `4` | 2.7422 |
| 5 | `15` | 2.6452 |

Après pondération hybride (`Score = 0.7 × ALS_norm + 0.3 × TrendScore`), les produits en forte affinité avec les sujets populaires du moment sur Twitter voient leur classement remonter, assurant une recommandation à la fois personnalisée et en phase avec les tendances actuelles.

---

## Guide d'Installation et d'Exécution

### Prérequis Système
- **Système d'exploitation** : Linux (Ubuntu recommandé pour le cluster) ou Windows avec environnement distribué.
- **Java** : JDK 11 ou 17.
- **Build Tool** : Apache Maven 3.8+.
- **Python** : Python 3.10 ou supérieur.
- **Services pré-installés** :
  - **Apache Kafka** & **Zookeeper**.
  - **Hadoop HDFS**.
  - **Ollama** avec le modèle téléchargé : `ollama pull gemma3:4b`.

---

### Étape 1 : Démarrer l'Infrastructure
Dans des terminaux séparés sur la machine Linux :

```bash
# 1. Démarrer Zookeeper et Kafka Broker
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties

# 2. Démarrer HDFS NameNode & DataNode
start-dfs.sh

# 3. Démarrer le serveur Ollama
ollama serve
```

---

### Étape 2 : Lancer la Normalisation des Thèmes (FastAPI)

```bash
cd project_on_linux/project_calcule_parallele/topic_service
pip install fastapi uvicorn sentence-transformers scikit-learn
uvicorn app:app --host 127.0.0.1 --port 8000
```

---

### Étape 3 : Lancer l'Ingestion et le Streaming Spark

```bash
cd project_on_linux/project_calcule_parallele
mvn clean package -DskipTests

# 1. Démarrer le traitement en streaming (Spark Streaming + Ollama + HDFS)
spark-submit --class TweetTopicExtractor \
  --master "local[*]" \
  target/project_calcule_parallele-1.0-SNAPSHOT.jar

# 2. Dans un autre terminal, lancer le producteur Kafka
java -cp target/project_calcule_parallele-1.0-SNAPSHOT.jar TweetProducer
```

---

### Étape 4 : Lancer l'API des Tendances & le Moteur de Recommandation

```bash
# 1. Démarrer l'API des tendances Spark (Port 5001)
cd project_on_linux/project_calcule_parallele/trendAPISender
pip install flask
python trend_api.py

# 2. Démarrer l'API de recommandation hybride (Port 5002)
cd ../recommendation_system
pip install flask sentence-transformers
python generate_product_embeddings.py
python recommendation_api.py
```

---

### Étape 5 : Lancer la Plateforme Web (Démonstrateur)

```powershell
cd presentation_website/UIT_Final_Project-SynchroWork

# 1. Installer les dépendances Python
pip install -r requirements.txt

# 2. Initialiser la base SQLite locale et les utilisateurs
python db_init.py
python migrate_users.py

# 3. Générer les CSV initiaux
python csv_generator.py

# 4. Lancer le serveur web
python app.py
```

Accéder ensuite à l'application web via le navigateur : **`http://127.0.0.1:5000`**

#### Comptes de Démonstration Disponibles :
- **Administrateur** : `achraf.salimi@uit.ac.ma` / `Salimi@01` (Gestion du catalogue et panneau `/admin/csv`)
- **Producteur** : `producer@damsostream.com` / `Producer123!` (Gestion des offres et dashboard des notes)
- **Client** : `test.client@damsostream.com` / `TestPassword123!` (Navigation, notations 1-5 étoiles, recommandations "For You")

---

## Licence & Cadre Académique

Ce projet est développé à des fins pédagogiques et académiques dans le cadre du cursus de Master BDIAA à l'**Université Ibn Tofaïl (Faculté des Sciences de Kénitra)**. Tous droits réservés aux auteurs et à l'équipe pédagogique.
