# Big Data Tweet Analytics & Hybrid Recommendation Engine

[Version Française](README.md) | **English Version**

Academic final project for the **Parallel and Distributed Computing** course, completed within the **Master's in Big Data, Artificial Intelligence, and Advanced Applications (BDIAA)** at the Faculty of Sciences of Kenitra, Ibn Tofail University.

---

## Academic Information

- **Institution**: Ibn Tofail University (UIT), Faculty of Sciences, Kenitra
- **Program**: Master's in Big Data, Artificial Intelligence, and Advanced Applications (BDIAA)
- **Academic Year**: 2025 / 2026
- **Course**: Parallel & Distributed Computing
- **Supervisor**: **Prof. Ayoub Allali**
- **Project Authors**:
  - **Salimi Achraf**
  - **El Mehdi Terech**
  - **Nabil Belaliaa**
  - **Hamza El Kabraoui**

---

## Table of Contents

1. [Context and Problem Statement](#context-and-problem-statement)
2. [Project Objectives](#project-objectives)
3. [Global System Architecture](#global-system-architecture)
4. [Technologies Used](#technologies-used)
5. [Pipeline Component Breakdown](#pipeline-component-breakdown)
   - [1. Stream Ingestion with Kafka (`TweetProducer`)](#1-stream-ingestion-with-kafka-tweetproducer)
   - [2. Streaming Processing & Local LLM (`TweetTopicExtractor`)](#2-streaming-processing--local-llm-tweettopicextractor)
   - [3. Semantic Topic Normalization (`topic_service`)](#3-semantic-topic-normalization-topic_service)
   - [4. Distributed Storage with HDFS](#4-distributed-storage-with-hdfs)
   - [5. Batch Trend Analysis (`TopicTrendAnalyzer`)](#5-batch-trend-analysis-topictrendanalyzer)
   - [6. Hybrid Recommendation Engine (`recommendation_system`)](#6-hybrid-recommendation-engine-recommendation_system)
   - [7. Web E-Commerce Platform & Visualization](#7-web-e-commerce-platform--visualization)
6. [Repository Structure](#repository-structure)
7. [Experimental Results & Evaluation](#experimental-results--evaluation)
8. [Installation and Execution Guide](#installation-and-execution-guide)

---

## Context and Problem Statement

### Context
Social networks, particularly Twitter (X), represent an uninterrupted stream of textual data reflecting global interests, opinions, and trending events in real time. For e-commerce businesses (such as creators and sellers of digital services or branded apparel), capturing these trends on the fly provides a crucial competitive advantage to align catalog offerings with immediate customer demand.

### Problem Statement
> **How can we design and deploy a distributed Big Data architecture capable of ingesting a live stream of tweets, extracting and normalizing topics using local AI, persisting data across a distributed cluster, and combining these real-time trends with collaborative filtering to power a personalized product recommendation engine?**

---

## Project Objectives

1. **Continuous Stream Ingestion**: Stream real-world tweet datasets through a distributed message broker (**Apache Kafka**).
2. **Distributed Real-Time Processing**: Process streams via micro-batches using **Apache Spark Structured Streaming**.
3. **Semantic Extraction with Local LLM**: Extract the core topic of each tweet using a lightweight, locally hosted language model (**Gemma 3:4b**) through **Ollama**, ensuring zero cloud API costs and complete data privacy.
4. **Vector Topic Normalization**: Group synonyms and lexical variations using text embeddings (`all-MiniLM-L6-v2`) and cosine similarity via a **FastAPI** microservice.
5. **Distributed Storage**: Persist processed tweet records and normalized topics in the **Hadoop HDFS** distributed filesystem.
6. **Multi-dimensional Batch Analytics**: Compute and rank topic frequencies across three temporal granularities (Global, Daily, Hourly) with **Spark Core / SQL**.
7. **Personalized Hybrid Recommendation**: Blend collaborative filtering (**Spark MLlib ALS**) with the semantic relevance of active market trends.
8. **End-User Presentation**: Deliver a full-featured web interface (**Flask**) for customers, producers, and administrators.

---

## Global System Architecture

The project implements an end-to-end pipeline connecting data streaming to user-facing dashboards:

```text
┌────────────────────────────────────────────────────────────────────────┐
│                        1. SOURCE & INGESTION                           │
│  Hugging Face Dataset (NLP-UniBW/tweets) ──> TweetProducer.java        │
│                                                   │                    │
│                                                   ▼                    │
│                                        Apache Kafka (Topic: tweets)    │
└───────────────────────────────────────────────┬────────────────────────┘
                                                │ Micro-batches (3s)
                                                ▼
┌────────────────────────────────────────────────────────────────────────┐
│                   2. STREAMING, LOCAL AI & HDFS                        │
│                       TweetTopicExtractor.java                         │
│                                   │                                    │
│       ┌───────────────────────────┴───────────────────────────┐        │
│       ▼                                                       ▼        │
│  Ollama Server                                         FastAPI :8000   │
│  (Gemma 3:4b Model)                                    all-MiniLM-L6-v2│
│  Raw topic extraction                                  Semantic Norm.  │
│       │                                                       │        │
│       └───────────────────────────┬───────────────────────────┘        │
│                                   ▼                                    │
│                    HDFS Storage (/hadoop/tweet_topics/)                │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                       3. BATCH TREND ANALYTICS                         │
│     TopicTrendAnalyzer.java (Spark Batch: Global / Daily / Hourly)     │
│                                   │                                    │
│                                   ▼                                    │
│                 Trend API Flask (:5001) ──> CSV Exports                │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                   4. HYBRID RECOMMENDATION ENGINE                      │
│  User Ratings (ratings.csv)         Daily Trends + Product Embeddings  │
│             │                                       │                  │
│             ▼                                       ▼                  │
│     Spark MLlib (ALS)                      SemanticTrendScorer         │
│     Normalized ALS score                   Semantic trend score        │
│             │                                       │                  │
│             └───────────────────┬───────────────────┘                  │
│                                 ▼                                      │
│               HybridRecommender: 0.7*ALS + 0.3*Trend                   │
│                                 │                                      │
│                                 ▼                                      │
│                    Recommendation API Flask (:5002)                    │
└─────────────────────────────────┬──────────────────────────────────────┘
                                  │
                                  ▼
┌────────────────────────────────────────────────────────────────────────┐
│                 5. WEB APPLICATION & PRESENTATION (:5000)              │
│               DamsoStream Web (Flask + Jinja2 + SQLite)                │
│                                                                        │
│   • Trends Explorer (/trends)      : Global, daily & hourly views      │
│   • Personalized Feed (/ForYou)    : Hybrid recommendations            │
│   • Producer Studio (/producer)    : Catalog CRUD & 1-5★ breakdown     │
│   • CSV Export Panel (/admin)      : Cluster synchronization           │
└────────────────────────────────────────────────────────────────────────┘
```

---

## Technologies Used

| Domain | Technology | Project Role |
|---|---|---|
| **Message Broker** | Apache Kafka | Ingestion and real-time buffering of high-throughput tweet streams. |
| **Distributed Processing**| Apache Spark (Streaming & Batch) | Continuous micro-batch processing (3s trigger) and distributed batch aggregations. |
| **Machine Learning** | Apache Spark MLlib | Collaborative filtering training and inference using Alternating Least Squares (ALS). |
| **Distributed Storage** | Hadoop HDFS | Distributed filesystem preserving enriched tweet records and normalized topics. |
| **Local LLM & AI** | Ollama & Gemma 3:4b | On-premise zero-shot topic extraction from live streaming tweets. |
| **Vector Embeddings** | `all-MiniLM-L6-v2` | Sentence-transformers for encoding products and topics for cosine similarity. |
| **Microservice APIs** | FastAPI & Flask (Python) | Topic normalization (8000), Trend API (5001), and Recommendation API (5002). |
| **Languages** | Java 11/17 & Python 3.10+ | Core distributed pipeline (Spark/Kafka) and supporting AI microservices. |
| **Web Platform** | Flask, Jinja2, SQLite, CSS3 | Complete e-commerce web platform with responsive dashboards and review analytics. |

---

## Pipeline Component Breakdown

### 1. Stream Ingestion with Kafka (`TweetProducer`)
- **Dataset Source**: Hugging Face `NLP-UniBW/tweets_dataset_jan_feb_big_deduplicated`.
- **Mechanism**: The Java class `TweetProducer` queries the Hugging Face Datasets Server REST API using pagination (HTTP GET requests with exponential backoff handling up to 3 retry attempts).
- **Extracted Fields**: `id`, `search_term`, `text`, and `date`.
- **Publication**: Messages are serialized as UTF-8 strings and published to the Kafka topic `tweets` (`localhost:9092`) with a configurable delay (default: 200 ms).

### 2. Streaming Processing & Local LLM (`TweetTopicExtractor`)
- **Streaming Engine**: Spark Structured Streaming running locally in multi-core mode (`local[*]`).
- **Configuration**:
  - `startingOffsets = latest`
  - `maxOffsetsPerTrigger = 10`
  - Trigger interval: **every 3 seconds** via `foreachBatch`.
- **RDD Transformation**: Each micro-batch is converted into a `JavaRDD<Row>`, filtering out empty or null text fields.
- **Local LLM Inference**: Direct HTTP calls to local **Ollama** running **Gemma 3:4b** (`callOllama()`), prompting the model to assign a concise topic per tweet returned as a JSON array.
- **Live Metrics Tracking**: Thread-safe atomic counters (`AtomicInteger`) track processed micro-batches, valid tweets, extracted topics, normalized topics, and records saved to HDFS.

### 3. Semantic Topic Normalization (`topic_service`)
To consolidate diverse formulations of the same subject (e.g., *"World Cup"*, *"FIFA WC"*, *"Coupe du monde"*), a FastAPI microservice exposes `/normalize` (port 8000):
- Extracted topics are vectorized using the `all-MiniLM-L6-v2` model.
- Cosine similarity is computed against a persistent canonical registry in `topic_registry.json`.
- **Decision Rule**:
  - If **similarity >= 0.80**: The topic is mapped to the existing canonical topic.
  - If **similarity < 0.80**: A new canonical topic is created and indexed.

### 4. Distributed Storage with HDFS
- Enriched tweet records (`id`, `search_term`, `text`, `date`, raw topic, canonical topic) are formatted into clean, escaped UTF-8 CSV rows.
- Files are saved to the Hadoop distributed filesystem at `/hadoop/tweet_topics/`.

### 5. Batch Trend Analysis (`TopicTrendAnalyzer`)
- Reads the batch files from HDFS using Apache Spark.
- Deduplicates tweets based on `id` and removes missing/invalid dates.
- Aggregates canonical topic frequencies across three temporal dimensions:
  - **Global**: Consolidated topics across the entire dataset.
  - **Daily**: Topics grouped by calendar date (`YYYY-MM-DD`).
  - **Hourly**: Topics grouped by hour (`YYYY-MM-DD HH:00`).
- Automatically exports to CSV and serves them through the Flask `trendAPISender` API (port 5001):
  - `GET /api/trends/global`
  - `GET /api/trends/daily?date=YYYY-MM-DD`
  - `GET /api/trends/hourly?date=YYYY-MM-DD&hour=HH`

### 6. Hybrid Recommendation Engine (`recommendation_system`)

The hybrid engine balances user rating history with current real-time market trends:

```text
  [Customer Ratings (ratings.csv)]              [HDFS Trends (daily_trends.csv)]
               │                                               │
               ▼                                               ▼
  [ALS Model (Spark MLlib)]                       [Trend Weight Normalization w_t]
               │                                               │
               ▼                                               ▼
  [Predicted ALS Score (0 to 1)]                  [Product & Topic Embeddings]
  (Min-Max Normalization)                                      │
               │                                               ▼
               │                                 [Semantic Trend Score]
               │                                 (Weighted Cosine Similarity)
               │                                               │
               └───────────────────────┬───────────────────────┘
                                       ▼
                         [Final Hybrid Product Score]
                     Score = (0.7 × ALS) + (0.3 × Trend)
                                       │
                                       ▼
                          [Ranking & Top-N Products]
```

#### A. Collaborative Filtering with ALS
- Algorithm: **Alternating Least Squares (ALS)** from Spark MLlib.
- Split: 80% training / 20% testing (`seed = 42`).
- 3-Fold Cross-Validation evaluating 9 hyperparameter configurations:
  - `rank` in {2, 4, 8} (latent factor dimensionality)
  - `regParam` in {0.1, 0.3, 1.0} (regularization parameter)
  - `maxIter = 10`
- **Validation Results**:
  - Best model: `rank = 2`, `regParam = 0.30`.
  - Test RMSE: **0.975** (compared to **0.877** for the global mean baseline).
- Min-Max score normalization:
  `ALS_norm(u, p) = (Predicted_Rating - S_min) / (S_max - S_min)`

#### B. Semantic Trend Scoring
- Trend weight normalized by maximum observed frequency:
  `w_t = f_t / f_max`
- Composite product text embeddings (combining name, category, theme, and color) generated via `generate_product_embeddings.py`.
- Weighted cosine similarity score:
  `TrendScore(p) = Σ [ w_t × max(0, sim(p, t)) ] / Σ w_t`

#### C. Hybrid Score Combination
`FinalScore(u, p) = (0.7 × ALS_norm(u, p)) + (0.3 × TrendScore(p))`

Results are saved to `output/recommendations.csv` and served by `recommendation_api.py` (port 5002).

### 7. Web E-Commerce Platform & Visualization

The web platform (`presentation_website/UIT_Final_Project-SynchroWork`, port 5000) provides an intuitive interface for end users:
- **Trends Explorer (`/trends`)**: Interactive visualization of topics extracted from tweets (global, daily, and hourly views) distinguishing between simulated mock data and live Spark/Kafka results.
- **Personalized Recommendations (`/recommendations` or "For You")**: Curated product recommendations reflecting the user's favorite theme and color preferences, boosted by live Twitter trends.
- **Producer Studio (`/producer/dashboard` & `/producer/ratings`)**: Catalog management for service producers, featuring sales KPIs and a detailed 1–5 star rating distribution bar chart.
- **Admin CSV Studio (`/admin/csv`)**: Atomic export utilities for datasets (`products.csv`, `users.csv`, `ratings.csv`) to trigger Spark model retraining.

---

## Repository Structure

```text
calcule parallele/projet/
│
├── Systeme_de_Recommandation_Tweets.docx  # Official academic project report
├── README.md                              # French documentation
├── README_EN.md                           # English documentation
│
├── project_on_linux/                      # Big Data & Distributed Computing Pipeline
│   └── project_calcule_parallele/
│       ├── pom.xml                        # Maven configuration (Spark, Kafka, Jackson)
│       │
│       ├── src/main/java/                 # Streaming and batch modules
│       │   ├── TweetProducer.java         # Ingestion from Hugging Face to Kafka
│       │   ├── TweetConsumer.java         # Kafka validation consumer
│       │   ├── TweetTopicExtractor.java   # Spark Streaming + Ollama LLM + HDFS
│       │   └── TopicTrendAnalyzer.java    # Spark Batch trend analysis (HDFS -> CSV)
│       │
│       ├── topic_service/                 # Semantic normalization service
│       │   ├── app.py                     # FastAPI server (/normalize on port 8000)
│       │   └── topic_registry.json        # Indexed canonical topic registry
│       │
│       ├── trendAPISender/                # REST gateway for Spark trends
│       │   └── trend_api.py               # Flask API (port 5001) executing TopicTrendAnalyzer
│       │
│       ├── recommendation_system/         # Spark MLlib recommendation engine
│       │   ├── pom.xml                    # Maven dependencies (Spark MLlib, Core)
│       │   ├── generate_product_embeddings.py # all-MiniLM product embedding generation
│       │   ├── recommendation_api.py      # Flask API (port 5002) for recommendations
│       │   ├── data/                      # Input datasets (users, products, ratings)
│       │   ├── models/                    # Serialized Parquet ALS model
│       │   ├── output/                    # Recommendation output (recommendations.csv)
│       │   └── src/main/java/recommendation/
│       │       ├── AlsTrainer.java        # ALS training & cross-validation
│       │       ├── TrendsLoader.java      # Daily trend weight normalizer
│       │       ├── SemanticTrendScorer.java # Weighted cosine similarity calculator
│       │       ├── HybridRecommender.java # Hybrid fusion (0.7 ALS + 0.3 Trends)
│       │       ├── Config.java            # Paths and tuning constants
│       │       └── SparkFactory.java      # SparkSession factory
│       │
│       └── exports/                       # Spark exported results (global, daily, hourly)
│
└── presentation_website/                  # Web E-Commerce Demonstration Platform
    └── UIT_Final_Project-SynchroWork/
        ├── app.py                         # Main Flask server (port 5000)
        ├── db_init.py                     # SQLite schema initialization (damsostream.db)
        ├── csv_generator.py               # Atomic CSV exporter for Spark pipelines
        ├── migrate_users.py               # User account seeder
        ├── damsostream.db                 # Local SQLite database
        ├── requirements.txt               # Web application Python dependencies
        │
        ├── csv_exports/                   # Datasets synchronized with Spark
        │   ├── products.csv
        │   ├── users.csv
        │   ├── ratings.csv
        │   └── global_trends.csv
        │
        ├── templates/                     # Jinja2 HTML templates
        └── static/                        # CSS stylesheets, JS scripts, and images
```

---

## Experimental Results & Evaluation

### 1. Collaborative Filtering Evaluation (ALS)
Evaluation on the 20% test partition yielded the following performance metrics:

| Parameter / Metric | Value |
|---|---|
| Optimal Latent Dimension (`rank`) | **2** |
| Regularization Parameter (`regParam`) | **0.30** |
| Maximum Iterations | **10** |
| Cross-Validation Folds | **3 folds** |
| **ALS Model RMSE** | **0.975** |
| **Baseline RMSE (Global Mean)** | **0.877** |

> *Observation*: Due to the relatively small size and sparsity of the initial test sample, the global average baseline slightly outperforms ALS on raw RMSE. However, ALS provides the personalized ranking indispensable for distinguishing unique user tastes.

### 2. Sample Top-5 Product Recommendation
For a test user profile, the unblended predicted ALS ratings are:

| Rank | Product ID | Predicted ALS Score |
|:---:|:---:|:---:|
| 1 | `3` | 2.8379 |
| 2 | `2` | 2.8177 |
| 3 | `1` | 2.8140 |
| 4 | `4` | 2.7422 |
| 5 | `15` | 2.6452 |

After hybrid re-ranking (`Score = 0.7 × ALS_norm + 0.3 × TrendScore`), products whose themes match high-frequency Twitter topics rise in rank, delivering recommendations that are both personalized and aligned with active market trends.

---

## Installation and Execution Guide

### Prerequisites
- **Operating System**: Linux (Ubuntu recommended for cluster deployment) or Windows with distributed tools.
- **Java**: JDK 11 or 17.
- **Build Tool**: Apache Maven 3.8+.
- **Python**: Python 3.10+.
- **Installed Services**:
  - **Apache Kafka** & **Zookeeper**.
  - **Hadoop HDFS**.
  - **Ollama** with the model downloaded: `ollama pull gemma3:4b`.

---

### Step 1: Start Infrastructure Services
In separate terminal windows on your Linux environment:

```bash
# 1. Start Zookeeper & Kafka Broker
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties

# 2. Start HDFS NameNode & DataNode
start-dfs.sh

# 3. Start Ollama Server
ollama serve
```

---

### Step 2: Start Topic Normalization Microservice (FastAPI)

```bash
cd project_on_linux/project_calcule_parallele/topic_service
pip install fastapi uvicorn sentence-transformers scikit-learn
uvicorn app:app --host 127.0.0.1 --port 8000
```

---

### Step 3: Run Ingestion and Spark Streaming

```bash
cd project_on_linux/project_calcule_parallele
mvn clean package -DskipTests

# 1. Start real-time stream processing (Spark Streaming + Ollama + HDFS)
spark-submit --class TweetTopicExtractor \
  --master "local[*]" \
  target/project_calcule_parallele-1.0-SNAPSHOT.jar

# 2. In another terminal, start the Kafka producer
java -cp target/project_calcule_parallele-1.0-SNAPSHOT.jar TweetProducer
```

---

### Step 4: Start Trend API & Hybrid Recommendation Engine

```bash
# 1. Start the Spark Trend API (Port 5001)
cd project_on_linux/project_calcule_parallele/trendAPISender
pip install flask
python trend_api.py

# 2. Start the Hybrid Recommendation Engine API (Port 5002)
cd ../recommendation_system
pip install flask sentence-transformers
python generate_product_embeddings.py
python recommendation_api.py
```

---

### Step 5: Launch the Web Platform (Demo)

```powershell
cd presentation_website/UIT_Final_Project-SynchroWork

# 1. Install Python dependencies
pip install -r requirements.txt

# 2. Initialize local SQLite database & seed accounts
python db_init.py
python migrate_users.py

# 3. Export initial CSV files
python csv_generator.py

# 4. Launch the Flask web server
python app.py
```

Access the platform through your web browser at: **`http://127.0.0.1:5000`**

#### Pre-Configured Demo Accounts:
- **Administrator**: `achraf.salimi@uit.ac.ma` / `Salimi@01` (Catalog management & `/admin/csv` panel)
- **Producer**: `producer@damsostream.com` / `Producer123!` (Product CRUD & 1–5★ rating breakdown)
- **Customer**: `test.client@damsostream.com` / `TestPassword123!` (Browsing, 1–5★ ratings, "For You" recommendations)

---

## Academic License & Rights

This project was developed for educational and academic purposes as part of the Master's in BDIAA at **Ibn Tofail University (Faculty of Sciences, Kenitra)**. All rights reserved by the authors and the academic staff.
