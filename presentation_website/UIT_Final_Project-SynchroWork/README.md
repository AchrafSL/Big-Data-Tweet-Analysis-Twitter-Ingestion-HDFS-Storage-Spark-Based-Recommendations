# DAMSO_STREAM — Web Application & Big Data Recommendation Engine Integration

## Overview

**DAMSO_STREAM** is an e-commerce platform offering digital service subscriptions (Netflix, Prime Video, Spotify, Microsoft 365, Adobe Creative Cloud, etc.) at competitive prices.

Key features include:
1. **Producer Studio & Dashboard**: Digital service producers can manage their catalog offerings and monitor sales/rating insights.
2. **Ratings & Reviews Breakdown**: Producers can view average ratings, total ratings, 1–5 star distribution, and buyer feedback.
3. **Real-Time Trends & Stream Intelligence**: Displays trending market topics, frequencies, rankings, and matching catalog services connected to Kafka / Spark results.
4. **Role-Based Access Control**: Strict isolation between customer (`client`), producer (`producer`), and administrator (`admin`) permissions.
5. **SQLite Storage & CSV Pipeline**: Local SQLite storage (`damsostream.db`) with CSV exports for Apache Kafka and Spark Streaming ETL.
6. **Responsive Interface**: Dark-themed UI with interactive 5-star rating system and catalog navigation.

---

## Project Authors & Academic Supervision

- **Authors**: Salimi Achraf, Dongmo Kengmo Duclair
- **Supervisor**: Pr. Enneya Nourddine
- **Institution**: Ibn Tofail University (UIT), Faculty of Sciences, Kenitra

---

## Demo Accounts

The application includes pre-configured accounts for each role:

| Role | Email | Password | Permissions & Features | Quick Link |
|---|---|---|---|---|
| **Producer** | `producer@damsostream.com` | `Producer123!` | Producer Studio, Product CRUD, Ratings Breakdown | [`/producer/dashboard`](http://127.0.0.1:5000/producer/dashboard) |
| **Customer** | `test.client@damsostream.com` | `TestPassword123!` | Browse catalog, rate products (1–5★), cart, comments | [`/Offers`](http://127.0.0.1:5000/Offers) |
| **Admin** | `achraf.salimi@uit.ac.ma` | `Salimi@01` | Full catalog control, order treatment, CSV Export Studio | [`/admin/csv`](http://127.0.0.1:5000/admin/csv) |
| **Employee** | `lilnavincisa2022@gmail.com` | `Salimi@01` | Order processing and client communications | [`/home`](http://127.0.0.1:5000/home) |

> **Registration**: New users can register at [`/Signup`](http://127.0.0.1:5000/Signup) and select their role: **Customer** or **Producer**.

---

## Core Features & Pages

### 1. Producer Studio & Dashboard (`/producer/dashboard`)
- **Direct URL**: [`http://127.0.0.1:5000/producer/dashboard`](http://127.0.0.1:5000/producer/dashboard)
- **Navigation**: Accessible via the navigation bar when logged in as a **Producer**.
- **Capabilities**:
  - **KPI Insights Bar**:
    - **Products Listed**: Total active services listed by the producer.
    - **Overall Average Rating**: Aggregated 1–5 star rating across all producer offerings.
    - **Total Ratings Received**: Total count of buyer reviews.
    - **Customer Reviews**: Total feedback comments written by clients.
  - **Add New Product**: Publish new services (Name, Price in MAD, Duration, Category / Theme, Description). Automatically assigns ownership to the active producer.
  - **Product Management Grid**: Card view of each owned product displaying price, duration, category badge, and average rating with direct **Edit**, **Delete**, and **View Public Page** buttons.
  - **Ownership Isolation**: Producers only have access to manage their own products.

### 2. Producer Ratings & Reviews Breakdown (`/producer/ratings`)
- **Direct URL**: [`http://127.0.0.1:5000/producer/ratings`](http://127.0.0.1:5000/producer/ratings)
- **Capabilities**:
  - Displays each product owned by the producer alongside its overall rating and star icons.
  - **1–5 Star Distribution**: Visual progress bars showing the count and percentage for each star level (5★, 4★, 3★, 2★, 1★).
  - **Customer Reviews & Comments**: Client feedback, author names, and timestamps for each specific product.

### 3. Trend Analysis (`/trends`)
- **Direct URL**: [`http://127.0.0.1:5000/trends`](http://127.0.0.1:5000/trends)
- **Capabilities**:
  - **Two Views**:
    - **Global Trends (`?tab=global`)**: Overall trending topics and frequencies from `global_trends.csv`.
    - **Daily Trends (`?tab=daily`)**: Daily trending topics and frequencies from `daily_trends.csv`.
  - **Results**: Displays Rank (`#1`, `#2`, `#3`...), Topic, and Frequency.
  - **Timestamp**: Displays the dataset's last modification time.
  - **Administrator Refresh**:
    - Administrators can trigger dataset recalculation / reload via `POST /trends/refresh`.
    - Integrated with `trigger_external_trend_refresh()` in `app.py` for API integration with the Spark/Kafka processing engine on Linux.

#### Trend Datasets:
| Dataset File | File Location | Schema / Headers | Description |
|---|---|---|---|
| **Global Trends** | `csv_exports/global_trends.csv` | `topic,frequency` | Overall trending topics and frequencies. |
| **Daily Trends** | `csv_exports/daily_trends.csv` | `topic,frequency` | Daily trending topics and frequencies. |

### 4. Customer Recommendations Page (`/recommendations`)
- **Direct URL**: [`http://127.0.0.1:5000/recommendations`](http://127.0.0.1:5000/recommendations)
- **Navigation**: "For You" link in the top menu.
- **Capabilities**:
  - **Recommended for You**: Matches user's configured `favTheme` and `favColor` preferences (e.g., Entertainment, Gaming).
  - **Popular Products**: Highlights catalog services with the highest community ratings and review volume.
  - **Trending Interests**: Dynamically maps to top trending topics from the trends pipeline.
  - **Direct Ordering**: Direct links to product detail pages and one-click "Add to Cart".

### 5. Interactive 5-Star Product Rating Widget (`/Product_Description`)
- **Direct URL Example**: [`http://127.0.0.1:5000/Product_Description?productId=14`](http://127.0.0.1:5000/Product_Description?productId=14)
- **Capabilities**:
  - Interactive 5-star rating widget embedded directly within the product detail card.
  - Authenticated customers can submit or update their ratings (1 to 5 stars) dynamically.

### 6. Role-Based Security & Permissions
- **Customer (`client`)**: Browse offers, place orders, update personal preferences (theme/color), write comments, and rate products. Access to `/producer/*` or `/admin/*` redirects to `/home`.
- **Producer (`producer`)**: Access to the Producer Studio and Ratings Breakdown for owned products.
- **Admin (`admin`)**: Complete administrative access to order management, catalog management, and the Big Data CSV export panel (`/admin/csv`).

### 7. Big Data CSV Exporter & SQLite Storage
- **Primary Database**: SQLite database `damsostream.db`.
- **CSV Generator (`csv_generator.py`)**: Exports 4 datasets to `csv_exports/`:
  - `products.csv`: `productId,name,category,color,theme`
  - `users.csv`: `userId,favTheme,favColor`
  - `ratings.csv`: `userId,productId,rating`
  - `trends_mock.csv`: `topic,frequency`
- **Spark Ingestion Endpoint (`POST /admin/csv/import_trends`)**: Allows the external Spark Streaming pipeline to POST aggregated topic counts directly into the application.

---

## Quick-Start Guide

### 1. Install Dependencies
```powershell
pip install -r requirements.txt
```

### 2. Database & Data Setup (If Starting Fresh)
```powershell
# 1. Initialize SQLite schema and seed catalog offers
python db_init.py

# 2. Seed default user accounts (Admin, Producer, Client)
python migrate_users.py

# 3. Generate initial recommendation CSVs
python csv_generator.py
```

### 3. Launch the Application
```powershell
python app.py
```
Open your browser at: **`http://127.0.0.1:5000`**

---

## Directory Structure

```text
UIT_Final_Project-SynchroWork/
│
├── app.py                     # Main Flask app, routes, and business logic
├── db_init.py                 # SQLite database schema and catalog seeding
├── csv_generator.py           # Exporter for recommendation engine datasets
├── migrate_users.py           # User account migration and seeder script
├── damsostream.db             # SQLite database
│
├── csv_exports/               # Recommendation datasets
│   ├── products.csv           # Product catalog with colors & themes
│   ├── users.csv              # User recommendation preferences
│   ├── ratings.csv            # 1-5 star ratings dataset
│   └── trends_mock.csv        # Market trends and frequencies
│
├── static/
│   ├── CSS/                   # Stylesheets
│   ├── JS/                    # Frontend scripts
│   └── Images/                # Artwork, product thumbnails, and profile pics
│
└── templates/                 # Jinja2 HTML templates
    ├── Producer_Dashboard.html# Producer studio management & product CRUD
    ├── Producer_Ratings.html  # Ratings & reviews breakdown per product
    ├── Trends.html            # Real-time trends display
    ├── Product_Description.html# Product details with 5-star rating widget
    ├── Offers.html            # Public catalog
    ├── Signup.html            # User registration
    ├── Login.html             # User authentication
    ├── Admin_CSV.html         # Admin dataset export studio
    └── Menu.html              # Global navigation header
```

---

## Academic License

Academic and educational project. Developed for Ibn Tofail University (UIT), Faculty of Sciences, Kenitra. All rights reserved.
