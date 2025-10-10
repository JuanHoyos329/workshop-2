# 🎵 Workshop-2: ETL Pipeline for Musical Analysis — Grammy & Spotify

## 📋 Project Description

This project implements a **complete ETL pipeline** that integrates data from the **Grammy Awards** and **Spotify** to perform strategic musical analysis.
The system automates data extraction, transformation, and loading using **Apache Airflow**, **Docker**, and **MySQL**, culminating in an interactive **Power BI dashboard**.

### 🎯 Objectives

* Automate the integration process of Grammy and Spotify data
* Perform exploratory data analysis (EDA) on musical features
* Identify success patterns within the music industry
* Provide strategic insights for record label decision-making
* Build a scalable and reproducible data pipeline

---

## 🏗️ System Architecture

```
┌─────────────────┐
│   Data Sources  │
│ Grammy + Spotify│
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   EXTRACTION    │  ← extract.py (MySQL)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ TRANSFORMATION  │  ← transformation.py
│  - Normalization│
│  - Merge Data   │
│  - Feature Eng. │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│      LOAD       │  ← load.py
│  - MySQL DB     │
│  - Google Drive │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  ORCHESTRATION  │  ← Apache Airflow
│   (dag_etl.py)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  VISUALIZATION  │  ← Power BI Dashboard
│   & ANALYSIS    │     eda.ipynb
└─────────────────┘
```

---

## 📂 Project Structure

```
workshop-2/
│
├── dags/                           # Airflow directory
│   ├── dag_etl.py                 
│   ├── extract.py                
│   ├── transformation.py          
│   ├── load.py                    
│   ├── config.py                  
│   ├── authenticate_drive.py      
│   ├── load_drive.py             
│   ├── client_secret.json        
│   ├── credentials.json           
│   ├── spotify_dataset.csv       
│   ├── the_grammy_awards.csv     
│   └── merged_grammy_spotify_clean.csv  
│
├── data/                         
├── logs/                          
├── plugins/                      
├── dashboard/                     
│   └── dashboard.pbix             
│
├── eda.ipynb                      
├── docker-compose.yaml            
├── requirements.txt               
├── .env                           
└── README.md                      
```

---

## 🔧 Technologies Used

### Backend & Orchestration

* **Apache Airflow 2.5.1** – ETL pipeline orchestration
* **Python** – Main programming language
* **Pandas** – Data manipulation and analysis
* **SQLAlchemy** – ORM for databases

### Infrastructure

* **Docker & Docker Compose** – Containerization
* **MySQL 8.0** – Main relational database
* **Redis** – Message broker for Celery

### Storage & APIs

* **Google Drive API** – Cloud storage integration
* **PyDrive2** – Python client for Google Drive

### Analysis & Visualization

* **Jupyter Notebook** – Interactive EDA
* **Matplotlib & Seaborn** – Data visualization
* **SciPy** – Statistical analysis
* **Power BI** – Business intelligence dashboard

---

## 🚀 Installation & Setup

### Prerequisites

* Docker Desktop installed
* Python
* Google Cloud account (for Drive API)
* Minimum 4GB RAM
* 10GB disk space

### 1. Clone the Repository

```bash
git clone https://github.com/JuanHoyos329/workshop-2.git
cd workshop-2
```

### 2. Configure Environment Variables

Create a `.env` file in the project root:

```env
AIRFLOW_UID=50000
AIRFLOW_IMAGE_NAME=apache/airflow:2.5.1
```

### 3. Configure Google Drive API

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project
3. Enable **Google Drive API**
4. Create OAuth 2.0 credentials
5. Download `client_secret.json` and place it in `dags/`
6. Run authentication:

```bash
python dags/authenticate_drive.py
```

### 4. Install Python Dependencies (Optional - Local)

```bash
pip install -r requirements.txt
```

### 5. Start Services with Docker

```bash
# Initialize Airflow (first time only)
docker-compose up airflow-init

# Start all services
docker-compose up -d

# Check container status
docker-compose ps
```

### 6. Access Interfaces

* **Airflow UI**: [http://localhost:8080](http://localhost:8080)

  * User: `airflow`
  * Password: `airflow`

* **Flower (Celery Monitor)**: [http://localhost:5555](http://localhost:5555)

* **MySQL**:

  * Host: `localhost`
  * Port: `3307`
  * User: `airflow`
  * Password: `airflow`
  * Database: `grammy_db`

---

## 🔄 ETL Pipeline — Workflow

### DAG: `etl_workflow`

**Configuration:**

* Start Date: August 1, 2025
* Schedule: Daily (`@daily`)
* Catchup: Enabled
* Max Active Runs: 1

### Pipeline Tasks

#### 1️⃣ **Extraction** (`extract.py`)

**Function:** Load Grammy data into MySQL

* Reads `the_grammy_awards.csv`
* Detects data types automatically
* Creates table `grammy_awards` in MySQL
* Inserts records with null handling
* **Result:** Grammy data table in MySQL

#### 2️⃣ **Transformation** (`transformation.py`)

**Function:** Intelligent merge and feature engineering

**Steps:**

1. **Load Data**

   * Spotify dataset from CSV
   * Grammy dataset from MySQL

2. **Cleaning**

   * Remove duplicates
   * Normalize text (lowercase, trim spaces)
   * Handle collaborations (feat., &, and)

3. **Smart Merge**

   * Classify categories (song vs album)
   * Exact and partial matching
   * Keep most popular version per track

4. **Feature Engineering**

   * `explicit_label`: Explicit/Clean
   * `duration_minutes`: Duration in minutes
   * `decade`: Song decade
   * `popularity_range`: Popularity categories
   * `energy_level`: Energy level
   * `dance_level`: Danceability level
   * `duration_category`: Duration category
   * `mood`: Mood (Sad/Neutral/Happy)
   * `acousticness_level`: Electronic/Hybrid/Acoustic
   * `tempo_category`: Slow/Moderate/Fast/Very Fast

5. **Filtering**

   * Keep only complete records (Grammy + Spotify)
   * Remove rows without relevant information

**Result:** `merged_grammy_spotify_clean.csv` ready for analysis

#### 3️⃣ **Load** (`load.py`)

**Function:** Persist processed data

**Operations:**

1. **Load into MySQL**

   * Table: `grammy_awards_cleaned`
   * Method: SQLAlchemy `to_sql`
   * Strategy: Replace (overwrite table)
   * Performance: ~500 rows per chunk

2. **Upload to Google Drive**

   * File: `merged_grammy_spotify_clean.csv`
   * Auth: OAuth 2.0
   * Automatic token refresh
   * Configurable folder ID

**Result:** Data available in DB and cloud

---

## 📊 Exploratory Data Analysis (EDA)

The `eda.ipynb` notebook contains a full **English** analysis with the following structure:

### EDA Structure

1. **Data Loading**

   * Load Spotify and Grammy datasets
   * Validate structure

2. **Spotify Dataset Analysis**

   * Column overview and data types
   * Data quality (nulls, duplicates)
   * Descriptive statistics
   * Visualizations:

     * Top 10 most frequent artists
     * Popularity distribution
     * Explicit vs Non-Explicit comparison
     * Audio feature distributions
     * Correlation matrix

3. **Grammy Dataset Analysis**

   * Structure and data types
   * Most common categories
   * Temporal distribution of awards
   * Top 10 most nominated artists
   * Winner vs Nominee distribution

4. **Outlier Detection**

   * Z-score analysis (|Z| > 3)
   * Boxplots for numerical features
   * Identification of outliers

5. **Key Findings Summary**

   * Comprehensive statistical summary
   * Key metrics from both datasets

---

## 📈 Power BI Dashboard

The file `dashboard/dashboard.pbix` contains interactive visuals for strategic analysis.

### Data Connection

1. Open `dashboard.pbix` with Power BI Desktop
2. Configure MySQL connection:

   * Server: `localhost:3307`
   * Database: `grammy_db`
   * Table: `grammy_awards_cleaned`
3. Update credentials
4. Refresh data

### Suggested Visuals

* 📊 KPIs: Total tracks, artists, genres
* 📉 Popularity trends over time
* 🎸 Distribution by music genre
* 🏆 Grammy winners analysis
* 🎵 Audio features by decade
* 🔥 Top artists and tracks

---

## 🛠️ Useful Commands

### Docker

```bash
# View Airflow logs
docker-compose logs airflow-scheduler -f

# Restart services
docker-compose restart

# Stop everything
docker-compose down

# Remove volumes (⚠️ deletes data)
docker-compose down -v

# Rebuild images
docker-compose build
```

### Airflow CLI

```bash
# List DAGs
docker-compose run airflow-worker airflow dags list

# Trigger DAG manually
docker-compose run airflow-worker airflow dags trigger etl_workflow

# Check task status
docker-compose run airflow-worker airflow tasks list etl_workflow
```

### MySQL

```bash
# Connect to MySQL terminal
docker exec -it workshop-2-mysql-1 mysql -u airflow -pairflow grammy_db

# Useful queries
SHOW TABLES;
SELECT COUNT(*) FROM grammy_awards_cleaned;
DESCRIBE grammy_awards_cleaned;
```

---

## 📝 Database Configuration

### MySQL Connection in `config.py`

```python
import mysql.connector

def get_db():
    return mysql.connector.connect(
        host="mysql",
        port=3306,
        user="airflow",
        password="airflow",
        database="grammy_db"
    )
```

---

## 🔍 Use Cases

### 1. Music Trend Analysis

* Identify success characteristics
* Genre evolution by decade
* Popularity prediction

### 2. Record Label Strategy

* Successful artist profiles
* Optimal features per genre
* Release timing strategies

### 3. Awards Analysis

* Grammy–Spotify popularity correlation
* Most competitive categories
* Nomination patterns

### 4. Feature Engineering

* Derived variables for ML
* Audience segmentation
* Song similarity clustering

---

## 🐛 Troubleshooting

### Error: "No module named 'pandas'"

```bash
docker-compose build --no-cache
docker-compose up -d
```

### MySQL Connection Error

```bash
docker-compose ps
docker-compose logs mysql
```

### Google Drive Authentication Error

```bash
python dags/authenticate_drive.py
ls -la dags/credentials.json
```

### Airflow Not Loading ([http://localhost:8080](http://localhost:8080))

```bash
netstat -an | findstr 8080  # Windows
docker-compose logs airflow-webserver
```

---

## 👥 Author

* **Juan Hoyos** – [@JuanHoyos329](https://github.com/JuanHoyos329)

**⭐ If this project was helpful, consider giving it a star on GitHub!**
