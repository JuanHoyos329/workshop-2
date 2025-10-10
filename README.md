# 🎵 Workshop-2: Pipeline ETL de Análisis Musical Grammy & Spotify

## 📋 Descripción del Proyecto

Este proyecto implementa un **pipeline ETL completo** que integra datos de los **Premios Grammy** y **Spotify** para realizar análisis musical estratégico. El sistema automatiza la extracción, transformación y carga de datos utilizando **Apache Airflow**, Docker, y MySQL, culminando en un dashboard interactivo de Power BI.

### 🎯 Objetivos

- Automatizar el proceso de integración de datos Grammy y Spotify
- Realizar análisis exploratorio de datos (EDA) sobre características musicales
- Identificar patrones de éxito en la industria musical
- Proveer insights estratégicos para decisiones de discográficas
- Crear un pipeline de datos escalable y reproducible

---

## 🏗️ Arquitectura del Sistema

```
┌─────────────────┐
│   Data Sources  │
│  Grammy + Spotify│
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
│  - Normalización │
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
│  ORQUESTACIÓN   │  ← Apache Airflow
│   (dag_etl.py)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  VISUALIZACIÓN  │  ← Power BI Dashboard
│   & ANÁLISIS    │     eda.ipynb
└─────────────────┘
```

---

## 📂 Estructura del Proyecto

```
workshop-2/
│
├── dags/                           # Directorio de Airflow
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

## 🔧 Tecnologías Utilizadas

### Backend & Orquestación
- **Apache Airflow 2.5.1** - Orquestación del pipeline ETL
- **Python 3.x** - Lenguaje principal
- **Pandas** - Manipulación y análisis de datos
- **SQLAlchemy** - ORM para bases de datos

### Infraestructura
- **Docker & Docker Compose** - Containerización
- **PostgreSQL** - Metadata de Airflow
- **MySQL 8.0** - Base de datos principal
- **Redis** - Message broker para Celery

### Almacenamiento & APIs
- **Google Drive API** - Almacenamiento en la nube
- **PyDrive2** - Cliente Python para Google Drive

### Análisis & Visualización
- **Jupyter Notebook** - EDA interactivo
- **Matplotlib & Seaborn** - Visualizaciones
- **SciPy** - Análisis estadístico
- **Power BI** - Dashboard empresarial

---

## 🚀 Instalación y Configuración

### Prerrequisitos

- Docker Desktop instalado
- Python 3.8+
- Cuenta de Google Cloud (para Google Drive API)
- 4GB RAM mínimo
- 10GB espacio en disco

### 1. Clonar el Repositorio

```bash
git clone https://github.com/JuanHoyos329/workshop-2.git
cd workshop-2
```

### 2. Configurar Variables de Entorno

Crear archivo `.env` en la raíz del proyecto:

```env
AIRFLOW_UID=50000
AIRFLOW_IMAGE_NAME=apache/airflow:2.5.1
```

### 3. Configurar Google Drive API

1. Ir a [Google Cloud Console](https://console.cloud.google.com/)
2. Crear un nuevo proyecto
3. Habilitar **Google Drive API**
4. Crear credenciales OAuth 2.0
5. Descargar `client_secret.json` y colocarlo en `dags/`
6. Ejecutar autenticación:

```bash
python dags/authenticate_drive.py
```

### 4. Instalar Dependencias Python (Opcional - Local)

```bash
pip install -r requirements.txt
```

### 5. Iniciar Servicios con Docker

```bash
# Inicializar Airflow (primera vez)
docker-compose up airflow-init

# Levantar todos los servicios
docker-compose up -d

# Verificar estado de contenedores
docker-compose ps
```

### 6. Acceder a las Interfaces

- **Airflow UI**: http://localhost:8080
  - Usuario: `airflow`
  - Contraseña: `airflow`
  
- **Flower (Celery Monitor)**: http://localhost:5555

- **MySQL**: 
  - Host: `localhost`
  - Puerto: `3307`
  - Usuario: `airflow`
  - Contraseña: `airflow`
  - Base de datos: `grammy_db`

---

## 🔄 Pipeline ETL - Flujo de Trabajo

### DAG: `etl_workflow`

**Configuración:**
- Inicio: 1 de agosto de 2025
- Frecuencia: Diaria (`@daily`)
- Catchup: Habilitado
- Max Active Runs: 1

### Tareas del Pipeline

#### 1️⃣ **Extracción** (`extract.py`)

**Función:** Cargar datos Grammy a MySQL

- Lee `the_grammy_awards.csv`
- Detecta tipos de datos automáticamente
- Crea tabla `grammy_awards` en MySQL
- Inserta registros con manejo de valores nulos
- **Resultado:** Tabla MySQL con datos Grammy

#### 2️⃣ **Transformación** (`transformation.py`)

**Función:** Merge inteligente y feature engineering

**Pasos:**
1. **Carga de datos:**
   - Spotify dataset desde CSV
   - Grammy dataset desde MySQL

2. **Limpieza:**
   - Eliminación de duplicados
   - Normalización de texto (lowercase, espacios)
   - Manejo de colaboraciones (feat., &, and)

3. **Merge inteligente:**
   - Clasificación de categorías (canción vs álbum)
   - Coincidencia exacta y parcial
   - Mantener versión más popular de cada track

4. **Feature Engineering:**
   - `explicit_label`: Explicit/Clean
   - `duration_minutes`: Duración en minutos
   - `decade`: Década de la canción
   - `popularity_range`: Categorías de popularidad
   - `energy_level`: Nivel de energía
   - `dance_level`: Nivel de bailabilidad
   - `duration_category`: Categoría por duración
   - `mood`: Estado de ánimo (Sad/Neutral/Happy)
   - `acousticness_level`: Electronic/Hybrid/Acoustic
   - `tempo_category`: Slow/Moderate/Fast/Very Fast

5. **Filtrado:**
   - Solo registros con datos completos (Grammy + Spotify)
   - Elimina registros sin información útil

**Resultado:** `merged_grammy_spotify_clean.csv` con datos listos para análisis

#### 3️⃣ **Carga** (`load.py`)

**Función:** Persistencia de datos procesados

**Operaciones:**
1. **Carga a MySQL:**
   - Tabla: `grammy_awards_cleaned`
   - Método: SQLAlchemy `to_sql`
   - Estrategia: Replace (sobrescribe tabla)
   - Performance: ~500 registros por chunk

2. **Upload a Google Drive:**
   - Archivo: `merged_grammy_spotify_clean.csv`
   - Autenticación: OAuth 2.0
   - Refresh automático de tokens
   - Folder ID configurable

**Resultado:** Datos disponibles en DB y Cloud

---

## 📊 Análisis Exploratorio de Datos (EDA)

El notebook `eda.ipynb` contiene un análisis completo en **inglés** con las siguientes secciones:

### Estructura del EDA

1. **Data Loading**
   - Carga de datasets Spotify y Grammy
   - Validación de estructura

2. **Spotify Dataset Analysis**
   - Overview de columnas y tipos
   - Calidad de datos (nulos, duplicados)
   - Estadísticas descriptivas
   - Visualizaciones:
     - Top 10 artistas más frecuentes
     - Distribución de popularidad
     - Comparación Explicit vs Non-Explicit
     - Distribución de features de audio
     - Matriz de correlación

3. **Grammy Dataset Analysis**
   - Estructura y tipos de datos
   - Categorías más frecuentes
   - Distribución temporal de premios
   - Top 10 artistas con más nominaciones
   - Distribución Winner vs Nominee

4. **Outlier Detection**
   - Z-score analysis (|Z| > 3)
   - Boxplots para features numéricas
   - Identificación de valores atípicos

5. **Key Findings Summary**
   - Resumen estadístico completo
   - Métricas clave de ambos datasets

### Ejecutar el EDA

```bash
# Instalar dependencias
pip install jupyter pandas matplotlib seaborn scipy

# Iniciar Jupyter
jupyter notebook eda.ipynb
```

---

## 📈 Dashboard Power BI

El archivo `dashboard/dashboard.pbix` contiene visualizaciones interactivas para análisis estratégico.

### Conexión a Datos

1. Abrir `dashboard.pbix` con Power BI Desktop
2. Configurar conexión MySQL:
   - Server: `localhost:3307`
   - Database: `grammy_db`
   - Table: `grammy_awards_cleaned`
3. Actualizar credenciales
4. Refresh datos

### Visualizaciones Sugeridas

- 📊 KPIs: Total tracks, artistas, géneros
- 📉 Tendencias temporales de popularidad
- 🎸 Distribución por género musical
- 🏆 Análisis de ganadores Grammy
- 🎵 Características de audio por década
- 🔥 Top artistas y tracks

---

## 🛠️ Comandos Útiles

### Docker

```bash
# Ver logs de Airflow
docker-compose logs airflow-scheduler -f

# Reiniciar servicios
docker-compose restart

# Detener todo
docker-compose down

# Eliminar volúmenes (CUIDADO: borra datos)
docker-compose down -v

# Reconstruir imágenes
docker-compose build
```

### Airflow CLI

```bash
# Listar DAGs
docker-compose run airflow-worker airflow dags list

# Trigger manual del DAG
docker-compose run airflow-worker airflow dags trigger etl_workflow

# Ver estado de tareas
docker-compose run airflow-worker airflow tasks list etl_workflow
```

### MySQL

```bash
# Conectar a MySQL desde terminal
docker exec -it workshop-2-mysql-1 mysql -u airflow -pairflow grammy_db

# Consultas útiles
SHOW TABLES;
SELECT COUNT(*) FROM grammy_awards_cleaned;
DESCRIBE grammy_awards_cleaned;
```

---

## 📝 Configuración de Base de Datos

### Conexión MySQL en `config.py`

```python
import mysql.connector

def get_db():
    return mysql.connector.connect(
        host="mysql",  # Nombre del servicio en docker-compose
        port=3306,
        user="airflow",
        password="airflow",
        database="grammy_db"
    )

def get_db_connection():
    return mysql.connector.connect(
        host="mysql",
        port=3306,
        user="airflow",
        password="airflow",
        database="grammy_db"
    )
```

---

## 🔍 Casos de Uso

### 1. Análisis de Tendencias Musicales
- Identificar características de éxito
- Evolución de géneros por década
- Predicción de popularidad

### 2. Estrategia de Discográfica
- Perfiles de artistas exitosos
- Características óptimas por género
- Timing de lanzamientos

### 3. Análisis de Premios
- Correlación Grammy - Popularidad Spotify
- Categorías más competitivas
- Patrones de nominaciones

### 4. Feature Engineering
- Variables derivadas para ML
- Segmentación de audiencias
- Clustering de canciones similares

---

## 🐛 Troubleshooting

### Error: "No module named 'pandas'"
```bash
# Verificar que requirements.txt esté en la imagen
docker-compose build --no-cache
docker-compose up -d
```

### Error de conexión MySQL
```bash
# Verificar que MySQL esté healthy
docker-compose ps

# Revisar logs de MySQL
docker-compose logs mysql
```

### Error de autenticación Google Drive
```bash
# Re-autenticar
python dags/authenticate_drive.py

# Verificar que credentials.json existe
ls -la dags/credentials.json
```

### Airflow no aparece en http://localhost:8080
```bash
# Verificar puerto disponible
netstat -an | findstr 8080  # Windows

# Verificar logs
docker-compose logs airflow-webserver
```

## 👥 Autor

- **Juan Hoyos** - [@JuanHoyos329](https://github.com/JuanHoyos329)

**⭐ Si este proyecto te fue útil, considera darle una estrella en GitHub!**