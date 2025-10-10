import pandas as pd
import os
from sqlalchemy import create_engine
from config import get_db_connection

# Configuración para Docker/Airflow
TABLE_NAME = 'grammy_awards_cleaned'
CSV_FILE_PATH = "/opt/airflow/dags/merged_grammy_spotify_clean.csv"
FOLDER_ID = "1_2yFobHWeBehntIZbYCdFN-q17t9tQ_s"  # Folder ID de Google Drive


def load_to_database():
    """Carga los datos del CSV a la base de datos MySQL usando pandas to_sql
    NOTA: Los datos ya vienen limpios desde transformation.py"""
    try:
        import time
        start_time = time.time()
        
        # Leer el archivo CSV (ya transformado y limpio)
        print(f"Leyendo archivo CSV limpio: {CSV_FILE_PATH}")
        df = pd.read_csv(CSV_FILE_PATH)
        print(f"✅ CSV cargado: {len(df)} filas, {len(df.columns)} columnas")
        
        # Validar que el CSV no tenga datos vacíos críticos
        if df.empty:
            raise ValueError("El CSV está vacío. Verifica que la transformación se haya ejecutado correctamente.")
        
        # Crear engine de SQLAlchemy desde la conexión MySQL
        print("Conectando a MySQL...")
        conn = get_db_connection()
        
        # Crear la URL de conexión para SQLAlchemy
        engine = create_engine(
            f"mysql+mysqlconnector://{conn.user}:{conn._password}@{conn.server_host}:{conn.server_port}/{conn.database}",
            pool_pre_ping=True,  # Verificar conexión antes de usar
            pool_recycle=3600     # Reciclar conexiones cada hora
        )
        
        print(f"Insertando datos en la tabla '{TABLE_NAME}'...")
        insert_start = time.time()
        
        # Usar to_sql para cargar los datos (replace elimina la tabla si existe y la recrea)
        df.to_sql(TABLE_NAME, engine, if_exists='replace', index=False, chunksize=500)
        
        insert_time = time.time() - insert_start
        print(f"✅ {len(df)} registros insertados en {insert_time:.2f} segundos")
        print(f"   Velocidad: {len(df)/insert_time:.0f} registros/segundo")
        
        conn.close()
        
        total_time = time.time() - start_time
        print(f"✅ Carga a base de datos completada en {total_time:.2f} segundos")
        
    except Exception as e:
        print(f"❌ Error al cargar datos a la base de datos: {e}")
        print(f"   Tipo de error: {type(e).__name__}")
        raise


from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import logging

logger = logging.getLogger("airflow.task")


def upload_to_drive():
    """
    Sube el archivo CSV limpio a Google Drive usando PyDrive2
    NOTA: Requiere que credentials.json haya sido generado previamente con authenticate_drive.py
    """
    try:
        # Configuración de rutas para Docker/Airflow
        CLIENT_SECRET_PATH = "/opt/airflow/dags/client_secret.json"
        CREDENTIALS_PATH = "/opt/airflow/dags/credentials.json"
        CSV_FILE_PATH = "/opt/airflow/dags/merged_grammy_spotify_clean.csv"
        
        logger.info(f"📤 Iniciando subida de archivo a Google Drive: {CSV_FILE_PATH}")
        
        # Configuración de PyDrive2
        settings = {
            "client_config_backend": "file",
            "client_config_file": CLIENT_SECRET_PATH,
            "save_credentials": True,
            "save_credentials_backend": "file",
            "save_credentials_file": CREDENTIALS_PATH,
            "get_refresh_token": True,
            "oauth_scope": [
                "https://www.googleapis.com/auth/drive.file",
                "https://www.googleapis.com/auth/drive.install"
            ]
        }
        
        # Autenticación
        gauth = GoogleAuth(settings=settings)
        gauth.LoadCredentialsFile(CREDENTIALS_PATH)
        
        # Refrescar token si está expirado
        if gauth.access_token_expired:
            logger.info("🔄 Token expirado, refrescando...")
            gauth.Refresh()
            gauth.SaveCredentialsFile(CREDENTIALS_PATH)
        else:
            gauth.Authorize()
        
        # Crear objeto Drive
        drive = GoogleDrive(gauth)
        
        # Subir archivo
        file_name = os.path.basename(CSV_FILE_PATH)
        file = drive.CreateFile({
            "title": file_name,
            "parents": [{"kind": "drive#fileLink", "id": FOLDER_ID}]
        })
        file.SetContentFile(CSV_FILE_PATH)
        file.Upload()
        
        logger.info(f"✅ Archivo subido exitosamente: {file_name}")
        logger.info(f"🔗 URL: https://drive.google.com/file/d/{file['id']}/view")
        
        return {
            'status': 'success',
            'file_id': file['id'],
            'url': f"https://drive.google.com/file/d/{file['id']}/view"
        }
        
    except Exception as e:
        logger.error(f"❌ Error al subir archivo a Google Drive: {str(e)}")
        raise

def main():
    """Función principal que ejecuta la carga a BD y Google Drive"""
    load_to_database()
    upload_to_drive()

if __name__ == "__main__":
    main()