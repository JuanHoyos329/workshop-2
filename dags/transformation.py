import pandas as pd
import re
import logging
from config import get_db_connection

# -----------------------------
# Configuración básica
# -----------------------------
SPOTIFY_CSV_PATH = "/opt/airflow/dags/spotify_dataset.csv"
OUTPUT_CSV_PATH = "/opt/airflow/dags/merged_grammy_spotify_clean.csv"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# -----------------------------
# Función de normalización
# -----------------------------
def normalize_text(s):
    if pd.isna(s):
        return ''
    s = s.lower().strip()
    s = re.sub(r'\s*(feat\.|featuring|ft\.|&|and)\s*', ';', s)
    s = re.sub(r'\s+', ' ', s)
    return s

# -----------------------------
# Función principal
# -----------------------------
def transform_data():
    try:
        logging.info("Cargando dataset de Spotify...")
        df_spotify = pd.read_csv(SPOTIFY_CSV_PATH)

        logging.info("Conectando a MySQL...")
        conn = get_db_connection()
        df_grammy = pd.read_sql("SELECT * FROM grammy_awards", conn)

    
        df_grammy = df_grammy[~((df_grammy['nominee'] == '') & (df_grammy['artist'] == ''))]
        df_grammy = df_grammy.drop(columns=['published_at','updated_at','workers','img'], errors='ignore')
        df_grammy['artist'] = df_grammy['artist'].fillna('Unknown')

        df_spotify = df_spotify.drop_duplicates(keep='first')
        
        # Crear columnas normalizadas
        for col in ['category','nominee','artist']:
            df_grammy[f'{col}_norm'] = df_grammy[col].astype(str).apply(normalize_text)

        df_spotify['track_name_norm'] = df_spotify['track_name'].astype(str).apply(normalize_text)
        df_spotify['album_name_norm'] = df_spotify['album_name'].astype(str).apply(normalize_text)
        df_spotify['artists_norm'] = df_spotify['artists'].astype(str).apply(lambda x: normalize_text(x).replace(',', ';'))

        # Filtrar años
        df_grammy = df_grammy[df_grammy['year'] >= 1958]

        # -----------------------------
        # MERGE INTELIGENTE entre Grammy y Spotify
        # -----------------------------
        logging.info("Realizando merge inteligente entre Grammy y Spotify...")
        
        # Clasificar categorías por tipo (canción vs álbum/otros)
        song_keywords = ['song', 'performance', 'recording', 'music', 'composition', 'track']
        mask_song = df_grammy['category_norm'].apply(lambda x: any(k in x for k in song_keywords))
        
        grammy_song = df_grammy[mask_song].copy()
        grammy_other = df_grammy[~mask_song].copy()
        
        # Mantener solo la versión más popular de cada canción
        spotify_top = (
            df_spotify.sort_values('popularity', ascending=False)
            .drop_duplicates(subset=['artists_norm', 'track_name_norm'])
        )
        
        logging.info(f"Categorías de canciones: {len(grammy_song)}, Otras categorías: {len(grammy_other)}")
        
        # Merge flexible para canciones (coincidencia exacta y parcial)
        merged_song = []
        
        for _, row in grammy_song.iterrows():
            artist = row['artist_norm']
            song = row['nominee_norm']
            
            # Intentar coincidencia exacta primero
            match = spotify_top[
                (spotify_top['artists_norm'].str.contains(artist, na=False, regex=False)) &
                (spotify_top['track_name_norm'] == song)
            ]
            
            # Si no hay match exacto, intentar coincidencia parcial en el nombre de la canción
            if match.empty and song:
                safe_song = re.escape(song.split('(')[0].strip())
                match = spotify_top[
                    (spotify_top['artists_norm'].str.contains(artist, na=False, regex=False)) &
                    (spotify_top['track_name_norm'].str.contains(safe_song, na=False, regex=True))
                ]
            
            if not match.empty:
                best = match.sort_values('popularity', ascending=False).iloc[0]
                combined = pd.concat([row, best])
                merged_song.append(combined)
            else:
                merged_song.append(row)
        
        merged_song_df = pd.DataFrame(merged_song)
        
        # Combinar canciones con otras categorías
        df_merged = pd.concat([merged_song_df, grammy_other], ignore_index=True)
        
        logging.info(f"Merge completado. Registros Grammy: {len(df_grammy)}, Registros después del merge: {len(df_merged)}")
        logging.info(f"Registros con datos de Spotify: {df_merged['track_id'].notna().sum() if 'track_id' in df_merged.columns else 0}")
        
        # -----------------------------
        # Limpiar columnas innecesarias
        # -----------------------------
        columns_to_drop = [
            'id',                    # ID no necesario
            'category_norm',         # Columnas de normalización temporal
            'nominee_norm', 
            'artist_norm',
            'track_name_norm',
            'album_name_norm', 
            'artists_norm',
            'Unnamed: 0',           # Columna de índice de pandas
            'track_id'              # ID de Spotify no necesario
        ]
        
        df_merged = df_merged.drop(columns=[col for col in columns_to_drop if col in df_merged.columns], errors='ignore')
        
        # -----------------------------
        # Manejo de valores NaN (limpieza de datos)
        # -----------------------------
        logging.info("Limpiando valores NaN...")
        
        # Identificar columnas numéricas de Spotify que existen en el DataFrame
        potential_numeric_columns = ['year', 'popularity', 'duration_ms', 'danceability', 'energy', 'key', 
                                    'loudness', 'mode', 'speechiness', 'acousticness', 'instrumentalness', 
                                    'liveness', 'valence', 'tempo', 'time_signature']
        
        numeric_columns = [col for col in potential_numeric_columns if col in df_merged.columns]
        
        # Reemplazar NaN en columnas numéricas con 0
        if numeric_columns:
            df_merged[numeric_columns] = df_merged[numeric_columns].fillna(0)
            logging.info(f"✅ Valores NaN reemplazados con 0 en {len(numeric_columns)} columnas numéricas")
        
        # Reemplazar NaN en columnas de texto con cadenas vacías
        text_columns = [col for col in df_merged.columns if col not in numeric_columns]
        df_merged[text_columns] = df_merged[text_columns].fillna('')
        logging.info(f"✅ Valores NaN reemplazados con '' en {len(text_columns)} columnas de texto")
        
        # Truncar valores largos si la columna 'workers' existe (por seguridad, aunque debería estar eliminada)
        if 'workers' in df_merged.columns:
            df_merged['workers'] = df_merged['workers'].apply(lambda x: x[:255] if isinstance(x, str) and len(x) > 255 else x)
            logging.info("✅ Valores largos en 'workers' truncados a 255 caracteres")
        
        # Guardar CSV final
        df_merged.to_csv(OUTPUT_CSV_PATH, index=False)
        logging.info(f"💾 CSV con merge guardado en {OUTPUT_CSV_PATH}")
        logging.info(f"📊 Total columnas: {len(df_merged.columns)}")
        logging.info(f"📊 Columnas finales: {df_merged.columns.tolist()}")

        conn.close()
        logging.info("Conexión a base de datos cerrada.")

    except Exception as e:
        logging.error(f"Error en ETL: {e}")
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    transform_data()