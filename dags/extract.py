import pandas as pd
import mysql.connector
import os
from datetime import datetime
from config import get_db

# Configuración de la base de datos
DB_NAME = 'grammy_db'
TABLE_NAME = 'grammy_awards'

def main():
    try:
        # Conexión a la base de datos
        with get_db() as conn:
            with conn.cursor() as cursor:
                print("Conectado a MySQL exitosamente.")

                # Crear la base de datos si no existe
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME} DEFAULT CHARACTER SET 'utf8mb4'")
                print(f"Base de datos '{DB_NAME}' creada o ya existente.")

                # Seleccionar la base de datos recién creada
                cursor.execute(f"USE {DB_NAME}")
                print(f"Conectado a la base de datos: {DB_NAME}")

                # ------------------ LECTURA DEL CSV ------------------ #
                csv_path = os.path.join(os.path.dirname(__file__), "the_grammy_awards.csv")

                try:
                    df = pd.read_csv(csv_path)
                    print(f"Archivo CSV cargado: {csv_path}")
                except FileNotFoundError:
                    print("No se encontró el archivo CSV. Verifica la ruta.")
                    return

                if df.empty:
                    print("El archivo CSV está vacío. No se puede procesar.")
                    return

                df.columns = [col.strip().replace(" ", "_").replace("-", "_").replace("/", "_") for col in df.columns]

                # ------------------ DETECCIÓN DE TIPOS ------------------ #
                def detect_mysql_type(series: pd.Series) -> str:
                    if pd.api.types.is_integer_dtype(series.dropna()):
                        return "INT"
                    elif pd.api.types.is_float_dtype(series.dropna()):
                        return "FLOAT"
                    elif pd.api.types.is_bool_dtype(series.dropna()):
                        return "TINYINT(1)"  # Boolean en MySQL
                    elif pd.api.types.is_datetime64_any_dtype(series):
                        return "DATETIME"
                    else:
                        max_len = series.astype(str).map(len).max() if not series.empty else 0
                        return f"VARCHAR({max_len})" if max_len < 255 else "TEXT"

                print("\nTipos detectados por columna:")
                columns_sql_parts = []
                for col in df.columns:
                    sql_type = detect_mysql_type(df[col])
                    columns_sql_parts.append(f"`{col}` {sql_type}")
                    print(f" - {col}: {sql_type}")
                columns_sql = ",\n    ".join(columns_sql_parts)

                # ------------------ CREACIÓN DE LA TABLA ------------------ #
                try:
                    cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
                    print(f"Tabla '{TABLE_NAME}' eliminada correctamente.")
                except mysql.connector.Error as err:
                    print(f"Error al eliminar la tabla existente: {err}")
                    return

                create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    {columns_sql}
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """

                try:
                    cursor.execute(create_table_query)
                    print(f"\nTabla '{TABLE_NAME}' creada correctamente.")
                except mysql.connector.Error as err:
                    print(f"Error al crear la tabla: {err}")
                    return

                # ------------------ INSERCIÓN DE DATOS ------------------ #
                def to_mysql_compatible(value):
                    if pd.isna(value):
                        return None
                    if isinstance(value, bool):
                        return int(value)
                    if str(value).lower() in ['true', 'yes', 'y', '1']:
                        return 1
                    if str(value).lower() in ['false', 'no', 'n', '0']:
                        return 0
                    return str(value)

                rows = [
                    tuple(to_mysql_compatible(x) for x in row)
                    for _, row in df.iterrows()
                ]
                placeholders = ", ".join(["%s"] * len(df.columns))
                insert_query = f"INSERT INTO {TABLE_NAME} ({', '.join(df.columns)}) VALUES ({placeholders})"

                try:
                    cursor.executemany(insert_query, rows)
                    conn.commit()
                    print(f"\n{len(rows)} registros insertados en la tabla '{TABLE_NAME}' correctamente.")
                except mysql.connector.Error as err:
                    print(f"Error al insertar datos: {err}")
                    return

    except mysql.connector.Error as err:
        print(f"Error de conexión: {err}")

if __name__ == "__main__":
    main()