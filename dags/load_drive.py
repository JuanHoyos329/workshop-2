from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import os

CLIENT_SECRET_PATH = "/opt/airflow/dags/client_secret.json"

CSV_FILE_PATH = "/opt/airflow/dags/merged_grammy_spotify_clean.csv" 

# Pass the client secret file explicitly as a settings dict to avoid loading a settings.yaml string
def main():
    gauth = GoogleAuth(settings={
        "client_config_backend": "file",
        "client_config_file": CLIENT_SECRET_PATH
    })

    gauth.LocalWebserverAuth()

    drive = GoogleDrive(gauth)

    folder_id = "1_2yFobHWeBehntIZbYCdFN-q17t9tQ_s"
    file_name = os.path.basename(CSV_FILE_PATH)

    file = drive.CreateFile({
        "title": file_name,
        "parents": [{"kind": "drive#fileLink", "id": folder_id}]
    })

    file.SetContentFile(CSV_FILE_PATH)

    file.Upload()

    print(f"Archivo {file_name} subido exitosamente a Google Drive.")

if __name__ == "__main__":
    main()