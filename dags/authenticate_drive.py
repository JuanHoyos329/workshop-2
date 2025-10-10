from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import os

def authenticate():
    """
    Realiza la autenticación OAuth con Google Drive
    """
    print("🔐 Iniciando proceso de autenticación con Google Drive...")
    
    # Detectar si estamos en Docker o en local
    if os.path.exists("/opt/airflow/dags"):
        DAGS_FOLDER = "/opt/airflow/dags"
        print(f"📁 Detectado entorno Docker, usando: {DAGS_FOLDER}")
    else:
        DAGS_FOLDER = os.path.dirname(os.path.abspath(__file__))
        print(f"📁 Detectado entorno local, usando: {DAGS_FOLDER}")
    
    CLIENT_SECRET_PATH = os.path.join(DAGS_FOLDER, "client_secret.json")
    CREDENTIALS_PATH = os.path.join(DAGS_FOLDER, "credentials.json")
    
    # Verificar que existe el client_secret
    if not os.path.exists(CLIENT_SECRET_PATH):
        print(f"❌ Error: No se encuentra el archivo client_secret.json en: {CLIENT_SECRET_PATH}")
        print("   Asegúrate de tener el archivo client_secret.json en la carpeta dags/")
        return False
    
    print(f"✅ Archivo client_secret.json encontrado")
    
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
    
    try:
        # Inicializar autenticación
        gauth = GoogleAuth(settings=settings)
        
        # Verificar si ya existen credenciales
        if os.path.exists(CREDENTIALS_PATH):
            print(f"📄 Encontrado archivo credentials.json existente")
            gauth.LoadCredentialsFile(CREDENTIALS_PATH)
            
            # Verificar si el token está expirado
            if gauth.credentials is None:
                print("⚠️  Credenciales inválidas, se requiere nueva autenticación")
                gauth.LocalWebserverAuth()
            elif gauth.access_token_expired:
                print("🔄 Token expirado, refrescando credenciales...")
                gauth.Refresh()
            else:
                print("✅ Credenciales válidas encontradas")
                gauth.Authorize()
        else:
            print("🌐 No se encontraron credenciales, iniciando autenticación OAuth...")
            print("   Se abrirá una ventana del navegador para autorizar el acceso")
            gauth.LocalWebserverAuth()
        
        # Guardar las credenciales
        gauth.SaveCredentialsFile(CREDENTIALS_PATH)
        print(f"✅ Credenciales guardadas en: {CREDENTIALS_PATH}")
        
        # Probar la conexión creando un objeto GoogleDrive
        drive = GoogleDrive(gauth)
        print("✅ Conexión con Google Drive establecida exitosamente")
        
        # Listar algunos archivos para verificar que funciona
        print("\n📂 Probando acceso listando archivos en tu Drive...")
        file_list = drive.ListFile({'q': "'root' in parents and trashed=false", 'maxResults': 5}).GetList()
        print(f"   Se encontraron {len(file_list)} archivos (mostrando máximo 5):")
        for file in file_list:
            print(f"   - {file['title']} (ID: {file['id']})")
        
        print("\n🎉 ¡Autenticación completada exitosamente!")
        print("   Ahora puedes ejecutar tu DAG de Airflow sin problemas")
        return True
        
    except Exception as e:
        print(f"\n❌ Error durante la autenticación: {str(e)}")
        print(f"   Tipo de error: {type(e).__name__}")
        return False


if __name__ == "__main__":
    print("=" * 70)
    print("  SCRIPT DE AUTENTICACIÓN DE GOOGLE DRIVE PARA AIRFLOW")
    print("=" * 70)
    print()
    
    success = authenticate()
    
    print()
    print("=" * 70)
    if success:
        print("✅ PROCESO COMPLETADO EXITOSAMENTE")
        print()
        print("Próximos pasos:")
        print("1. Verifica que el archivo credentials.json existe en dags/")
        print("2. Si usas Docker, asegúrate de que el archivo esté montado correctamente")
        print("3. Ejecuta tu DAG de Airflow: docker compose up -d")
    else:
        print("❌ PROCESO FALLIDO")
        print()
        print("Soluciones posibles:")
        print("1. Verifica que client_secret.json esté en la carpeta dags/")
        print("2. Asegúrate de tener instalado PyDrive2: pip install pydrive2")
        print("3. Verifica tu configuración de OAuth en Google Cloud Console")
    print("=" * 70)
