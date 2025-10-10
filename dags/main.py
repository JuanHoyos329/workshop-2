import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from transformation import main as transformation_main
from load import main as load_main
from extract import main as extract_main
from kpis import main as kpis_main


if __name__ == "__main__":
    print("Extrayendo y cargando datos desde CSV a MySQL...")
    extract_main()

    print("Ejecutando proceso de transformación de datos...")
    transformation_main()

    print("Cargando datos limpios a MySQL...")
    load_main()

    print("Generando KPIs y visualizaciones...")
    kpis_main()

    print("Proceso completado.")