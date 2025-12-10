import csv
import json
from datetime import datetime

def convertir_dem(path_csv):
    resultados = []
    with open(path_csv, newline='', encoding='utf-8') as f:
        lector = csv.DictReader(f)

        for fila in lector:
            # limpiar número con coma de miles
            demanda = int(fila["Demanda sistémica real (Mwh)"].replace(",", ""))

            dt = datetime.strptime(fila["Fecha y hora"], "%Y-%m-%d %H:%M:%S")

            registro = {
                "hora": dt.hour,
                "demanda_mwh": demanda
            }

            resultados.append(registro)

    return resultados


# Ejecución
ruta = "../dataset/csv/dem.csv"
json_resultado = convertir_dem(ruta)

# Guardar
with open("../dataset/json/dem.json", "w", encoding="utf-8") as f:
    json.dump(json_resultado, f, ensure_ascii=False, indent=2)

print("Conversión completada.")
