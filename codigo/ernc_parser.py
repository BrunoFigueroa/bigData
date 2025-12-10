import csv
import json
from datetime import datetime

# Mapeo simple por palabras clave en el nombre
def clasificar_tipo(nombre):
    nombre = nombre.upper()
    if "PE " in nombre:
        return "eolica"
    if "PFV" in nombre or "FV" in nombre:
        return "fotovoltaica"
    if "CSP" in nombre:
        return "solar_termica"
    if "GEO" in nombre:
        return "geotermica"
    if "HP" in nombre:
        return "hidraulica"
    if "TER" in nombre:
        return "termica"
    return "desconocido"

# Procesar el CSV
def convertir_csv_a_json(path_csv):
    resultados = []
    with open(path_csv, newline='', encoding='utf-8') as f:
        lector = csv.DictReader(f)
        for fila in lector:
            fecha = datetime.strptime(fila["Fecha y Hora"], "%Y-%m-%d %H:%M:%S.%f")
            registro = {
                "hora": fecha.hour,
                "planta": fila["Nombre Central"],
                "tipo": clasificar_tipo(fila["Nombre Central"]),
                "generacion_mwh": float(fila["Generación ERNC (MWh)"])
            }
            resultados.append(registro)
    return resultados

# Ejecución
ruta = "../dataset/csv/ernc.csv"
json_resultado = convertir_csv_a_json(ruta)

# Guardar
with open("../dataset/json/ernc.json", "w", encoding="utf-8") as f:
    json.dump(json_resultado, f, ensure_ascii=False, indent=2)

print("Conversión completada.")
