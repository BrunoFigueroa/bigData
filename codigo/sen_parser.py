import csv
import json

def convertir_sen(path_csv):
    resultados = []
    with open(path_csv, newline='', encoding='utf-8') as f:
        lector = csv.reader(f)
        next(lector)  # saltar encabezado

        for fila in lector:
            # fila = ["", "1/1/2024", "1", "1", "8,728.29"]
            _, fecha, dia, hora, sen = fila

            # limpiar formato del número "8,728.29"
            sen_limpio = float(sen.replace(",", ""))

            registro = {
                "hora": int(hora),
                "sen_mwh": sen_limpio
            }
            resultados.append(registro)

    return resultados


# Ejecución
ruta = "../dataset/csv/sen.csv"
json_resultado = convertir_sen(ruta)

# Guardar
with open("../dataset/json/sen.json", "w", encoding="utf-8") as f:
    json.dump(json_resultado, f, ensure_ascii=False, indent=2)

print("Conversión completada.")
