import json

# Leer el archivo JSON
with open("../dataset/json/sen.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# Restar 1 a todas las horas
for entry in data:
    entry["hora"] -= 1

# Guardar el resultado en un nuevo archivo
with open("../dataset/json/sen2.json", "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2, ensure_ascii=False)
