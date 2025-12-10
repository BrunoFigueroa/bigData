import json
import pandas as pd
import matplotlib.pyplot as plt

# ------------------------------
# Cargar JSON raw
# ------------------------------
with open("output_flink.json", "r") as f:
    raw_data = json.load(f)

# ------------------------------
# Transformar datos para gráfico
# ------------------------------
records = []

for entry in raw_data:
    dia = entry["ernc"][0]["dia"]
    hora = entry["ernc"][0]["hora"]
    for e in entry["ernc"]:
        records.append({
            "dia": dia,
            "hora": hora,
            "tipo": e["tipo"],
            "generacion_mwh": e["generacion_mwh"]
        })

df = pd.DataFrame(records)

# Filtrar solo generacion positiva
df = df[df["generacion_mwh"] > 0]

# Ordenar por tiempo
df = df.sort_values(by=["dia", "hora"])

# Pivotar para tener tipos como columnas
df_pivot = df.pivot_table(index=["dia", "hora"], columns="tipo", values="generacion_mwh", fill_value=0)

# ------------------------------
# Graficar líneas por tipo de energía (sin círculos)
# ------------------------------
plt.figure(figsize=(15,6))

for col in df_pivot.columns:
    plt.plot(range(len(df_pivot)), df_pivot[col], linestyle='-', label=col)  # sin marker

plt.xlabel("Tiempo (ordenado por día y hora)")
plt.ylabel("Generación (MWh)")
plt.title("Generación por tipo de energía")
plt.legend()
plt.grid(True)
plt.tight_layout()

# Guardar gráfico
plt.savefig("../imagenes/generacion_por_tipo.png")
plt.show()
