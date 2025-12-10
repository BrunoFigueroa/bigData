import json
from confluent_kafka import Producer
import time

# Configuración del producer
config = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "master-producer"
}
producer = Producer(config)

def load_json_list(path):
    with open(path, "r") as f:
        return json.load(f)

def send(topic, msg):
    producer.produce(topic, json.dumps(msg).encode("utf-8"))
    producer.poll(0)

if __name__ == "__main__":
    dem_data = load_json_list("../dataset/json/dem.json")
    sen_data = load_json_list("../dataset/json/sen.json")
    ernc_data = load_json_list("../dataset/json/ernc.json")

    ernc_index = 0  # Índice para recorrer el JSON de ERNC en orden
    day_counter = 0

    for i in range(300):  # Limitar a 300 horas
        hora_actual = i % 24  # Para tener horas entre 0-23
        # Actualizar el día cada 24 horas
        if i > 0 and hora_actual == 0:
            day_counter += 1

        # 1) Enviar DEM
        dem_msg = dem_data[i].copy()
        dem_msg["dia"] = day_counter
        dem_msg["hora"] = hora_actual
        send("dem_hora", dem_msg)
        print(f"[DEM] enviado: {dem_msg}")
        time.sleep(0.01)

        # 2) Enviar los 9 ERNC siguientes en orden
        for _ in range(9):
            ernc_msg = ernc_data[ernc_index].copy()
            ernc_msg["dia"] = day_counter
            ernc_msg["hora"] = hora_actual
            send("ernc_hora", ernc_msg)
            print(f"[ERNC] enviado: {ernc_msg}")
            ernc_index += 1
            time.sleep(0.01)
        
        # 3) Enviar SEN
        sen_msg = sen_data[i].copy()
        sen_msg["dia"] = day_counter
        sen_msg["hora"] = hora_actual
        send("sen_hora", sen_msg)
        print(f"[SEN] enviado: {sen_msg}")
        time.sleep(0.01)

    producer.flush()
    print("Todos los mensajes enviados.")
