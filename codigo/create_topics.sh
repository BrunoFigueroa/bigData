#!/bin/bash

echo "Creando topics en Kafka (contenedor)..."

TOPICS=("dem_hora" "sen_hora" "ernc_hora")

for t in "${TOPICS[@]}"; do
    echo "Creando topic: $t"
    docker exec kafka kafka-topics --create \
        --topic "$t" \
        --bootstrap-server localhost:9092 \
        --if-not-exists
done

echo "Listando topics:"
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
