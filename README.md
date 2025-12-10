# bigData

## Dependencias

### Java

```
sudo apt install openjdk-17-jdk
```

### Flink

```
wget https://downloads.apache.org/flink/flink-1.18.1/flink-1.18.1-bin-scala_2.12.tgz
tar -xzf flink-1.18.1-bin-scala_2.12.tgz
mv flink-1.18.1 ~/flink

wget https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.1.0-1.18/flink-connector-kafka-3.1.0-1.18.jar
cp flink-connector-kafka-3.1.0-1.18.jar ~/flink/lib/

export FLINK_HOME=~/flink
export PATH=$PATH:$FLINK_HOME/bin
```

### Kafka

```
cd ~/flink/lib
wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.1/kafka-clients-3.6.1.jar
```

### Librerías Python

```
pip install apache-flink
pip install kafka-python
pip install confluent-kafka
```

## Ejecución

### Iniciar Flink localmente

```
cd ~/flink
./bin/start-cluster.sh
```

> Puedes verlo en [http://localhost:8081/#/overview](http://localhost:8081/#/overview)

### Iniciar Kafka y Zookeeper

```
docker-compose up -d
```

### Crear los topics

```
./create_topics.sh
```

### Ejecutar el productor

```
python3 master_producer.py
```

> Inicialmente eran 3 productores, uno por cada topic, pero se redujo a 1 para simplificar la ejecución y depuración del código.

### Ejecutar el pipeline Flink

```
python3 pipeline.py
```

> Al ejecutar el pipeline, este leerá mediciones de cada planta por "hora", las agrupará en ventanas de tiempo y exportará los datos a archivos `output_X.json`.
> El pipeline está diseñado para funcionar 24/7, por lo que debe detenerse manualmente con Ctrl + C o deteniendo Apache Flink desde la terminal.
