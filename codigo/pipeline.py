from pyflink.datastream import StreamExecutionEnvironment, KeyedProcessFunction
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.state import ValueStateDescriptor
import json
import os

# ------------------------------
# ProcessFunction para acumular registros, normalizar y calcular energia extra
# ------------------------------
class AggregateNormalizeExtra(KeyedProcessFunction):

    def open(self, runtime_context):
        descriptor = ValueStateDescriptor("buffer", Types.PICKLED_BYTE_ARRAY())
        self.buffer_state = runtime_context.get_state(descriptor)

        # Archivos de salida
        self.output_file_raw = os.path.join(os.getcwd(), "output_flink.json")
        self.output_file_normalized = os.path.join(os.getcwd(), "output_normalized.json")
        self.output_file_extra = os.path.join(os.getcwd(), "output_energia_extra.json")

        # Inicializar archivos JSON como arrays
        for fpath in [self.output_file_raw, self.output_file_normalized, self.output_file_extra]:
            with open(fpath, "w") as f:
                f.write("[\n")
        self.first_entry_raw = True
        self.first_entry_normalized = True
        self.first_entry_extra = True

    def process_element(self, value, ctx):
        data = json.loads(value)
        buffer = self.buffer_state.value() or {"demanda": None, "sen": None, "ernc": []}

        if "demanda_mwh" in data:
            buffer["demanda"] = data["demanda_mwh"]
        elif "sen_mwh" in data:
            buffer["sen"] = data["sen_mwh"]
        elif "planta" in data:
            buffer["ernc"].append(data)

        # Emitir solo cuando tengamos 1 DEM, 1 SEN y 9 ERNC
        if buffer["demanda"] is not None and buffer["sen"] is not None and len(buffer["ernc"]) == 9:
            # Raw JSON
            self.write_json(self.output_file_raw, buffer, raw=True)

            # Normalizado
            normalized = self.process_normalized(buffer)

            # Energia extra
            self.process_energia_extra(normalized)

            self.buffer_state.clear()
        else:
            self.buffer_state.update(buffer)

    def process_normalized(self, buffer):
        ernc = buffer["ernc"]
        sen = buffer["sen"]

        total_ernc = sum(e["generacion_mwh"] for e in ernc)
        hidraulicos = sum(e["generacion_mwh"] for e in ernc if e.get("tipo", "").lower() == "hidraulica")
        no_hidraulicos = total_ernc - hidraulicos

        factor_no_hidraulico = no_hidraulicos / total_ernc if total_ernc else 0
        factor_hidraulico = hidraulicos / total_ernc if total_ernc else 0

        produccion_no_hidraulica = factor_no_hidraulico * sen
        produccion_hidraulica = factor_hidraulico * sen

        normalized_output = {
            "dia": ernc[0]["dia"],
            "hora": ernc[0]["hora"],
            "demanda": buffer["demanda"],
            "produccion_no_hidraulica": produccion_no_hidraulica,
            "produccion_hidraulica": produccion_hidraulica
        }

        self.write_json(self.output_file_normalized, normalized_output, raw=False)
        return normalized_output

    def process_energia_extra(self, normalized):
        hidraulica = normalized["produccion_hidraulica"]
        if hidraulica <= 0:
            return  # solo calcular si hidraulica > 0

        diferencia = normalized["demanda"] - normalized["produccion_no_hidraulica"]
        if diferencia <= 0:
            energia_extra = hidraulica
        else:
            energia_extra = hidraulica - diferencia

        energia_extra_output = {
            "dia": normalized["dia"],
            "hora": normalized["hora"],
            "energia_extra": energia_extra
        }

        self.write_json(self.output_file_extra, energia_extra_output, raw=False)

    def write_json(self, filepath, data, raw=False):
        first_entry = {True: self.first_entry_raw,
                       False: self.first_entry_normalized if filepath != self.output_file_extra else self.first_entry_extra}[raw]
        with open(filepath, "a") as f:
            if not first_entry:
                f.write(",\n")
            f.write(json.dumps(data, indent=2))
        if raw:
            self.first_entry_raw = False
        elif filepath == self.output_file_normalized:
            self.first_entry_normalized = False
        else:
            self.first_entry_extra = False

    def close(self):
        for fpath in [self.output_file_raw, self.output_file_normalized, self.output_file_extra]:
            with open(fpath, "a") as f:
                f.write("\n]\n")

# ------------------------------
# Pipeline principal
# ------------------------------
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(5000)

    kafka_props = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "flink-group",
        "auto.offset.reset": "earliest"
    }

    dem_stream = env.add_source(FlinkKafkaConsumer("dem_hora", SimpleStringSchema(), kafka_props))
    sen_stream = env.add_source(FlinkKafkaConsumer("sen_hora", SimpleStringSchema(), kafka_props))
    ernc_stream = env.add_source(FlinkKafkaConsumer("ernc_hora", SimpleStringSchema(), kafka_props))

    unified = dem_stream.union(sen_stream).union(ernc_stream)

    keyed = unified.key_by(lambda x: (json.loads(x)["dia"], json.loads(x)["hora"]))

    keyed.process(AggregateNormalizeExtra(), output_type=Types.STRING())

    env.execute("Pipeline ERNC + SEN + DEM con energia extra")

if __name__ == "__main__":
    main()
