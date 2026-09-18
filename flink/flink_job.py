import json
import happybase
from datetime import datetime

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Types, Duration, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.common.time import Time
from pyflink.datastream.window import SlidingEventTimeWindows


def gravar_hbase(registro):
    """
    registro = (id_usuario, evento, produto, timestamp, contagem)
    Grava um alerta na tabela 'alertas' do HBase.
    """
    try:
        conn = happybase.Connection("hbase", port=9090, timeout=5000)
        conn.open()
        tabela = conn.table("alertas")
        row_key = f"{registro[0]}_{registro[3]}".encode()
        tabela.put(row_key, {
            b"info:id_usuario":  str(registro[0]).encode(),
            b"info:evento":      str(registro[1]).encode(),
            b"info:produto":     str(registro[2]).encode(),
            b"info:timestamp":   str(registro[3]).encode(),
            b"info:contagem":    str(registro[4]).encode(),
        })
        conn.close()
    except Exception as e:
        print(f"[HBase] Erro ao gravar: {e}")

class MeuTimestampAssigner(TimestampAssigner):

    def extract_timestamp(self, value, record_timestamp):
        tempo = datetime.fromisoformat(value[3])
        return int(tempo.timestamp() * 1000)


def contar_eventos(a, b):
    return (
        a[0],
        a[1],
        a[2],
        a[3],
        a[4] + b[4]
    )


def main():

    env = StreamExecutionEnvironment.get_execution_environment()

    env.set_parallelism(1)


    eventos = env.read_text_file(
        "/dados/eventos.log"
    )


    eventos_json = eventos.map(
        lambda x: json.loads(x),
        output_type=Types.MAP(
            Types.STRING(),
            Types.STRING()
        )
    )


    eventos_formatados = eventos_json.map(
        lambda x: (
            str(x["id_usuario"]),
            x["evento"],
            x["produto"],
            x["timestamp"],
            1
        ),
        output_type=Types.TUPLE([
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.INT()
        ])
    )


    eventos_com_watermark = (
        eventos_formatados
        .assign_timestamps_and_watermarks(
            WatermarkStrategy
            .for_bounded_out_of_orderness(
                Duration.of_seconds(10)
            )
            .with_timestamp_assigner(
                MeuTimestampAssigner()
            )
        )
    )


    resultado = (
        eventos_com_watermark
        .key_by(
            lambda x: x[0]
        )
        .window(
            SlidingEventTimeWindows.of(
                Time.minutes(5),
                Time.minutes(1)
            )
        )
        .reduce(
            contar_eventos
        )
    )


    # resultado.print()
    resultado.map(lambda x: (gravar_hbase(x), x)[1]).print()



    env.execute(
        "Semana 2 - Flink Watermark Sliding Window"
    )


if __name__ == "__main__":
    main()