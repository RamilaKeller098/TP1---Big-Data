import json

from datetime import datetime

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Types, Duration, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.common.time import Time
from pyflink.datastream.window import SlidingEventTimeWindows


class MeuTimestampAssigner(TimestampAssigner):

    def extract_timestamp(self, value, record_timestamp):

        tempo = datetime.fromisoformat(value[3])

        return int(tempo.timestamp() * 1000)



def main():

    env = StreamExecutionEnvironment.get_execution_environment()

    env.set_parallelism(1)


    # Eventos simulando o stream da loja

    eventos = env.from_collection(
        [
            '{"id_usuario":"1","evento":"carrinho","produto":"Notebook","timestamp":"2026-09-04T21:30:00"}',
            '{"id_usuario":"1","evento":"carrinho","produto":"Mouse","timestamp":"2026-09-04T21:30:05"}',
            '{"id_usuario":"2","evento":"compra","produto":"Celular","timestamp":"2026-09-04T21:30:10"}',
            '{"id_usuario":"3","evento":"clique","produto":"Teclado","timestamp":"2026-09-04T21:30:15"}'
        ],
        type_info=Types.STRING()
    )


    # Converter JSON

    eventos_json = eventos.map(
        lambda x: json.loads(x),
        output_type=Types.MAP(
            Types.STRING(),
            Types.STRING()
        )
    )


    # Organizar campos

    eventos_formatados = eventos_json.map(
        lambda x: (
            x["id_usuario"],
            x["evento"],
            x["produto"],
            x["timestamp"]
        ),
        output_type=Types.TUPLE([
            Types.STRING(),
            Types.STRING(),
            Types.STRING(),
            Types.STRING()
        ])
    )


    # WATERMARK
    # Aceita eventos atrasados em até 10 segundos

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


    # JANELA DESLIZANTE
    #
    # Janela: 5 minutos
    # Slide: 1 minuto

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
            lambda a, b: a
        )
    )


    resultado.print()


    env.execute(
        "Semana 2 - Flink Watermark Sliding Window"
    )


if __name__ == "__main__":
    main()