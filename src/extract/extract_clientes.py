# src/extract/extract_clientes.py
import logging
from typing import List, Tuple

from pyspark.sql import SparkSession, DataFrame

from src.utils.parameter import ParameterClass

# Configurando o nível de log e o formato da mensagem
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

PATH_SPEC = "../data/input/spec_clientes.csv"


# =============================================================================#
# Extração de Particoes da Base do Data Mesh
# =============================================================================#
def extract_clients(spark: SparkSession, parameters: ParameterClass) -> List[Tuple[int, str, DataFrame]]:
    # Dates from parameter
    partitions_dates = parameters.get_all_partition_dates()

    # Ler Data Mesh
    spark.read.option("header", True).csv(PATH_SPEC).createOrReplaceTempView(
        "spec_cliente"
    )

    # Obter a data da última e da partição anterior
    last_anomesdia = _get_max_partition_date(spark, None)
    previous_anomesdia = _get_max_partition_date(spark, last_anomesdia)

    # Adicionar as datas aos parâmetros de processamento
    partitions_dates.update([last_anomesdia, previous_anomesdia])

    # Carregar partições pela data
    list_data_frame_cliente = [
        (
            partition_date,
            'last' if partition_date == last_anomesdia else 'prev' if partition_date == previous_anomesdia else '',
            spark.sql(f"SELECT * FROM spec_cliente WHERE anomesdia = {partition_date}"),
        )
        for partition_date in partitions_dates
    ]

    # Filtrar partições vazias e registrar as que não foram encontradas
    non_empty_partitions = []
    for partition_date, type, df in list_data_frame_cliente:
        if df.count() > 0:
            non_empty_partitions.append((partition_date, type, df))
        else:
            print(f">> Partition {partition_date} not found")

    return non_empty_partitions


# =============================================================================#
def _get_max_partition_date(spark: SparkSession, exclude_date: int) -> int:
    query = f"SELECT MAX(anomesdia) as max_anomesdia FROM spec_cliente"
    if exclude_date is not None:
        query += f" WHERE anomesdia != {exclude_date}"

    result = spark.sql(query)
    return result.first()["max_anomesdia"]
