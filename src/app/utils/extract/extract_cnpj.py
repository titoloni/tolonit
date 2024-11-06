from typing import Tuple

from pyspark.sql import DataFrame, SparkSession


def extract_cnpj(
        spark: SparkSession,
        ultima_data_cache: int,
        ultima_data_mesh: int
) -> Tuple[DataFrame, DataFrame]:
    # Extrair Data Mesh com ultima data do Cache
    df_mesh_last_partition = spark.read.parquet(f"s3://s3-raw-cnpj-raw-zone/mesh={ultima_data_mesh}/")

    # Extrair Data Cache com ultima data do Cache
    df_mesh_prev_partition = spark.read.parquet(f"s3://s3-raw-cnpj-raw-zone/data={ultima_data_cache}/")

    return df_mesh_last_partition, df_mesh_prev_partition
