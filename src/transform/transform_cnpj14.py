# src/transform/transform_cnpj14.py

import logging
from typing import Tuple, List

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, current_date, col

from src.utils.parameter import ParameterClass
from src.utils.utils import get_partition_date
from src.cache.cache_handler import cache_cnpj14

# Configurando o nível de log e o formato da mensagem
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# =============================================================================#
# Gerar Base Delta de cnpj14
# =============================================================================#
def transform_cnpj14(spark, clients_data: List[Tuple[int, str, DataFrame]], params: ParameterClass) -> DataFrame:
    last_param_date = params.get_last_base_date("CNPJ14")
    previous_param_date = params.get_previous_base_date("CNPJ14")
    is_full_type_process = params.get_type_process("CNPJ14") == "full"

    logging.info(f"Transformando cnpj14 ... {params.get_type_process('CNPJ14')}")
    df_spec = _transform_spec_cnpj14(
        clients_data,
        last_param_date,
        previous_param_date,
        is_full_type_process,
    )
    df_spec.show()

    logging.info("Get Cache ...")
    df_cache = cache_cnpj14(spark)
    df_cache.show()

    df_transformed = _transform_delta(df_spec, df_cache)
    df_transformed.show()

    return df_transformed

# =============================================================================#
# Gerar Delta entre partições do DataMesh ou retornar full da última partição
# =============================================================================#
def _transform_spec_cnpj14(
        list_data_frame_cliente: List[Tuple[int, str, DataFrame]],
        last_param_date: int,
        prev_param_date: int,
        is_full_type_process: bool,
) -> DataFrame:
    last_base_date, prev_base_date = _get_partition_dates(
        list_data_frame_cliente, last_param_date, prev_param_date
    )

    if is_full_type_process:
        return _get_distinct_partition_cnpj14(list_data_frame_cliente, last_base_date)

    df_last_partition = _get_distinct_partition_cnpj14(
        list_data_frame_cliente, last_base_date
    )
    df_prev_partition = _get_distinct_partition_cnpj14(
        list_data_frame_cliente, prev_base_date
    )

    return _get_insert_df(df_last_partition, df_prev_partition).unionByName(
        _get_update_df(df_last_partition, df_prev_partition)
    )


# =============================================================================#
def _get_partition_dates(
        list_data_frame_cliente: List[Tuple[int, str, DataFrame]],
        last_param_date: int,
        prev_param_date: int,
) -> Tuple[int, int]:
    partitions_dates = [item[0] for item in list_data_frame_cliente]
    return (
        get_partition_date(last_param_date, partitions_dates, "last_date"),
        get_partition_date(prev_param_date, partitions_dates, ""),
    )


# =============================================================================#
def _get_insert_df(
        df_last_partition: DataFrame, df_prev_partition: DataFrame
) -> DataFrame:
    return (
        df_last_partition.join(df_prev_partition, "num_cpfcnpj14", "left_anti")
        # .withColumn("operation", lit("INSERT"))
        # .withColumn("processing_date", current_date())
    )


# =============================================================================#
def _get_update_df(
        df_last_partition: DataFrame, df_prev_partition: DataFrame
) -> DataFrame:
    return (
        df_last_partition.alias("last")
        .join(df_prev_partition.alias("previous"), "num_cpfcnpj14")
        .where(
            col("last.des_nome_cliente_razao_social")
            != col("previous.des_nome_cliente_razao_social")
        )
        .select(
            col("last.num_cpfcnpj14"),
            col("last.des_nome_cliente_razao_social"),
            col("last.des_cpfcnpj14_status"),
            # lit("UPDATE").alias("operation"),
            # current_date().alias("processing_date"),
        )
    )


# =============================================================================#
def _get_distinct_partition_cnpj14(
        list_data_frame: List[Tuple[int, str, DataFrame]], anomesdia: int
) -> DataFrame:
    data_frame = next(df for date, str, df in list_data_frame if date == anomesdia)
    return (
        data_frame.dropDuplicates(["num_cpfcnpj14"])
        .select("num_cpfcnpj14", "des_nome_cliente_razao_social", "des_cpfcnpj14_status")
    )


# =============================================================================#
# Gerar Delta entre DataMesh e Cache Dynamics
# =============================================================================#
def _transform_delta(spec: DataFrame, cache: DataFrame) -> DataFrame:
    return (
        spec.alias("spec")
        .join(cache.alias("cache"), "num_cpfcnpj14", "left")
        .select(
            col("spec.num_cpfcnpj14"),
            col("spec.des_nome_cliente_razao_social"),
            col("spec.des_cpfcnpj14_status"),
            # col("spec.operation"),
            col("cache.id_chave_dynamics"),
            # current_date().alias("processing_date"),
        )
    )
