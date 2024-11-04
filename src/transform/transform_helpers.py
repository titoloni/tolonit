from typing import Tuple, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from src.utils.utils import get_partition_date


def transform_spec(
        list_data_frame_cliente: List[Tuple[int, str, DataFrame]],
        last_param_date: int,
        prev_param_date: int,
        is_full_type_process: bool,
        column_id: str,
        doc_type: str
) -> DataFrame:
    """
    Gera Delta entre partições ou retorna o full da última partição.
    """
    last_base_date, prev_base_date = get_partition_dates(list_data_frame_cliente, last_param_date, prev_param_date)

    if is_full_type_process:
        return get_distinct_partition(list_data_frame_cliente, last_base_date, column_id, doc_type)

    df_last_partition = get_distinct_partition(list_data_frame_cliente, last_base_date, column_id, doc_type)
    df_prev_partition = get_distinct_partition(list_data_frame_cliente, prev_base_date, column_id, doc_type)

    return get_insert_df(df_last_partition, df_prev_partition, column_id).unionByName(
        get_update_df(df_last_partition, df_prev_partition, column_id)
    )


def get_partition_dates(list_data_frame_cliente: List[Tuple[int, str, DataFrame]], last_param_date: int, prev_param_date: int) -> Tuple[int, int]:
    partitions_dates = [item[0] for item in list_data_frame_cliente]
    return (
        get_partition_date(last_param_date, partitions_dates, "last_date"),
        get_partition_date(prev_param_date, partitions_dates, ""),
    )


def get_insert_df(df_last_partition: DataFrame, df_prev_partition: DataFrame, column_id: str) -> DataFrame:
    return df_last_partition.join(df_prev_partition, f"num_{column_id}", "left_anti")


def get_update_df(df_last_partition: DataFrame, df_prev_partition: DataFrame, column_id: str) -> DataFrame:
    return (
        df_last_partition.alias("last")
        .join(df_prev_partition.alias("previous"), f"num_{column_id}")
        .where(col("last.des_nome_cliente_razao_social") != col("previous.des_nome_cliente_razao_social"))
        .select(col(f"last.num_{column_id}"), col("last.des_nome_cliente_razao_social"), col(f"last.des_{column_id}_status"))
    )


def get_distinct_partition(
        list_data_frame: List[Tuple[int, str, DataFrame]],
        anomesdia: int,
        column_id: str,
        doc_type: str
) -> DataFrame:
    data_frame = next(df for date, str, df in list_data_frame if date == anomesdia)

    if doc_type == "CNPJ9":
        data_frame = data_frame.filter(col("filial") == "0001")

    return data_frame.dropDuplicates([f"num_{column_id}"]).select(
        f"num_{column_id}", "des_nome_cliente_razao_social", f"des_{column_id}_status"
    )


def transform_delta(spec: DataFrame, cache: DataFrame, column_id: str) -> DataFrame:
    return spec.alias("spec").join(cache.alias("cache"), f"num_{column_id}", "left").select(
        col(f"spec.num_{column_id}"),
        col("spec.des_nome_cliente_razao_social"),
        col(f"spec.des_{column_id}_status"),
        col("cache.id_chave_dynamics"),
    )
