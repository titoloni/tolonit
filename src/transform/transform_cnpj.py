import logging
from typing import Tuple, List
from pyspark.sql import DataFrame
from src.utils.parameter import ParameterClass
from src.transform.transform_helpers import (
    transform_spec,
    transform_delta,
)
from src.cache.cache_handler import cache_cnpj

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def transform_cnpj(spark, clients_data: List[Tuple[int, str, DataFrame]], params: ParameterClass, doc_type: str) -> DataFrame:
    """
    Transforma dados de CNPJ com base no tipo especificado (CNPJ9 ou CNPJ14).
    """
    last_param_date = params.get_last_base_date(doc_type)
    previous_param_date = params.get_previous_base_date(doc_type)
    is_full_type_process = params.get_type_process(doc_type) == "full"
    column_id = "cpfcnpj" if doc_type == "CNPJ9" else "cpfcnpj14"

    logging.info(f"Transformando {doc_type} ... {params.get_type_process(doc_type)}")

    # Executa transformações de especificação
    df_spec = transform_spec(clients_data, last_param_date, previous_param_date, is_full_type_process, column_id, doc_type)
    df_spec.show()

    # Carrega cache correspondente
    logging.info("Carregando Cache ...")
    df_cache = cache_cnpj(spark, doc_type)
    df_cache.show()

    # Realiza a transformação delta
    df_transformed = transform_delta(df_spec, df_cache, column_id)
    df_transformed.show()

    return df_transformed
