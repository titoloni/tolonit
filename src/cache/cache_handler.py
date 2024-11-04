from pyspark.sql import SparkSession, DataFrame

# Caminhos para os arquivos de entrada
PATH_CACHE_CNPJ9 = "..//data//input//cache_cnpj9.csv"
PATH_CACHE_CNPJ14 = "..//data//input//cache_cnpj14.csv"


def cache_cnpj(spark: SparkSession, doc_type: str) -> DataFrame:
    if doc_type == "CNPJ9":
        return cache_cnpj9(spark)
    elif doc_type == "CNPJ14":
        return cache_cnpj14(spark)
    else:
        raise ValueError("Tipo de documento inválido. Use 'CNPJ9' ou 'CNPJ14'.")


def cache_cnpj9(spark: SparkSession) -> DataFrame:
    # Implementação para o cache de CNPJ9
    return spark.read.option("header", True).csv(PATH_CACHE_CNPJ9)


def cache_cnpj14(spark: SparkSession) -> DataFrame:
    # Implementação para o cache de CNPJ14
    return spark.read.option("header", True).csv(PATH_CACHE_CNPJ14)
