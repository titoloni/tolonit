from pyspark.sql import DataFrame

# Caminho de saída para os dados Delta CNPJ9
PATH_DELTA_CNPJ9 = "..//data//output//delta_cnpj9"
PATH_DELTA_CNPJ14 = "..//data//output//delta_cnpj14"


# ================================================================== #
# Load Delta Cnpj9
# ================================================================== #
def load_cnpj9(df: DataFrame):
    """
    Carrega um DataFrame para o sistema de arquivos em um formato especificado.

    """

    df.write.mode("overwrite").csv(PATH_DELTA_CNPJ9, header=True)

# ================================================================== #
# Load Delta Cnpj14
# ================================================================== #
def load_cnpj14(df: DataFrame):
    """
    Carrega um DataFrame para o sistema de arquivos em um formato especificado.

    """

    df.write.mode("overwrite").csv(PATH_DELTA_CNPJ14, header=True)
