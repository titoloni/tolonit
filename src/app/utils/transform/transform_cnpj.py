from pyspark.sql import DataFrame
from pyspark.sql.functions import lit


def transform_cnpj(df_mesh_last: DataFrame, df_mesh_prev: DataFrame, schema) -> DataFrame:

    df_mesh_last, df_mesh_prev = _drop_duplicates(df_mesh_last, df_mesh_prev, schema)

    if schema['tabela'] == "CNPJ9":
        df_mesh = _transform_cnpj9(df_mesh_last, df_mesh_prev)

    elif schema['tabela'] == "CNPJ14":
        df_mesh = _transform_cnpj14(df_mesh_last, df_mesh_prev)

    else:
        raise ValueError("Invalid table name. Must be CNPJ9, CNPJ2 or CNPJ1.")

    return df


def _drop_duplicates(df_mesh_last, df_mesh_prev, schema):
    if schema['tabela'] == 'cnpj9':
        df_mesh_last = df_mesh_last.dropDuplicates(["cnpj_basico", "cnpj_ordem", "cnpj_dv", "cnpj"])
        df_mesh_prev = df_mesh_prev.dropDuplicates(["cnpj_basico", "cnpj_ordem", "cnpj_dv", "cnpj"])

    elif schema['tabela'] == 'cnpj14':
        df_mesh_last = df_mesh_last.dropDuplicates(["cnpj_raiz", "cnpj"])
        df_mesh_prev = df_mesh_prev.dropDuplicates(["cnpj_raiz", "cnpj"])

    else:
        raise ValueError("Invalid table name. Must be CNPJ9, CNPJ2 or CNPJ1.")

    return df_mesh_last, df_mesh_prev


# def _transform_cnpj9(df_mesh_last: DataFrame, df_mesh_prev: DataFrame) -> DataFrame:
#     # Realiza o join dos DataFrames usando o CNPJ9
#     df_joined = df_mesh_last.alias("last").join(
#         df_mesh_prev.alias("prev"),
#         on="cnpj9",
#         how="full_outer"
#     )
#
#     # Adiciona a coluna "acao" com as condições solicitadas
#     df_result = df_joined.withColumn(
#         "acao",
#         F.when(
#             (F.col("last.cnpj9").isNotNull()) & (F.col("prev.cnpj9").isNull()), "I"  # Inclui
#         ).when(
#             (F.col("last.cnpj9").isNotNull()) & (F.col("prev.cnpj9").isNotNull()) &
#             ((F.col("last.razao_social") != F.col("prev.razao_social")) |
#              (F.col("last.id_cliente") != F.col("prev.id_cliente"))), "A"  # Atualiza
#         ).when(
#             (F.col("last.cnpj9").isNull()) & (F.col("prev.cnpj9").isNotNull()), "E"  # Exclui
#         )
#     )
#
#     # Seleciona as colunas finais, incluindo "acao" para o resultado final
#     df_final = df_result.select("cnpj9", "acao", "last.*", "prev.*")
#
#     return df_final

def _transform_cnpj9(df_mesh_last: DataFrame, df_mesh_prev: DataFrame, spark: SparkSession) -> DataFrame:
    # Registra os DataFrames temporariamente como tabelas
    df_mesh_last.createOrReplaceTempView("mesh_last")
    df_mesh_prev.createOrReplaceTempView("mesh_prev")

    # Executa a query para aplicar as condições de inclusão, atualização e exclusão
    query = """
        SELECT
            COALESCE(last.cnpj9, prev.cnpj9) AS cnpj9,
            CASE 
                WHEN last.cnpj9 IS NOT NULL AND prev.cnpj9 IS NULL THEN 'I'  -- Incluir
                WHEN last.cnpj9 IS NOT NULL AND prev.cnpj9 IS NOT NULL 
                     AND (last.razao_social != prev.razao_social OR last.id_cliente != prev.id_cliente) THEN 'A'  -- Atualizar
                WHEN last.cnpj9 IS NULL AND prev.cnpj9 IS NOT NULL THEN 'E'  -- Excluir
                ELSE NULL
            END AS acao,
            last.*, 
            prev.*
        FROM mesh_last AS last
        FULL OUTER JOIN mesh_prev AS prev
        ON last.cnpj9 = prev.cnpj9
    """

    # Executa a query
    df_result = spark.sql(query)
    return df_result


def _transform_cnpj14(df_mesh_last: DataFrame, df_mesh_prev: DataFrame) -> DataFrame:
    # Join
    df = df_mesh_last.join(df_mesh_prev, on=["cnpj_raiz", "cnpj"], how="left_anti")

    # Select
    df = df.select("cnpj_raiz", "cnpj")

    return df
