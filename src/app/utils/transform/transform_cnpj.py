from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import current_date

from utils.cache.cache_handler import carregar_dados_cache_com_filtro


def transform(df_mesh_last: DataFrame, df_mesh_prev: DataFrame, spark: SparkSession) -> DataFrame:
    """
    Aplica transformações entre `df_mesh_last` e `df_mesh_prev` e carrega dados do S3 com filtro.
    """
    # Calcula o delta entre as partições
    df_mesh = _delta_entre_particoes(df_mesh_last, df_mesh_prev)

    # Extrai os valores de "cnpj_raiz" para filtrar no S3
    valores_para_filtrar = df_mesh.select("cnpj_raiz").rdd.flatMap(lambda x: x).collect()

    # Carrega dados do S3 aplicando o filtro de "cnpj_raiz" em lotes
    df_s3_filtrado = carregar_dados_cache_com_filtro(spark, valores_para_filtrar)

    # Combina o resultado do delta com os dados carregados do S3 (se necessário) <<<
    
    # Ajuste conforme as necessidades de combinação <<<<<
    df_resultado = df_mesh.union(df_s3_filtrado)

    return df_resultado


def _delta_entre_particoes(df_mesh_last: DataFrame, df_mesh_prev: DataFrame):
    """
    join pelo cnpj_raiz
    quando existir em df_mesh_last nao existir em df_mesh_prev, Incluir (I)
    quando existir em df_mesh_last e df_mesh_prev como a razao_social ou id_chave_cliente for diferente, Alterar (A)
    quando nao existir em df_mesh_last mas existir em df_mesh_prev, Excluir (D)

    :param df_mesh_last:
    :param df_mesh_prev:
    :return:
    """

    # Retirar duplicados de df_mesh_last e df_mesh_prev
    df_mesh_last = (
        df_mesh_last.dropDuplicates(["cnpj_raiz"])
        .select("cnpj_raiz",
                "razao_social",
                "id_chave_cliente",
                "status_cnpj"))

    df_mesh_prev = (
        df_mesh_prev.dropDuplicates(["cnpj_raiz"])
        .select("cnpj_raiz",
                "razao_social",
                "id_chave_cliente",
                "status_cnpj"))

    process_date = current_date()

    # Realizar um join `outer` para identificar inclusões, alterações e exclusões em uma única operação
    df_merged = df_mesh_last.alias("last") \
        .join(df_mesh_prev.alias("prev"), "cnpj_raiz", "outer") \
        .select(
        F.coalesce(F.col("last.cnpj_raiz"), F.col("prev.cnpj_raiz")).alias("cnpj_raiz"),
        F.col("last.razao_social").alias("razao_social_last"),
        F.col("prev.razao_social").alias("razao_social_prev"),
        F.col("last.id_chave_cliente").alias("id_chave_cliente_last"),
        F.col("prev.id_chave_cliente").alias("id_chave_cliente_prev"),
        F.col("last.status_cnpj").alias("status_cnpj_last"),
        F.col("prev.status_cnpj").alias("status_cnpj_prev")
    )

    # Definir tipo de ação com base nas condições
    df_result = df_merged.withColumn(
        "acao",
        F.when(F.col("razao_social_last").isNotNull() & F.col("razao_social_prev").isNull(), "I")  # Inclusões
        .when(F.col("razao_social_last").isNull() & F.col("razao_social_prev").isNotNull(), "D")  # Exclusões
        .when((F.col("razao_social_last") != F.col("razao_social_prev")) |
              (F.col("id_chave_cliente_last") != F.col("id_chave_cliente_prev")), "A")  # Alterações
    )

    # Selecionar apenas as colunas finais e adicionar `data_processamento`
    df_result = df_result.select(
        "cnpj_raiz",
        F.coalesce(F.col("razao_social_last"), F.col("razao_social_prev")).alias("razao_social"),
        F.coalesce(F.col("id_chave_cliente_last"), F.col("id_chave_cliente_prev")).alias("id_chave_cliente"),
        F.coalesce(F.col("status_cnpj_last"), F.col("status_cnpj_prev")).alias("status_cnpj"),
        "acao"
    ).withColumn("data_processamento", process_date)

    return df_result

# # Adicionar coluna para identificar data de processamento
# process_date = current_date()
#
# # Inclusões: registros que estão em df_mesh_last, mas não estão em df_mesh_prev
# df_inclusoes = df_mesh_last.join(df_mesh_prev, "cnpj_raiz", "left_anti") \
#     .withColumn("acao", lit("I")) \
#     .withColumn("data_processamento", process_date)
#
# # Alterações: registros presentes em ambos os DataFrames, mas com diferenças na razão social ou id_chave_cliente
# df_alteracoes = df_mesh_last.alias("last").join(df_mesh_prev.alias("prev"), "cnpj_raiz", "inner") \
#     .where((F.col("last.razao_social") != F.col("prev.razao_social")) |
#            (F.col("last.id_chave_cliente") != F.col("prev.id_chave_cliente"))) \
#     .select("last.cnpj_raiz", "last.razao_social", "last.id_chave_cliente", "last.status_cnpj") \
#     .withColumn("acao", lit("A")) \
#     .withColumn("data_processamento", process_date)
#
# # Exclusões: registros que estão em df_mesh_prev, mas não estão em df_mesh_last
# df_exclusoes = df_mesh_prev.join(df_mesh_last, "cnpj_raiz", "left_anti") \
#     .select("cnpj_raiz", "razao_social", "id_chave_cliente", "status_cnpj") \
#     .withColumn("acao", lit("D")) \
#     .withColumn("data_processamento", process_date)
#
# # União dos DataFrames para obter o resultado final
# df_mesh = df_inclusoes.unionByName(df_alteracoes).unionByName(df_exclusoes)
#
# return df_mesh

# from pyspark.sql import DataFrame
# from pyspark.sql.functions import lit, current_date
#
#
# def transform(df_mesh_last: DataFrame, df_mesh_prev: DataFrame, schema) -> DataFrame:
#     # Retirar duplicados de df_mesh_last e df_mesh_prev
#     df_mesh_last = df_mesh_last.dropDuplicates(["cnpj_raiz"]).select("cnpj_raiz", "razao_social", "id_chave_cliente",
#                                                                      "status_cnpj")
#     df_mesh_prev = df_mesh_prev.dropDuplicates(["cnpj_raiz"]).select("cnpj_raiz", "razao_social", "id_chave_cliente",
#                                                                      "status_cnpj")
#
#     # Adicionar coluna para identificar data de processamento
#     process_date = current_date()
#
#     # Inclusões: registros que estão em df_mesh_last, mas não estão em df_mesh_prev
#     df_inclusoes = df_mesh_last.join(df_mesh_prev, "cnpj_raiz", "left_anti") \
#         .withColumn("acao", lit("I")) \
#         .withColumn("data_processamento", process_date)
#
#     # Alterações: registros presentes em ambos os DataFrames, mas com diferenças na razão social ou id_chave_cliente
#     df_alteracoes = df_mesh_last.alias("last").join(df_mesh_prev.alias("prev"), "cnpj_raiz", "inner") \
#         .where((F.col("last.razao_social") != F.col("prev.razao_social")) |
#                (F.col("last.id_chave_cliente") != F.col("prev.id_chave_cliente"))) \
#         .select("last.cnpj_raiz", "last.razao_social", "last.id_chave_cliente", "last.status_cnpj") \
#         .withColumn("acao", lit("A")) \
#         .withColumn("data_processamento", process_date)
#
#     # Exclusões: registros que estão em df_mesh_prev, mas não estão em df_mesh_last
#     df_exclusoes = df_mesh_prev.join(df_mesh_last, "cnpj_raiz", "left_anti") \
#         .select("cnpj_raiz", "razao_social", "id_chave_cliente", "status_cnpj") \
#         .withColumn("acao", lit("D")) \
#         .withColumn("data_processamento", process_date)
#
#     # União dos DataFrames para obter o resultado final
#     df_mesh = df_inclusoes.unionByName(df_alteracoes).unionByName(df_exclusoes)
#
#     return df_mesh





# from pyspark.sql import SparkSession, DataFrame
# 
# 
# def carregar_dados_cache_com_filtro(spark: SparkSession, valores_para_filtrar: list, batch_size: int = 1000) -> DataFrame:
#     """
#     Carrega dados do S3 em lotes, filtrando pela lista de valores em `cnpj_raiz`.
#     """
#     df_total = None
#     for i in range(0, len(valores_para_filtrar), batch_size):
#         batch = valores_para_filtrar[i:i + batch_size]
#         df_batch = spark.read.parquet("s3://meu-bucket/minha-pasta").filter(F.col("cnpj_raiz").isin(batch))
#         df_total = df_batch if df_total is None else df_total.union(df_batch)
# 
#     return df_total

============================================================================================================================
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
