from pyspark.sql import DataFrame


def delete_last_partition_cache(spark: DataFrame, ultima_data_cache: int,  tabela: str):
    # Delete last partition from cache
    spark.sql(f"""
        DELETE FROM s3-raw-cnpj-raw-zone.{tabela}
        WHERE data = {ultima_data_cache}
    """)

