from pyspark.sql import DataFrame


def load_cnpj(df_transformed: DataFrame, table: str):
    # load s3
    df_transformed \
        .write \
        .mode("overwrite") \
        .parquet(f"s3://s3-raw-cnpj-raw-zone/{table}")
