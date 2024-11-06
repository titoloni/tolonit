from pyspark.sql import SparkSession

from utils.datas_processamento import get_ultima_data_cache, get_ultima_data_mesh
from utils.delete_cache import delete_last_partition_cache
from utils.extract.extract_cnpj import extract_cnpj
from utils.load.load_cnpj import load_cnpj
from utils.transform.transform_cnpj import transform_cnpj
from utils.utils import get_schema


def main():
    # Datas de Processamento
    ultima_data_cache = get_ultima_data_cache()  # lista base - data
    ultima_data_mesh = get_ultima_data_mesh()

    # Spark Session
    spark = SparkSession.builder \
        .appName("CNPJ") \
        .getOrCreate()

    # Extract
    df_mesh_last, df_mesh_prev = extract_cnpj(spark, ultima_data_cache, ultima_data_mesh)

    # Transform
    df_transformed = transform_cnpj(df_mesh_last, df_mesh_prev, get_schema("CNPJ9"))

    # Load
    load_cnpj(df_transformed, "CNPJ9")

    # Delete last partition from cache
    delete_last_partition_cache(spark, ultima_data_cache, "CNPJ9")

    # Stop Spark Session
    spark.stop()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
