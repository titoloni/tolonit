# src/local_orchestrator.py
from pyspark.sql import SparkSession
from extract.extract_clientes import extract_clients
from utils.parameter import ParameterClass
from transform.transform_cnpj9 import transform_cnpj9
from transform.transform_cnpj14 import transform_cnpj14
from load.load_cnpj9 import load_cnpj9
from transform.transform_cnpj import transform_cnpj

def run_workflow():
    # Parametros de Entrada
    params = ParameterClass()

    # Start Spark Session
    spark = SparkSession.builder.master("local[*]").appName("Encart JOB").getOrCreate()

    # Extração
    clients_data = extract_clients(spark, params)

    # Transformação
    # cnpj9_data = transform_cnpj9(spark, clients_data, params)
    # cnpj14_data = transform_cnpj14(spark, clients_data, params)

    cnpj9_data = transform_cnpj(spark, clients_data, params, "CNPJ9")
    cnpj14_data = transform_cnpj(spark, clients_data, params, "CNPJ14" )

    print(">> CNPJ9 TRANSFORMADO")
    print(cnpj9_data.show())
    print(">> CNPJ14 TRANSFORMADO")
    print(cnpj14_data.show())

    # Carga
    # load_cnpj9(cnpj9_data)

if __name__ == "__main__":
    run_workflow()

