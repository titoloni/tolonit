# from awsglue.context import GlueContext
# from pyspark.context import SparkContext
# from awsglue.job import Job
# from src.extract.extract_clients_task import extract_clients
# from src.transform.transform_prime_task import transform_prime
# from src.transform.transform_gold_task import transform_gold
# from src.transform.transform_silver_task import transform_silver
# from src.load.load_prime_task import load_prime
# from src.load.load_gold_task import load_gold
# from src.load.load_silver_task import load_silver
#
# # Configuração do contexto do Glue e job
# sc = SparkContext()
# glue_context = GlueContext(sc)
# job = Job(glue_context)
#
# def main():
#     # Task de Extração
#     extract_task = job.task(lambda: extract_clients(glue_context, "s3://bucket/source", "2023-10-01"))
#
#     # Tasks de Transformação, dependem do extract_task
#     transform_prime_task = job.task(lambda: transform_prime(extract_task.result()))
#     transform_gold_task = job.task(lambda: transform_gold(extract_task.result()))
#     transform_silver_task = job.task(lambda: transform_silver(extract_task.result()))
#
#     # Tasks de Carga, dependem das transformações
#     load_prime_task = job.task(lambda: load_prime(glue_context, transform_prime_task.result(), "s3://bucket/target/prime"))
#     load_gold_task = job.task(lambda: load_gold(glue_context, transform_gold_task.result(), "s3://bucket/target/gold"))
#     load_silver_task = job.task(lambda: load_silver(glue_context, transform_silver_task.result(), "s3://bucket/target/silver"))
#
#     # Executa o job com as tasks orquestradas
#     job.run()
#
# if __name__ == "__main__":
#     main()


