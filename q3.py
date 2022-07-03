from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.appName('Q3').getOrCreate()

# Caminho dos dados
path_collection_events = "C:/Users/Luan Monteiro/Desktop/Faculdade/spark-google-data-analysis/data/google-traces/collection_events/*.csv"

#Configurando Schema
collection_schema = StructType([
    StructField("time", IntegerType(), False),
    StructField("type", IntegerType(), False),
    StructField("collection_id", IntegerType(), False),
    StructField("priority", IntegerType(), False)
    ])


# Carregando os dataframes

# dataframe com dados dos eventos dos jobs
df_collection_events = spark.read.csv(path_collection_events, schema=collection_schema, header=True, sep=",")

##### Quantos jobs são submetidos por hora? #####

jobs_submetidos = df_collection_events.filter(df_collection_events.type == 0)

jobs_submetidos_count = jobs_submetidos.count()

print(f"A quantidade de jobs submetidas foi: {jobs_submetidos_count}\n")

last_job_submit = df_collection_events.groupby().max('time').collect()[0]['max(time)']
first_job_submit = df_collection_events.groupby().min('time').collect()[0]['min(time)']

time_interval_jobs = (last_job_submit - first_job_submit)

average_jobs_submited = jobs_submetidos_count / (time_interval_jobs / 3600)

print(f"Media de jobs submetidos por hora: {average_jobs_submited}")