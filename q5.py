import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.appName('Q5').getOrCreate()

# Importando os dados
path_instance_events = "C:/Users/Luan Monteiro/Desktop/Faculdade/spark-google-data-analysis/data/google-traces/instance_events/*.csv"

#Configurando Schema

instance_schema = StructType([
    StructField("time", IntegerType(), False),
    StructField("type", IntegerType(), False),
    StructField("collection_id", IntegerType(), False),
    StructField("priority", IntegerType(), False),
    StructField("instance_index", IntegerType(), False),
    StructField("resource_request.cpus", FloatType(), False),
    StructField("resource_request.memory", FloatType(), False)
    ])

# Carregando os dataframes

# dataframe com dados dos eventos das tarefas
df_instance_events = spark.read.csv(path_instance_events, schema = instance_schema, header=True, sep=",") 

## Quanto tempo demora para a primeira tarefa de um job começar a ser executada?

first_submited_tasks_by_job = df_instance_events.filter(df_instance_events.type == 0).groupBy('collection_id').min('time').collect()
first_scheduled_tasks_by_job = df_instance_events.filter(df_instance_events.type == 3).groupBy('collection_id').min('time').collect()
first_scheduled_tasks_by_job = first_scheduled_tasks_by_job.withColumn('Result', ( first_scheduled_tasks_by_job['min(time)'] - first_submited_tasks_by_job['min(time)'] ))

# Media de tempo para que a primeira tarefa comece a ser executada 
average_time_to_start_first_task = first_scheduled_tasks_by_job.select(F.avg('Result'))

print(f"Media de tempo para comecar a primeira tarefa: {average_time_to_start_first_task}")

#std deviation
std_deviation_time_to_start_first_task = first_scheduled_tasks_by_job.select(F.stddev('Result'))

print(f"Std Deviation de tempo para comecar a primeira tarefa:{std_deviation_time_to_start_first_task}")