import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.appName('Q4').getOrCreate()

# Caminho dos dados
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

##### Quantas tarefas são submetidas por hora? #####

tasks_submetidas = df_instance_events.filter(df_instance_events.type == 0)
tasks_submetidas_count = tasks_submetidas.count()

print(f"A quantidade de tarefas submetidas foi: {tasks_submetidas_count}\n")

last_task_submit = df_instance_events.groupby().max('time').collect()[0]['max(time)']

first_task_submit = df_instance_events.groupby().min('time').collect()[0]['min(time)']

time_interval_tasks = (last_task_submit - first_task_submit)

average_tasks_submited = tasks_submetidas_count / (time_interval_tasks / 3600)

print(f"Media de tarefas submetidas por hora: {average_tasks_submited}")