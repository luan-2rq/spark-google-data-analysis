from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.appName('Q5').getOrCreate()

# Caminho dos dados
path_instance_events = "C:/Users/Luan Monteiro/Desktop/Faculdade/spark-google-data-analysis/data/google-traces/instance_events/*.csv"

#Configurando Schema
instance_schema = StructType([
    StructField("time", LongType(), False),
    StructField("type", LongType(), False),
    StructField("collection_id", LongType(), False),
    StructField("priority", LongType(), False),
    StructField("instance_index", LongType(), False),
    StructField("resource_request.cpus", FloatType(), False),
    StructField("resource_request.memory", FloatType(), False)
    ])

# Carregando os dataframes

# dataframe com dados dos eventos das tarefas
df_instance_events = spark.read.csv(path_instance_events, schema = instance_schema, header=True, sep=",") 

##### Quanto tempo demora para a primeira tarefa de um job começar a ser executada? #####

## Filtrando pela task scheduled e pelo instance_index para pegar a primeira tarefa do job
first_scheduled_tasks_by_job = df_instance_events.filter(df_instance_events.type == 3)

min_time_type_3_df = first_scheduled_tasks_by_job.groupBy('collection_id').agg({"time":"min"})

avg = min_time_type_3_df.agg({"min(time)": "avg"})

describe = min_time_type_3_df.describe("min(time)")

describe.show()

avg.show()
