import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.appName('Q5').getOrCreate()

# Importando os dados
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

##### Quanto tempo demora para a primeira tarefa de um job começar a ser executada? #######

##PARTE 1 - descobrindo os eventos com tempo minimo do tipo 3##

## Filtrando pela task scheduled e pelo instance_index para pegar a primeira tarefa do job
first_scheduled_tasks_by_job = df_instance_events.filter(df_instance_events.type == 3)

min_time_type_3_df = first_scheduled_tasks_by_job.groupBy('collection_id').agg({"time":"min"})

avg = min_time_type_3_df.agg({"min(time)": "avg"})

describe = min_time_type_3_df.describe("min(time)")

describe.show()

avg.show()

"""

first_submited_tasks_by_job.withColumn("row",row_number().over(window_partition_by_collection_id)) \
  .withColumn("min", F.min(col("time")).over(window_partition_by_collection_id)).where(col("row")==1) \
  .select("time") \
  .show()

  window_partition_by_collection_id = Window.partitionBy("collection_id").orderBy("time")
print(f"Count filteres by type 3: {first_scheduled_tasks_by_job.count()}")

##min_time_type_3_df = first_scheduled_tasks_by_job.groupBy("collection_id").agg({"time":"min"})
# 1 - collection_id.instance_index = id (tipo 3)
# 2 - min_time_type_3_df = first_scheduled_tasks_by_job.groupBy('collection_id').agg({"time":"min"}).orderBy("collection_id") - ultimo schedule da task (sucesso)
# 3 - tipo 0: collection_id.instance_index = id 
# 4 - min_time_type_0_df = df_instance_events.filter(df_instance_events.type == 0): recuperar instance_index
# 5 - joined_tables = min_time_type_3_df.join(min_time_type_0_df, ["collection_id", "instance_index"])
# 6 - joined_tables.max("type_0_time")
# 7 - 
# 1 - 1 - 0 - time1
# 1 - 1 - 0 - time2
# 1 - 1 - 0 - time3
# 4 - min_time_type_0_df = min_time_type_0_df.groupBy("collection_id").agg({"time" : "max"}).orderBy("collection_id") - ultimo submit da task (sucesso)
# 5 - join(tipo 0 com as do tipo 3, pela coluna id)

#Tamanho 5942
min_time_type_3_df = first_scheduled_tasks_by_job.groupBy('collection_id').agg({"time":"min"}).orderBy("collection_id")
#min_time_type_3_df = first_scheduled_tasks_by_job.select('collection_id',F.struct('instance_index', 'time').alias("TI")).groupBy('collection_id').agg(F.min("TI").alias("TI")).select('collection_id','TI.time','TI.instance_index').orderBy("collection_id")
min_time_type_3_df.show()

print(f"Count min_time_type_3_df: {min_time_type_3_df.count()}")

min_time_type_3_df = min_time_type_3_df.withColumnRenamed("min(time)", "time")

# min_time_type_3_df = min_time_type_3_df.join(first_scheduled_tasks_by_job, ["time"])
# print(f"Count after join: {min_time_type_3_df.count()}")
# min_time_type_3_df.show()

#min_time_type_3_df = first_scheduled_tasks_by_job.groupBy("collection_id").min("time").show()

##PARTE 2 - descobrindo os eventos com tempo minimo do tipo 0##

## Filtrando pela task submited e pelo instance_index para pegar a primeira tarefa do job
min_time_type_0_df = df_instance_events.filter(df_instance_events.type == 0)

#first_submited_tasks_by_job.withColumn("min", min(col("time")).over(events_ordered_by_time_and_grouped_by_the_collection_id)).where(col("row")==1)
min_time_type_0_df = min_time_type_0_df.groupBy("collection_id").agg({"time" : "max"}).orderBy("collection_id")

min_time_type_0_df = min_time_type_0_df.withColumnRenamed("max(time)", "time")

print(f"Count min_time_type_0_df: {min_time_type_0_df.count()}")

min_time_type_0_df.show()

##PARTE 3 - descobrindo o tempo para começar a executar(type_3_tasks_time - type_0_tasks_time)

result_df = min_time_type_3_df.join(min_time_type_0_df, ["collection_id", "instance_index", "time"]).withColumn("Result", min_time_type_3_df.time-min_time_type_0_df.time)

print(f"Count result_df: {result_df.count()}")

avg = result_df.agg({"Result": "avg"}).show()


## Filtrando pela task scheduled e pelo instance_index para pegar a primeira tarefa do job
first_scheduled_tasks_by_job = df_instance_events.filter(df_instance_events.type == 3)

## Dando join nos dois data frames no collection_id e adicionando uma nova coluna Result com a subtração dos tempos 
first_scheduled_tasks_by_job.join(first_submited_tasks_by_job, on="collection_id").withColumn("Result", first_scheduled_tasks_by_job.time-first_submited_tasks_by_job.time)

# Media de tempo para que a primeira tarefa comece a ser executada 
average_time_to_start_first_task = first_scheduled_tasks_by_job.select(F.avg('Result'))

print(f"Media de tempo para comecar a primeira tarefa: {average_time_to_start_first_task}")

#std deviation
std_deviation_time_to_start_first_task = first_scheduled_tasks_by_job.select(F.stddev('Result'))

print(f"Std Deviation de tempo para comecar a primeira tarefa:{std_deviation_time_to_start_first_task}")
"""