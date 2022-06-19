import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName('Q1').getOrCreate()

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
df_instance_events = df_instance_events.withColumnRenamed("resource_request.cpus", "resource_request_cpus")
df_instance_events = df_instance_events.withColumnRenamed("resource_request.memory", "resource_request_memory")

#  Como é a requisição de recursos computacionais (memória e CPU) do cluster durante o tempo?
min_time = 0
print(f"Tempo minimo: {min_time}")
max_time = df_instance_events.agg({'time': 'max'}).collect()[0]['max(time)']
print(f"Tempo maximo: {max_time}")

n_intervals = 10
interval = (max_time - min_time) / n_intervals

cpu_request_means_over_time = []
memory_request_means_over_time = []
interval_average_array = []

for i in range(n_intervals):
    print(f"Intervalo Inicial{i}: {(i)*interval}")
    print(f"Intervalo Final{i+1}: {(i+1)*interval}")
    print(f"Intervalo Médio: {((((i+1)*interval) + (i*interval)) / 2)}")
    #acrescenta a média do intervalo final e o intervalo inicial em horas
    interval_average_array.append(((((i+1)*interval) + (i*interval)) / 2) / 3600)
    #acrescenta o resultado final do agrupamento de uso de CPU baseado no intevalo acima
    cpu_request_means_over_time.append(df_instance_events.filter((df_instance_events.time >= (i)*interval) & (df_instance_events.time < (i+1)*interval)).agg({'resource_request_cpus': 'avg'}).collect()[0]['avg(resource_request_cpus)'])
    memory_request_means_over_time.append(df_instance_events.filter((df_instance_events.time >= (i)*interval) & (df_instance_events.time < (i+1)*interval)).agg({'resource_request_memory': 'avg'}).collect()[0]['avg(resource_request_memory)'])
    print(f"Average {i}: {cpu_request_means_over_time[i]}")

#plotando um gráfico e exibindo o resultado
plt.title("Uso de CPU/Memory do cluster")
plt.xlabel("Tempo")
plt.ylabel("Média de uso de CPU/Memory")
plt.plot(interval_average_array, cpu_request_means_over_time, "CPU")
plt.plot(interval_average_array, memory_request_means_over_time, "Memory")
plt.show()

