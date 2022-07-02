from matplotlib import pyplot as plt
import pandas as pd
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.appName('Q7').getOrCreate()

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

##### Quantos eventos de cada tipo são disparados para as tarefas? #####
type_counts = [0]*11
types = []*11

##Count de cada um dos tipos
counts = df_instance_events.groupBy("type").count().orderBy('type').collect()

for row in counts:
    type_counts[int(row['type'])] = int(row['count'])

for i in range(11):
    types.append(f"Tipo {i}")

# Histograma dos tipos 

print(type_counts)
print(types)

width = 0.5

fig, ax = plt.subplots()

ax.bar(types, type_counts, width, label='Frequência', color='red')

ax.set_ylabel('Frequência')
ax.set_xlabel('Tipos')
ax.legend()

plt.ticklabel_format(style='plain', axis='y')
plt.xticks(rotation = 90)
plt.show()