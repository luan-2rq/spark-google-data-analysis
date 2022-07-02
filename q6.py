from matplotlib import pyplot as plt
import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.appName('Q6').getOrCreate()

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


##### Quantos eventos de cada tipo são disparados para os jobs? #####
type_counts = [0]*11
types = []*11

##Count de cada um dos tipos
counts = df_collection_events.groupBy("type").count().orderBy('type').collect()

for row in counts:
    type_counts[int(row['type'])] = int(row['count'])

for i in range(11):
    types.append(f"Tipo {i}")

# Histograma dos tipos 

print(type_counts)
print(types)

df = pd.DataFrame({ 
    'Tipos': types,
    'Frequencia': type_counts
}) 

ax = df.plot(x="Tipos", y="Frequencia", kind="bar", color='orange')

plt.show()