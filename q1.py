from inspect import stack
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import pandas as pd
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName('Q1').getOrCreate()

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
df_instance_events = df_instance_events.withColumnRenamed("resource_request.cpus", "resource_request_cpus")
df_instance_events = df_instance_events.withColumnRenamed("resource_request.memory", "resource_request_memory")
df_instance_events = df_instance_events.filter(df_instance_events.type == 3)

#####  Como é a requisição de recursos computacionais (memória e CPU) do cluster durante o tempo? #####

min_time = 0
max_time = df_instance_events.agg({'time': 'max'}).collect()[0]['max(time)']

n_intervals = 21
interval = (max_time - min_time) / n_intervals

cpu_request_means_over_time = []
memory_request_means_over_time = []
intervals = []

for i in range(n_intervals):
    current_interval = f"Intervalo {i+1}"
    intervals.append(current_interval)

    current_cpu_request_avg = df_instance_events.filter((df_instance_events.time >= (i)*interval) & (df_instance_events.time < (i+1)*interval)).agg({'resource_request_cpus': 'avg'}).collect()[0]['avg(resource_request_cpus)']
    current_memory_request_avg = df_instance_events.filter((df_instance_events.time >= (i)*interval) & (df_instance_events.time < (i+1)*interval)).agg({'resource_request_memory': 'avg'}).collect()[0]['avg(resource_request_memory)']
    
    if current_cpu_request_avg != None:
        cpu_request_means_over_time.append(current_cpu_request_avg)
    else:
        cpu_request_means_over_time.append(0)

    print(f"Media de CPU Request {len(cpu_request_means_over_time)-1}: {cpu_request_means_over_time[len(cpu_request_means_over_time)-1]}")

    if current_memory_request_avg != None:
        memory_request_means_over_time.append(current_memory_request_avg)
    else:
        memory_request_means_over_time.append(0)

    print(f"Media de Memory Request {len(cpu_request_means_over_time)-1}: {cpu_request_means_over_time[len(cpu_request_means_over_time)-1]}")

#### Graficos ####

df = pd.DataFrame({ 
    'Intervalos de Tempo': intervals, 
    'Media(CPU Request)': cpu_request_means_over_time,
    'Media(Memory Request)': memory_request_means_over_time 
}) 

### Grafico 1 - Stacked Cpu Request and Memory Request ###

width = 0.5

fig, ax = plt.subplots()

ax.bar(intervals, cpu_request_means_over_time, width, label='Média(CPU Request)', color='blue')
ax.bar(intervals, memory_request_means_over_time, width, bottom=cpu_request_means_over_time,
       label='Média(Memory Request)', color='orange')

ax.set_ylabel('Médias')
ax.set_xlabel('Intervalos de Tempo')
ax.set_title('Média de requisições de CPU e Memoria')
ax.legend()

plt.xticks(rotation = 90)

plt.show()

### Grafico 2 - CPU request means ###

#ax = df.plot(x="Intervalos de Tempo", y="Media(Memory Request)", kind="bar", color='orange')
#plt.show()

### Grafico 3 - Memory request means ###

#ax = df.plot(x="Intervalos de Tempo", y="Media(CPU Request)", kind="bar", color='blue')
#plt.show()


