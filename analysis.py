import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

# Importando os dados
path_instance_events = "/data/google-traces/instance_events/*.csv"
path_collection_events = "/data/google-traces/collection_events/*.csv"

# Carregando os dataframes

# dataframe com dados dos eventos das tarefas
df_instance_events = spark.read.csv(path_instance_events,header=True,sep=",") 
# dataframe com dados dos eventos dos jobs
df_collection_events = spark.read.csv(path_collection_events,header=True,sep=",")

## Quantos jobs são submetidos por hora?
jobs_submetidos = df_collection_events.filter(df_collection_events.type == 0)
jobs_submetidos_cont = jobs_submetidos.count()
last_job_submit = df_collection_events.agg({'time': 'max'})
first_job_submit = df_collection_events.agg({'time': 'min'})

time_interval_jobs = (last_job_submit[0].time - first_job_submit[0].time)

average_jobs_submited = jobs_submetidos_cont / (time_interval_jobs / 3600)

##Quantas tarefas são submetidas por hora?

tasks_submetidas = df_instance_events.filter(df_instance_events.type == 0)
tasks_submetidas_cont = tasks_submetidas.count()
last_task_submit = df_instance_events.agg({'time': 'max'})
first_task_submit = df_instance_events.agg({'time': 'min'})

time_interval_tasks = (last_task_submit - first_task_submit)

average_tasks_submited = tasks_submetidas_cont / (time_interval_tasks / 3600)



#  Como é a requisição de recursos computacionais (memória e CPU) do cluster durante o tempo?

df_instance_events.filter(df_instance_events.resource_request.cpus )