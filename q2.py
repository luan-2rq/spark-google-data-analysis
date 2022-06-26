import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.appName('Q2').getOrCreate()

# Importando os dados da coleção
path_collection_events = "C:/Users/Luan Monteiro/Desktop/Faculdade/spark-google-data-analysis/data/google-traces/collection_events/*.csv"
# Importando os dados da instância
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


#Configurando Schema
collection_schema = StructType([
    StructField("time", IntegerType(), False),
    StructField("type", IntegerType(), False),
    StructField("collection_id", IntegerType(), False),
    StructField("priority", IntegerType(), False)
    ])

# Carregando os dataframes

# dataframe com dados dos eventos dos jobs
df_collection_events = spark.read.csv(path_collection_events, schema = collection_schema, header=True, sep=",") 
# dataframe com dados dos eventos das tarefas
df_instance_events = spark.read.csv(path_instance_events, schema = instance_schema, header=True, sep=",")
df_instance_events = df_instance_events.withColumnRenamed("resource_request.cpus", "resource_request_cpus")
df_instance_events = df_instance_events.withColumnRenamed("resource_request.memory", "resource_request_memory")
#As diversas categorias de jobs possuem características diferentes 
#(requisição de recursos computacionais, frequência de submissão, etc.)?

df_collection_events.filter


# ordenando a coleção
df_collection_events.orderBy(['priority'], ascending=[True])

# separando em categorias (jobs)
free_tier = df_collection_events.filter(df_collection_events.priority <= 99)
best_effort = df_collection_events.filter((df_collection_events.priority >= 100) & (df_collection_events.priority <= 115))
mid_tier = df_collection_events.filter((df_collection_events.priority >= 116) & (df_collection_events.priority <= 119))
production_tier = df_collection_events.filter((df_collection_events.priority >= 120) & (df_collection_events.priority <= 359))
monitoring_tier = df_collection_events.filter(df_collection_events.priority >= 360)

# separando em categorias (tarefas)
free_tier_instance = df_instance_events.filter(df_instance_events.priority <= 99)
best_effort_instance = df_instance_events.filter((df_instance_events.priority >= 100) & (df_instance_events.priority <= 115))
mid_tier_instance = df_instance_events.filter((df_instance_events.priority >= 116) & (df_instance_events.priority <= 119))
production_tier_instance = df_instance_events.filter((df_instance_events.priority >= 120) & (df_instance_events.priority <= 359))
monitoring_tier_instance = df_instance_events.filter(df_instance_events.priority >= 360)

# requisição de recursos computacionais de cada categoria

def reqRecursos(category, category_name):
    ## cpus
    print(f'Category {category_name}:')

    print("\nCPU:")
    describe_cpus = category.describe('resource_request_cpus').show()
    variance_cpus = category.select(F.variance('resource_request_cpus')).show()

    ##  memory
    print("\nMemory:")
    describe_memory = category.describe('resource_request_memory').show()
    variance_memory = category.select(F.variance('resource_request_memory')).show()
    variance2_memory = category.agg({'resource_request_memory': 'variance'}).show()
    
    print("\n\n")

reqRecursos(free_tier_instance, "free_tier")
reqRecursos(best_effort_instance, "best_effort")
reqRecursos(mid_tier_instance, "mid_tier")
reqRecursos(monitoring_tier_instance, "monitoring_tier")
reqRecursos(production_tier_instance, "production_tier")


# frequência de submissão de cada categoria


def submit_category(category, category_name):
    jobs_submetidos = category.filter(category.type == 0)

    jobs_submetidos_count = jobs_submetidos.count()

    last_job_submit = category.groupby().max('time').collect()[0]['max(time)']
    first_job_submit = category.groupby().min('time').collect()[0]['min(time)']

    time_interval_tier = (last_job_submit - first_job_submit)

    average_jobs_submited = jobs_submetidos_count / (time_interval_tier / 3600)

    print(f'Category: {category_name}')
    print(f'Average jobs submited per hour: {average_jobs_submited}')
    print("\n")


submit_category(free_tier, "free_tier")
submit_category(best_effort, "best_effort")
submit_category(mid_tier, "mid_tier")
submit_category(monitoring_tier, "monitoring_tier")
submit_category(production_tier, "production_tier")
