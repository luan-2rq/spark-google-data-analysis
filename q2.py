import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.appName('Q2').getOrCreate()

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

# As diversas categorias de jobs possuem características diferentes (requisição de recursos computacionais, frequência de submissão, etc.)?

