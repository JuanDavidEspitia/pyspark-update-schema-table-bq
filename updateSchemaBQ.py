from pyspark.sql.types import *
from google.cloud import storage
import pyspark
import json
from pyspark.sql import SparkSession

spark = SparkSession.builder \
  .appName('Jupyter BigQuery Storage')\
  .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar') \
  .getOrCreate()

# Funcion que se encarga de Capturar el JSON que esta en Cloud Storage
def jsonToDf(tableName):
  storage_client = storage.Client()
  bucket = storage_client.get_bucket('bdb-gcp-cds-st-tools-zone')
  blob = bucket.blob("SchemasJSON/{}.json".format(tableName))
  data = json.loads(blob.download_as_string(client=None))
  return data

# Funcion que se encarga de castear los tipos de datos  de cada columna
def castColumn(df, cn, tpe):
    df2 = df.withColumn(cn, df[cn].cast(tpe))
    return df2

# Funcion que chequea la existencia de las columnas en el SchemaJSON
def checkColumn(name, columns):
  name = name.lower()
  if name in columns:
    return True
  else:
    return False

# Funcion que itera cada campo del JSON y se lo envia como parametro a la funcion de casteo
def modifyingSchema(df, jsonData):
    dfTarget = df
    for i in range(0, len(jsonData['fields'])):

      name = jsonData['fields'][i]['name']
      tipo = jsonData['fields'][i]['type']
      nulleable = jsonData['fields'][i]['nullable']
      # print(tipo.lower())
      check = checkColumn(name, dfTarget.columns)
      # print(check)
      if check == True:
        dfTarget = castColumn(dfTarget, name.lower(), tipo.lower())

    return dfTarget

# Parametros y punto de partida
bq_dataset="maestro"
bq_table="ODS_HIS_ATAHORROS"
jsonData = jsonToDf(bq_table)

# Leemos la tabla que vamos a cambiar sus tipos de datos
df = spark.read \
  .format("bigquery") \
  .option("table","{}.{}".format(bq_dataset, bq_table))  \
  .load()
df.printSchema()

# Validamos que el
dfTarget= modifyingSchema(df, jsonData)
dfTarget.printSchema()

# Escribimos el DF ya con sus tipos de datos homologados en la table de BQ
gcs_bucket= "bdb-gcp-cds-st-staging-bucket-dataproc/python"
dfTarget.write \
  .format("bigquery") \
  .option("table","{}.{}".format(bq_dataset, bq_table)) \
  .option("temporaryGcsBucket", gcs_bucket) \
  .mode('overwrite') \
  .save()

