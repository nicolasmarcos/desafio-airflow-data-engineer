# Importa bibliotecas necessárias
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg 
from pyspark.sql.window import Window
from airflow.decorators import task

# Function que gera carga da camada refined
@task(task_id="t_carga_refined", trigger_rule='all_success')
def carga_refined(filepath_input, filepath_output):
    
    # Inicializa SparkSession
    spark = SparkSession \
    .builder \
    .appName("Carga_Trusted") \
    .getOrCreate()
    

    # Lê arquivos da camada refined 
    df = spark.read.option("mergeSchema", "true").parquet(filepath_input)

    # Gera df agregado por país, data e ano
    df_trusted = df.groupBy(col("pais"), \
            col("data"), \
            col("ano")) \
    .agg(sum(col("quantidade_confirmados")).alias("soma_confirmados"), \
         sum(col("quantidade_mortes")).alias("soma_mortes"), \
         sum(col("quantidade_recuperados")).alias("soma_recuperados") )
    
    #Cria window funtion para realizar média móvel por data, particionado por país com x dias de média móvel
    dias_media_movel = -7
    w = (Window.orderBy(col("data")).partitionBy(col("pais")).rowsBetween(dias_media_movel, 0))

    # Gera DF com colunas desejadas
    df_trusted = df_trusted.withColumn("media_movel_confirmados",avg(col("soma_confirmados")).over(w)) \
        .withColumn("media_movel_mortes",avg(col("soma_mortes")).over(w)) \
        .withColumn("media_movel_recuperados",avg(col("soma_recuperados")).over(w)) \
        .selectExpr("pais", "data", \
                    "cast(round(media_movel_confirmados) as long) media_movel_confirmados", \
                    "cast(round(media_movel_mortes) as long) media_movel_mortes", \
                    "cast(round(media_movel_recuperados) as long) media_movel_recuperados", \
                    "ano")

    # Gera parquet final
    df_trusted.coalesce(1).write.partitionBy("ano").parquet(filepath_output, mode="overwrite")
    
    # Encerra a sessão do Spark
    spark.stop()