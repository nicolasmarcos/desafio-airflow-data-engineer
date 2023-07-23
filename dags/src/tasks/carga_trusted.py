# Importa bibliotecas necessárias
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, to_timestamp, sum, avg 
from pyspark.sql.functions import array, explode, struct, lit, col
from airflow.decorators import task
import os        

# Function utilizada para ajustar os dfs raw, pivotando as colunas de datas e as transformando em linhas
def ajusta_df(df, by, kname, vname):

    # Filtra dtypes e splita em nomes de coluna e definição de tipo
    cols, dtypes = zip(*((c, t) for (c, t) in df.dtypes if c not in by))
    # Spark SQL apenas suporta colunas de mesmo tipo
    assert len(set(dtypes)) == 1, "All columns have to be of the same type"

    # Cria e realiza explode em array por estrutura de (key name, key value)
    kvs = explode(array([
    struct(lit(c).alias(kname), col(c).alias(vname)) for c in cols
    ])).alias("kvs")

    return df.select(by + [kvs]).select(by + [f"kvs.{kname}", f"kvs.{vname}"])



# Function p/ carga da camada trusted, consumindo camada raw
@task(task_id="t_carga_trusted")
def carga_trusted(filepath_input, filepath_output):
    
    # Inicializa SparkSession
    spark = SparkSession.builder \
        .appName("Carga_Refined") \
        .getOrCreate()

    # Para os arquivos de input, geera os respectivos dfs
    path_confirmados = os.path.join(filepath_input,'time_series_covid19_confirmed_global.csv')
    df_confirmados = spark.read.csv(path_confirmados, header=True, inferSchema=True)
    path_mortes = os.path.join(filepath_input,'time_series_covid19_deaths_global.csv')
    df_mortes = spark.read.csv(path_mortes, header=True, inferSchema=True)
    path_recuperados = os.path.join(filepath_input,'time_series_covid19_recovered_global.csv')
    df_recuperados = spark.read.csv(path_recuperados, header=True, inferSchema=True)
    
    # Ajusta cada df transformando colunas de datas em linhas
    # Para que seja possível join mais a frente sem cartesiano, necessário preencher valores nulos
    # Tentou-se estratégias sem preenchimento de valores, como utilizando eqNullSafe com full join, sem sucesso
    df_confirmados_aj = ajusta_df(df_confirmados, \
                ["Province/State", "Country/Region", "Lat", "Long"], \
                "data_t", \
                "quantidade_confirmados") \
                .na.fill(value="-1",subset=["Province/State"])\
                .na.fill(value=-1,subset=["Lat", "Long"])
                    
    df_mortes_aj = ajusta_df(df_mortes, \
                    ["Province/State", "Country/Region", "Lat", "Long"], \
                    "data_t", \
                    "quantidade_mortes") \
                    .na.fill(value="-1",subset=["Province/State"])\
                    .na.fill(value=-1,subset=["Lat", "Long"])
    df_recuperados_aj = ajusta_df(df_recuperados, \
                    ["Province/State", "Country/Region", "Lat", "Long"], \
                    "data_t", \
                    "quantidade_recuperados") \
                    .na.fill(value="-1",subset=["Province/State"])\
                    .na.fill(value=-1,subset=["Lat", "Long"])
    
    # Unifica os dataframes
    merged_df = df_confirmados_aj.join(df_mortes_aj, \
                                ["Province/State", "Country/Region", "Lat", "Long", "data_t"], \
                                    how="full") \
                                .join(df_recuperados_aj, \
                                    ["Province/State", "Country/Region", "Lat", "Long", "data_t"], \
                                    how="full")
    
    # Gera df com colunas e estruturas desejadas
    merged_df = merged_df.select("*") \
    .withColumnRenamed("Country/Region","pais") \
    .withColumnRenamed("Province/State","estado") \
    .withColumnRenamed("Lat","latitude") \
    .withColumnRenamed("Long","longitude") \
    .withColumn("data", to_timestamp(col("data_t"),"M/d/yy")) \
    .drop("data_t") \
    .withColumn("ano",year(col("data"))) \
    .withColumn("mes",month(col("data")))

    # Gera df com ordem de colunas desejada e campos de qtd com tipagem desejada
    merged_df = merged_df.selectExpr("pais", \
                            "estado", \
                            "latitude", \
                            "longitude", \
                            "data", \
                            "cast(quantidade_confirmados as long) quantidade_confirmados", \
                            "cast(quantidade_mortes as long) quantidade_mortes", \
                            "cast(quantidade_recuperados as long) quantidade_recuperados", \
                            "ano",
                            "mes"
                            )
    
    # Salva o DataFrame resultante em parquet, gerando camada trusted
    merged_df.coalesce(1).write.partitionBy("ano","mes").parquet(filepath_output, mode="overwrite")


    # Encerra a sessão do Spark
    spark.stop()