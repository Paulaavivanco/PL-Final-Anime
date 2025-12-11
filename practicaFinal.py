from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import requests
import json

from pyspark.sql.types import *

# crear sesion spark
spark = SparkSession.builder.appName("Practica Final").getOrCreate()

# cargar los datos con indicaciones para funcionar correctamente
# header --> nombres columnas, inferSchema --> detectar tipo de columna, multiline --> manejo de titulos con comas, encoding --> codificacion caracteres, escape --> manejo comillas
animeCSV = spark.read.csv("anime.csv", header=True, inferSchema=True, multiLine=True, encoding="UTF-8",escape='"')

ratingCSV = spark.read.csv("rating_complete.csv", header=True, inferSchema=True, encoding="UTF-8",)

valoracionesCSV= spark.read.csv('valoraciones_EP.csv',header=False, inferSchema=True, encoding="UTF-8").toDF("user_id", "anime_id", "rating")

# Visualizar primeras filas de csv anime
animeCSV.printSchema()
animeCSV.show(10)

# Visualizar primeras filas de csv rating
ratingCSV.printSchema()
ratingCSV.show(10)

# Visualizar primeras filas de csv valoraciones EP
valoracionesCSV.printSchema()
valoracionesCSV.show(10)

# lista para almacenar las columnas que se quieren eliminar
columnas_eliminar = [
    "Japanese name",
    "Aired",
    "Premiered",
    "Episodes",
    "Duration",
    "Rating",
    "Source",
    "Popularity",
    "Favorites",
    "Watching",
    "On-Hold",
    "Plan to Watch",
    "Producers",
    "Licensors",
    "Ranked"
]

#lista para almacenar las columnas de score de 1 a 10 (score-1) ya que van a ser eliminadas
columnas_score = []
for columna in animeCSV.columns:
    if "score-" in columna.lower():
        columnas_score.append(columna)

# juntamos todas las listas que se quieren eliminar
columnasEliminar = columnas_eliminar + columnas_score

# eliminar columnas del csv
animeCSV = animeCSV.drop(*columnasEliminar)

# mostrar dataset para ver si se han eliminado correctamente
animeCSV.printSchema()
animeCSV.show(10)

# cambiar nombres con espacios (normalizar)
animeCSV = animeCSV.withColumnRenamed("English name", "English_name")

# print para indica cambio de tipo
print("Cambiar tipo de la columna Score (de String a Float)")

# conversion de tipos
animeCSV = animeCSV.replace("Unknown", None, subset=["Score"])
animeCSV = animeCSV.withColumn("Score", col("Score").cast("float"))
animeCSV.show(10)

# limpiar valores nulos (solo se realiza en la columna Score)
media_score = animeCSV.select(avg("Score")).first()[0]
print("media:", media_score)
animeCSV = animeCSV.fillna({"Score": media_score})

# En rating CSV hay que cambiar que rating sea double como en valoraciones
ratingCSV = ratingCSV.withColumn("rating", col("rating").cast(DoubleType()))
# Unir rating con las valoraciones del usuario
ratingsTotal = ratingCSV.unionByName(valoracionesCSV)

# modificar columna de generos para separación y tipos para tener todo en minusculas
animeCSV = animeCSV.withColumn("Genres", split(col("Genres"), ", "))
animeCSV = animeCSV.withColumn("Type", lower(col("Type")))

# Preparar dataset para ALS, cambio de nombres de columnas user_id y anime_id
ratingsALS = ratingsTotal.withColumn("rating", col("rating").cast("double")).withColumnRenamed("user_id", "userId").withColumnRenamed("anime_id", "itemId")
print("Info")
animeCSV.show(10)
ratingsALS.show(10)

# comprobacion union ratings y valoraciones
ratingsEP = ratingsALS.filter(col("userId") == "666666")
ratingsEP.show(2)

# crear columna para valoracion media por item
columnaValoracionMedia = ratingsALS.groupby("itemId").agg(avg("rating").alias("valoracion_media"))
animeCSV = animeCSV.join(columnaValoracionMedia, animeCSV.ID == columnaValoracionMedia.itemId, how="left").drop("itemId")
animeCSV.show(10)