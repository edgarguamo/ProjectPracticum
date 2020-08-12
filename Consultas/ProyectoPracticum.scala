// Databricks notebook source
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// COMMAND ----------

import org.apache.spark.sql.types._
val myDataSchema = StructType(
    Array(
        StructField("id",DecimalType(26,0), true),
        StructField("anio", IntegerType, true),
        StructField("mes", IntegerType, true),
        StructField("provincia", IntegerType,true),
        StructField("canton", IntegerType, true),
        StructField("area", StringType, true),
        StructField("genero", StringType, true),
        StructField("edad", IntegerType, true),
        StructField("estado_civil", StringType, true),
        StructField("nivel_de_instruccion", StringType, true),
        StructField("etnia", StringType, true),
        StructField("ingreso_laboral", IntegerType, true),
        StructField("condicion_actividad", StringType, true),
        StructField("sectorizacion", StringType, true),
        StructField("grupo_ocupacion", StringType, true),
        StructField("rama_actividad", StringType, true),
        StructField("factor_expansion",DoubleType,true)
        )
    );

// COMMAND ----------

val data = spark
.read
.schema(myDataSchema)
.option("header", "true")
.option("delimiter","\t")
.csv("/FileStore/tables/Datos_ENEMDU_PEA_v2.csv")

// COMMAND ----------

val provincesSchema = "idProv INT, ProvinciaS STRING"
val cantonesSchema = "idCanton INT, Cantones STRING"

// COMMAND ----------

val dataProvincias = spark
.read
.schema(provincesSchema)
.option("charset", "ISO-8859-1")
.option("header", "true")
.csv("/FileStore/tables/Provincias.csv")

// COMMAND ----------

val dataCantones = spark
.read
.schema(cantonesSchema)
.option("charset", "ISO-8859-1")
.option("header", "true")
.csv("/FileStore/tables/Cantones_1_.csv")

// COMMAND ----------

val aux = data.join(dataProvincias, data("provincia")=== dataProvincias("idProv"), "inner").join(dataCantones, data("canton") === dataCantones("idCanton"), "inner").drop("provincia", "idProv", "idCanton", "canton")
val dataD = aux.select("id","anio","mes","Provincias","Cantones","area","genero", "edad","estado_civil","nivel_de_instruccion","etnia","ingreso_laboral","condicion_actividad","sectorizacion","grupo_ocupacion", "rama_actividad","factor_expansion")
dataD.show

// COMMAND ----------

// DBTITLE 1,Promedio de ingreso_laboral por cada provincia 


// COMMAND ----------

// DBTITLE 1,Sumatoria total de ingreso laboral por cada provincia 


// COMMAND ----------

// DBTITLE 1,Distribución de etnias 


// COMMAND ----------

// DBTITLE 1,Etnia que genear mas ingresos a travez de los anio 
display(dataD.groupBy("etnia").pivot("anio").sum("ingreso_laboral"))

// COMMAND ----------

// DBTITLE 1,La mayor etnia distribuida por nivel_de_instruccion
display(dataD.groupBy("nivel_de_instruccion").pivot("etnia").count())

// COMMAND ----------

// DBTITLE 1,Etnia distribuida en  genero en función de los anios 
display(dataD.groupBy("genero", "anio").pivot("etnia").count().sort($"anio"))

// COMMAND ----------

// DBTITLE 1,estado civil distribuido por genero 


// COMMAND ----------

// DBTITLE 1,estado civil distrivuido por provincias 


// COMMAND ----------

// DBTITLE 1,Nivel de ingresos por estado civil en la provincia de loja 


// COMMAND ----------


