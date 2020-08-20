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

display(dataD.groupBy("Provincias").agg(avg("ingreso_laboral")))


// COMMAND ----------

// DBTITLE 1,Sumatoria total de ingreso laboral por cada provincia 
display(dataD.groupBy("Provincias").sum("ingreso_laboral"))

// COMMAND ----------

// DBTITLE 1,Distribución de etnias 
val pIndigena = (dataD.where($"etnia" === "1 - Indígena").count / dataD.count.toDouble)*100
val pAfroecuatoriano = (dataD.where($"etnia" === "2 - Afroecuatoriano").count / dataD.count.toDouble)*100
val pNegro = (dataD.where($"etnia" === "3 - Negro").count / dataD.count.toDouble)*100
val pMulato = (dataD.where($"etnia" === "4 - Mulato").count / dataD.count.toDouble)*100
val pMontubio = (dataD.where($"etnia" === "5 - Montubio").count / dataD.count.toDouble)*100
val pMestizo = (dataD.where($"etnia" === "6 - Mestizo").count / dataD.count.toDouble)*100
val pBlanco = (dataD.where($"etnia" === "7 - Blanco").count / dataD.count.toDouble)*100
val pOtro = (dataD.where($"etnia" === "8 - Otro").count / dataD.count.toDouble)*100

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
display(data.groupBy("genero").pivot("estado_civil").count)

// COMMAND ----------

// DBTITLE 1,estado civil distrivuido por provincias 
display(data.groupBy("provincia").pivot("estado_civil").count.sort(asc("provincia")))

// COMMAND ----------

// DBTITLE 1,Nivel de ingresos por estado civil en la provincia de loja 
display(data.filter("provincia = 11").groupBy("estado_civil").avg("ingreso_laboral").sort(desc("avg(ingreso_laboral)")))

// COMMAND ----------


