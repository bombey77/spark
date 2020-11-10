package unionCSVandJSON;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

public class UnionCSVandJSON {

  public static void main(String[] args) {
    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);

    SparkSession spark = SparkSession
            .builder()
            .master("local")
            .appName("Combine CSV and JSON files")
            .getOrCreate();

    Dataset<Row> jsonDF = getJsonDF(spark);
    Dataset<Row> csvDF = getCsvDF(spark);

//    jsonDF.show(10, 80);
//    jsonDF.printSchema();

//    csvDF.show(10, 80);
//    csvDF.printSchema();

//    данные из jsonDF будут находится сверху csvDF
//    ВНИМАНИЕ!!! printSchema метод должен выдавать одинаковые параметры для обеих таблиц
    Dataset<Row> df = jsonDF.unionByName(csvDF);
    df.show(10);
  }

  private static Dataset<Row> getJsonDF(final SparkSession spark) {
    Dataset<Row> df = spark.read()
            .format("json")
            .option("multiline", true)
            .load("/media/roman/4C5A0611FB135B42/Project/spark/import/parks.json");

    df = df.withColumn("id", lit("UNKNOWN"))
            .withColumn("address", lit("UNKNOWN"))
            .withColumn("park_role",
                    concat(df.col("name"), lit("_"), df.col("role.name")
                            .getItem(0)))//получаю первый элемент массива
            .withColumn("country", lit("USA"))//колонка отсутствует и я делаю хард-код для нее
            .withColumnRenamed("name", "park_name")//  смена имени колонки
            .drop(df.col("role"));
    return df;
  }

  private static Dataset<Row> getCsvDF(final SparkSession spark) {
    Dataset<Row> df = spark.read()
            .format("csv")
            .option("header", true)
            .load("/media/roman/4C5A0611FB135B42/Project/spark/import/parks.csv");

//  смена имени колонки
    df = df.withColumnRenamed("ASSET_NAME", "park_name");
//  первый способ -> фильтрация по колонке 'park_name', где в значении есть слово park
//    df = df.filter(lower(col("park_name")).like("%park%"));
//  второй способ -> фильтрация по колонке 'park_name', где в значении есть слово park
    df = df.filter("lower(park_name) like '%park%'")//обрати внимание на синтаксис
            .withColumnRenamed("OBJECTID", "id")
            .withColumnRenamed("ADDRESS", "address")
            .withColumn("park_role", lit("UNKNOWN"))
            .withColumn("age", lit("UNKNOWN"))
            .withColumn("country", lit("USA"))
            .drop(df.col("SITE_NAME"))//первый способ -> можно удалять колонку таким способом
            .drop("CHILD_OF")//второй способ -> можно удалять колонку таким способом
            .drop("TYPE")
            .drop("DESCRIPTION")
            .drop("SQ_FEET")
            .drop("ACREAGE")
            .drop("ZIPCODE")
            .drop("ALLIAS")
            .drop("NOTES")
            .drop("TENANT")
            .drop("LABEL")
            .drop("DPP_ASSET_ID")
            .drop("PPR_USE")
            .drop("Shape__Area")
            .drop("Shape__Length");
    return df;
  }
}
// Вид таблиц после операции UNION
//+-------+--------------------+-------+------------------+--------------------+-------+
//|    age|           park_name|     id|           address|           park_role|country|
//+-------+--------------------+-------+------------------+--------------------+-------+
//|    143|          Nixon Park|UNKNOWN|           UNKNOWN|  Nixon Park_central|    USA|
//|     87|         Boston Park|UNKNOWN|           UNKNOWN|  Boston Park_winter|    USA|
//|    122|New York Central ...|UNKNOWN|           UNKNOWN|New York Central ...|    USA|
//|UNKNOWN|Schuylkill River ...|      3|    400 S TANEY ST|             UNKNOWN|    USA|
//|UNKNOWN|Wissahickon Valle...|      7|              null|             UNKNOWN|    USA|
//|UNKNOWN| West Fairmount Park|      8|                  |             UNKNOWN|    USA|
//|UNKNOWN|   82nd & Lyons Park|      9|    8200 LYONS AVE|             UNKNOWN|    USA|
//|UNKNOWN|Jose Manuel Colla...|     11|   3261 MASCHER ST|             UNKNOWN|    USA|
//|UNKNOWN|60th & Baltimore ...|     14|5961 BALTIMORE AVE|             UNKNOWN|    USA|
//|UNKNOWN|47th & Grays Ferr...|     17| 4700 PASCHALL AVE|             UNKNOWN|    USA|
//+-------+--------------------+-------+------------------+--------------------+-------+

// Вид таблиц до операции UNION
//+---+---------------------+-------+-------+-------------------------+-------+
//|age|            park_name|     id|address|                park_role|country|
//+---+---------------------+-------+-------+-------------------------+-------+
//|143|           Nixon Park|UNKNOWN|UNKNOWN|       Nixon Park_central|    USA|
//| 87|          Boston Park|UNKNOWN|UNKNOWN|       Boston Park_winter|    USA|
//|122|New York Central Park|UNKNOWN|UNKNOWN|New York Central Park_old|    USA|
//+---+---------------------+-------+-------+-------------------------+-------+
//
//+---+------------------------+------------------+---------+-------+-------+
//| id|               park_name|           address|park_role|    age|country|
//+---+------------------------+------------------+---------+-------+-------+
//|  3|   Schuylkill River Park|    400 S TANEY ST|  UNKNOWN|UNKNOWN|    USA|
//|  7| Wissahickon Valley Park|              null|  UNKNOWN|UNKNOWN|    USA|
//|  8|     West Fairmount Park|                  |  UNKNOWN|UNKNOWN|    USA|
//|  9|       82nd & Lyons Park|    8200 LYONS AVE|  UNKNOWN|UNKNOWN|    USA|
//| 11|Jose Manuel Collazo Park|   3261 MASCHER ST|  UNKNOWN|UNKNOWN|    USA|
//| 14|   60th & Baltimore Park|5961 BALTIMORE AVE|  UNKNOWN|UNKNOWN|    USA|
//| 17| 47th & Grays Ferry Park| 4700 PASCHALL AVE|  UNKNOWN|UNKNOWN|    USA|
//| 21|          Pennypack Park|                  |  UNKNOWN|UNKNOWN|    USA|
//| 22|     East Fairmount Park|                  |  UNKNOWN|UNKNOWN|    USA|
//| 23|       Tacony Creek Park|                  |  UNKNOWN|UNKNOWN|    USA|
//+---+------------------------+------------------+---------+-------+-------+
