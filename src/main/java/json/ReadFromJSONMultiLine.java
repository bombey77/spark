package json;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadFromJSONMultiLine {

  public static void main(String[] args) {
    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);

    SparkSession spark = SparkSession
            .builder()
            .appName("Read From JSON")
            .master("local[1]")
            .getOrCreate();

    Dataset<Row> df = spark
            .read()
            .format("json")
            .option("multiline", true)
            .load("/media/roman/4C5A0611FB135B42/Project/spark/import/jsonWithMultiLine.json");
    df.show();
    df.printSchema();
  }

//  +---+-----+---------------+
//  |age| name|           role|
//  +---+-----+---------------+
//  | 34|Roman|[[admin, user]]|
//  | 29|Kathy|       [[user]]|
//  | 45|Petro|      [[guest]]|
//  | 28|Petro|       [[user]]|
//  +---+-----+---------------+
//
//  root
// |-- age: long (nullable = true)
// |-- name: string (nullable = true)
// |-- role: struct (nullable = true)
// |    |-- name: array (nullable = true)
// |    |    |-- element: string (containsNull = true)
}
