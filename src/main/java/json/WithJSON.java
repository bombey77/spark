package json;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class WithJSON {

  public static void main(String[] args) {
    SparkSession spark = SparkSession
            .builder()
            .master("local[1]")
            .appName("Java Spark JSON example")
            .getOrCreate();
    Dataset<Row> df = spark
            .read()
            .json("/media/roman/4C5A0611FB135B42/Project/spark/import/import.json");
//    отображает JSON как таблицу
//    df.show();

//    отображает схему таблицы
//    df.printSchema();

//    фильтрует юзеров, где возраст свыше 30 лет
//    df.filter(df.col("age").gt(30)).show();
//    +---+-----+---------------+
//    |age| name|           role|
//    +---+-----+---------------+
//    | 34|Roman|[[admin, user]]|
//    | 45|Petro|      [[guest]]|
//    +---+-----+---------------+

//    групирует юзеров по колонке имя
//    df.groupBy(df.col("name")).count().show();
//    +-----+-----+
//    | name|count|
//    +-----+-----+
//    |Petro|    2|
//    |Kathy|    1|
//    |Roman|    1|
//    +-----+-----+

//    мапит JSON как таблицу people И работает как с БД
    df.createOrReplaceTempView("people");
    Dataset<Row> sql = spark.sql("select * from people");
    sql.show();

//    +---+-----+---------------+
//    |age| name|           role|
//    +---+-----+---------------+
//    | 34|Roman|[[admin, user]]|
//    | 29|Kathy|       [[user]]|
//    | 45|Petro|      [[guest]]|
//    | 28|Petro|       [[user]]|
//    +---+-----+---------------+
  }
}
