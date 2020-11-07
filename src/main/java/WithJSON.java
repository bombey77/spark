import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class WithJSON {

  public static void main(String[] args) {
    SparkSession session = SparkSession
            .builder()
            .master("local[1]")
            .appName("Java Spark JSON example")
            .getOrCreate();
    Dataset<Row> json = session
            .read()
            .json("/media/roman/4C5A0611FB135B42/Project/spark/import/import.json");
//    отображает JSON как таблицу
//    json.show();

//    отображает схему таблицы
//    json.printSchema();

//    фильтрует юзеров, где возраст свыше 30 лет
//    json.filter(json.col("age").gt(30)).show();

//    групирует юзеров по колонке имя
//    json.groupBy(json.col("name")).count().show();

//    мапит JSON как таблицу people И работает как с БД
//    json.createOrReplaceTempView("people");
//    Dataset<Row> sql = session.sql("select * from people");
//    sql.show();
  }
}
