package csv;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadFromCSV {

  public static void main(String[] args) {
    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);

    SparkSession spark = SparkSession
            .builder()
            .appName("Read From CSV")
            .master("local[1]")
            .getOrCreate();

    Dataset<Row> ds = spark
            .read()
            .format("csv")
            .option("header", true)//устанавливаю, что есть header в файле
            .option("multiLine", false)
            .option("encoding", "utf-8")
            .option("sep", ";")//устанавливаю разделитель ';'
            .option("quote", "^")//устанавливаю, что кавычки - это знак каретки и они будут игнорироваться при выводе (до ^6 straight^, после 6 straight)
            .option("dateFormat", "M/d/y")
            .option("inferSchema", true)//колонки будут отображаться не как стринг, а отображать тип который они содержат (пример: integer)
            .load("/media/roman/4C5A0611FB135B42/Project/spark/import/cars.csv");
//    ds.show(2, 10); выведет только первые 2 строчки и обрежет данные в колонках, где длина свыше 10 символов
    ds.show();
  }
}
