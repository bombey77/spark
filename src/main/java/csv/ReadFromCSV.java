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

    Dataset<Row> df = spark
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
//    df.show(2, 10); выведет только первые 2 строчки и обрежет данные в колонках, где длина свыше 10 символов
    df.show();
    df.printSchema();
  }

//  +---+--------+------+----------+-----+-----+--------+--------------------+
//  | id|   brand| model|    engine|doors|color|    date|         description|
//  +---+--------+------+----------+-----+-----+--------+--------------------+
//  |  1|Mercedes|  W124|6 straight|    5|black|12/31/20|   the best car ever|
//  |  2|     BMW|    M5|      W 12|    3|  red|10/28/08|I would like to h...|
//  |  3|   Volvo|Tigger|4 straight|    5|white|02/02/12| do not buy this car|
//  +---+--------+------+----------+-----+-----+--------+--------------------+
//
//  root
// |-- id: integer (nullable = true)
// |-- brand: string (nullable = true)
// |-- model: string (nullable = true)
// |-- engine: string (nullable = true)
// |-- doors: integer (nullable = true)
// |-- color: string (nullable = true)
// |-- date: string (nullable = true)
// |-- description: string (nullable = true)
}
