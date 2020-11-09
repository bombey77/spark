package csv;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DefineCSVSchema {

  public static void main(String[] args) {
    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);

    SparkSession spark = SparkSession
            .builder()
            .appName("Read From CSV")
            .master("local[1]")
            .getOrCreate();

    StructType schema = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("id", DataTypes.IntegerType, false),
            DataTypes.createStructField("brand", DataTypes.StringType, false),
            DataTypes.createStructField("model", DataTypes.StringType, false),
            DataTypes.createStructField("engine", DataTypes.StringType, false),
            DataTypes.createStructField("doors", DataTypes.IntegerType, false),
            DataTypes.createStructField("color", DataTypes.StringType, false),
            DataTypes.createStructField("date", DataTypes.DateType, false),
            DataTypes.createStructField("description", DataTypes.StringType, false)
    });

    Dataset<Row> df = spark
            .read()
            .format("csv")
            .option("header", true)//устанавливаю, что есть header в файле
            .option("multiLine", false)
            .option("encoding", "utf-8")
            .option("sep", ";")//устанавливаю разделитель ';'
            .option("quote", "^")//устанавливаю, что кавычки - это знак каретки и они будут игнорироваться при выводе (до ^6 straight^, после 6 straight)
            .option("dateFormat", "MM/dd/yy")
            .schema(schema)
            .load("/media/roman/4C5A0611FB135B42/Project/spark/import/cars.csv");
//    df.show(2, 10); выведет только первые 2 строчки и обрежет данные в колонках, где длина свыше 10 символов
    df.show();
    df.printSchema();
  }

//  +---+--------+------+----------+-----+-----+----------+--------------------+
//  | id|   brand| model|    engine|doors|color|      date|         description|
//  +---+--------+------+----------+-----+-----+----------+--------------------+
//  |  1|Mercedes|  W124|6 straight|    5|black|2020-12-31|   the best car ever|
//  |  2|     BMW|    M5|      W 12|    3|  red|2008-10-28|I would like to h...|
//  |  3|   Volvo|Tigger|4 straight|    5|white|2012-02-02| do not buy this car|
//  +---+--------+------+----------+-----+-----+----------+--------------------+
//
//  root
// |-- id: integer (nullable = true)
// |-- brand: string (nullable = true)
// |-- model: string (nullable = true)
// |-- engine: string (nullable = true)
// |-- doors: integer (nullable = true)
// |-- color: string (nullable = true)
// |-- date: date (nullable = true)
// |-- description: string (nullable = true)
}
