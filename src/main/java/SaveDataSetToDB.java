import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

public class SaveDataSetToDB {

  public static void main(String[] args) {
    SparkSession spark = SparkSession
            .builder()
            .master("local[1]")
            .appName("Save DataSet To DB")
            .getOrCreate();

    Dataset<Row> df = spark.read().format("csv")
            .option("header", true)//устанавливаю, что есть header в файле
            .load("/media/roman/4C5A0611FB135B42/Project/spark/import/users_with_header.csv");
//    df.show();
//    создаю новую колонку и конкатинирую данные из двух колонок
    df = df.withColumn("full_info", concat(
            df.col("name"), lit(", "), df.col("age")))
//            .filter(df.col("name").rlike("Kathy"))  тут регулярное выражение
            .filter(df.col("age").lt(34))
            .orderBy("name");
    df.show();

    String url = "jdbc:mysql://localhost:3306/roman";
    Properties properties = new Properties();
    properties.setProperty("driver", "com.mysql.jdbc.Driver");
    properties.setProperty("user", "root");
    properties.setProperty("password", "test");

    df.write()
            .mode(SaveMode.Overwrite)
            //создаю таблицу updated_users и сохраняю ее в БД
            .jdbc(url, "updated_users", properties);

//    mysql> select * from updated_users;
//            +-------+------+-----------+
//            | name  | age  | full_info |
//            +-------+------+-----------+
//            | Kathy | 29   | Kathy, 29 |
//            | Petro | 32   | Petro, 32 |
//            +-------+------+-----------+
  }
}
