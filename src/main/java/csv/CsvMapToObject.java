package csv;

import model.Person;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CsvMapToObject {

  public static void main(String[] args) {
    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);

    SparkSession spark = SparkSession
            .builder()
            .master("local")
            .appName("Read From CSV And Map To User Class")
            .getOrCreate();

    Dataset<Row> df = spark.read().format("csv")
            .option("header", true)
            .option("inferSchema", true)
            .option("sep", ",")
            .load("/media/roman/4C5A0611FB135B42/Project/spark/import/users_with_header.csv")
            .toDF();

    Dataset<Person> ds = df.map((MapFunction<Row, Person>) row -> {
      Person person = new Person();
      person.setName(row.getAs("name"));
      person.setAge(row.getAs("age"));
      return person;
    }, Encoders.bean(Person.class));

    ds.show();
    ds.printSchema();
  }
}

//  +---+-----+
//  |age| name|
//  +---+-----+
//  | 34|Roman|
//  | 29|Kathy|
//  | 32|Petro|
//  +---+-----+
//
//  root
//  |-- age: integer (nullable = true)
//  |-- name: string (nullable = true)
