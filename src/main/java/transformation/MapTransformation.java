package transformation;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class MapTransformation {

  public static void main(String[] args) {
    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);

    SparkSession spark = SparkSession
            .builder()
            .master("local")
            .appName("Transformation (map)")
            .getOrCreate();

    List<String> list = Arrays.asList("banana", "lemon", "apple", "cherry", "strawberry");

//    Dataset<String> ds = spark.createDataset(list, Encoders.STRING());
    Dataset<String> ds = spark.createDataset(list, Encoders.STRING());
//  первый способ
    ds = ds.map((MapFunction<String, String>) fruit -> "name: " + fruit, Encoders.STRING());
//    второй способ
//    ds = ds.map(new FruitMapper(), Encoders.STRING());
    ds.show();

//    +----------------+
//    |           value|
//    +----------------+
//    |    name: banana|
//    |     name: lemon|
//    |     name: apple|
//    |    name: cherry|
//    |name: strawberry|
//    +----------------+
  }

  private static class FruitMapper implements MapFunction<String, String>, Serializable {

    @Override
    public String call(String value) throws Exception {
      return "name: " + value;
    }
  }
}

