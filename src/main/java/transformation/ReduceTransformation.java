package transformation;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class ReduceTransformation {

  public static void main(String[] args) {
    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);

    SparkSession spark = SparkSession
            .builder()
            .master("local")
            .appName("Transformation (reduce)")
            .getOrCreate();

    List<String> list = Arrays.asList("banana", "lemon", "apple", "cherry", "strawberry");

//    Dataset<String> ds = spark.createDataset(list, Encoders.STRING());
    Dataset<String> ds = spark.createDataset(list, Encoders.STRING());
//  первый способ
    String reduce = ds.reduce((ReduceFunction<String>) (fruit1, fruit2) -> fruit1 + "-" + fruit2);
//    второй способ
//    String reduce = ds.reduce(new FruitReducer());
    System.out.println(reduce);
//    banana-lemon-apple-cherry-strawberry
  }

  private static class FruitReducer implements ReduceFunction<String>, Serializable {

    private static final long serialVersionUID = -646268219357565322L;

    @Override
    public String call(String fruit1, String fruit2) {
      return fruit1 + "-" + fruit2;
    }
  }
}

