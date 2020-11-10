package data_frame_data_set_convert;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class DataFrameDataSetConvert {

//  Dataset<String> -> это Data Set (типо безопасный, но большие потери в скорости)
//  Dataset<Row> -> это Data Frame (не типо безопасный, но преимущество в скорости работы)

  public static void main(String[] args) {
    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);

    SparkSession spark = SparkSession
            .builder()
            .master("local")
            .appName("Data Frame Data Set Convert")
            .getOrCreate();

    List<String> list = Arrays.asList("banana", "lemon", "apple", "cherry", "strawberry");
    Dataset<String> ds = spark.createDataset(list, Encoders.STRING());

//  конвертации Data Set -> Data Frame
    Dataset<Row> df = ds.toDF();

//  конвертации Data Frame -> Data Set
    Dataset<String> convertedDS = df.as(Encoders.STRING());
  }
}
