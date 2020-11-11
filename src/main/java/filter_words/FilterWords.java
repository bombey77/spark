package filter_words;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class FilterWords {

  public static void main(String[] args) {
    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);

    SparkSession spark = SparkSession
            .builder()
            .master("local")
            .appName("Read From Txt File And Filter Words")
            .getOrCreate();

    Dataset<Row> df = spark.read().format("text")
            .load("/media/roman/4C5A0611FB135B42/Project/spark/import/sometext.txt");

    Dataset<String> ds = df.flatMap((FlatMapFunction<Row, String>) func -> {
      return Arrays.stream(func.toString().split(" ")).iterator();
    }, Encoders.STRING());

//    фильтрует только на определенные слова указанные в условиях
//    ds = ds.filter((FilterFunction<String>) word -> {
//      return !word.equals("not") && !word.equals("looking");
//    });

//    фильтрует только на слова указанные в filterWords
    String filterWords = " ('not', 'looking', 'for', 'words')";

    df = ds.toDF();
    df = df.groupBy("value")
            .count()
            .filter("lower(value) NOT IN " + filterWords);
    df.show();
  }
}
