import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

public class Task implements Serializable {

  public static void main(String[] args) throws InterruptedException {
    SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkTest");
    JavaSparkContext context = new JavaSparkContext(conf);

    JavaRDD file = context.textFile("/media/roman/4C5A0611FB135B42/Project/spark/test.txt");
    Thread.sleep(777777777777777777L);
    System.out.println(file.collect());
  }
}
