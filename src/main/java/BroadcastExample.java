import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class BroadcastExample {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkApp");
    JavaSparkContext context = new JavaSparkContext(conf);

    JavaRDD<String> file = context.textFile("/media/roman/4C5A0611FB135B42/Project/spark/test.txt");
//  Позволяет шарить переменную между кластерами
    Broadcast<JavaRDD<String>> broadcast = context.broadcast(file);
    System.out.println(broadcast.value());
  }
}
