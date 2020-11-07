import model.Person;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

public class MappedWithJavaRDD {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf()
            .setMaster("local[1]")
            .setAppName("Mapped With Java RDD");
    JavaSparkContext sc = new JavaSparkContext(conf);

    List<Person> list = new ArrayList<>();
    list.add(new Person("Roman", 34));
    list.add(new Person("Kathy", 29));

    JavaRDD<Person> rdd = sc.parallelize(list);
    SparkSession session = SparkSession
            .builder()
            .master("local[1]")
            .appName("Mapped With Java RDD")
            .getOrCreate();
    Dataset<Row> dataset = session.createDataFrame(rdd, Person.class);
    dataset.show();
  }
}
