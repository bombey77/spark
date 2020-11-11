package csv;

import model.Person;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

public class MappedWithFile {

  public static void main(String[] args) {
    SparkSession spark = SparkSession
            .builder()
            .master("local[1]")
            .appName("Mapped Class With File")
            .getOrCreate();

    JavaRDD<Person> rdd = spark.read()
            .textFile("/media/roman/4C5A0611FB135B42/Project/spark/import/users.csv")
            .javaRDD()
            .map(line -> {
              String[] parts = line.split(",");
              Person person = new Person();
              person.setName(parts[0].trim());
              person.setAge(Integer.parseInt(parts[1].trim()));
              return person;
            });
    System.out.println(rdd.collect());
//  [Person{name='Roman', age=34}, Person{name='Kathy', age=29}]
  }
}
