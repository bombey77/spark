import model.Person;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JDBC {
//  from console
//  docker exec -it <container_name> bash
//  mysql -uroot -ptest
//  create database roman;
//  use roman;
//  create table usr(id int not null auto_increment primary key, name varchar(255), age int);
//  insert into usr (name, age) values ("ROMAN", 34);
//  insert into usr (name, age) values ("KATHY", 29);

  public static void main(String[] args) {
    SparkSession spark = SparkSession
            .builder()
            .master("local[1]")
            .appName("JDBC connection")
            .getOrCreate();

    Dataset<Row> jdbcDF = spark.read()
            .format("jdbc")
            .option("driver", "com.mysql.jdbc.Driver")
            .option("url", "jdbc:mysql://localhost:3306/roman")
            .option("dbtable", "usr")
            .option("user", "root")
            .option("password", "test")
            .load();
//    jdbcDF.show();
//    jdbcDF.collectAsList().forEach(System.out::println);

    JavaRDD<Person> rdd =
            jdbcDF.javaRDD()
                    .map(line -> {
//                    line = [1,ROMAN,34]
                      String[] parts = line.toString().split(",");
                      Person person = new Person();
                      person.setName(parts[1].trim());
                      String cutLastSign = parts[2].substring(0, parts[2].length() - 1);
//                    line = 34] -> 34
                      person.setAge(Integer.parseInt(cutLastSign));
                      return person;
                    });
    System.out.println(rdd.collect());
//  [Person{name='ROMAN', age=34}, Person{name='KATHY', age=29}]
  }
}
