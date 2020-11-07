import model.Person;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

public class RemoveLogInfo {
// 1). Add the following imports to the top of the class that contains your main method.
//import org.apache.log4j.Logger;
//import org.apache.log4j.Level;

//2). After that, inside your main method, add the following 2 lines.
//Logger.getLogger("org").setLevel(Level.ERROR);
//Logger.getLogger("akka").setLevel(Level.ERROR);

//  This should now only show the error messages, and your console prints and ignore all of those INFO messages.

  public static void main(String[] args) {
    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);

    List<Person> list = new ArrayList<>();
    list.add(new Person("Roman", 34));
    list.add(new Person("Kathy", 29));

    SparkSession session = SparkSession
            .builder()
            .master("local[1]")
            .appName("Remove Log Info")
            .getOrCreate();
    Dataset<Row> dataset = session.createDataFrame(list, Person.class);
    dataset.show();
  }
}
