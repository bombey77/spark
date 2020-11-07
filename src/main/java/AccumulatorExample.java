import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;

public class AccumulatorExample {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkApp");
    JavaSparkContext context = new JavaSparkContext(conf);

    LongAccumulator accumulator = context.sc().longAccumulator();
    context.parallelize(Arrays.asList(1, 2, 3, 4))
            .map(x -> {
                      accumulator.add(x);
                      return x * 2;
                    }
            ).collect();
    System.out.println(accumulator.value());//output -> 10
  }
}
