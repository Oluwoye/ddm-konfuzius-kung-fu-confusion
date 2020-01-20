import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import java.util.ArrayList;

public class SparkMain {
    private int numberOfColumns = 0;

    public static void main(String[] args) {
        SparkConf sc = new SparkConf().setMaster("local").setAppName("DDM");
        sc.set("spark.driver.memory", "15g");
        System.out.println("hello");
        
    }
}
