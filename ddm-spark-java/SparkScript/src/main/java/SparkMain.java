import org.apache.spark.SparkConf;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class SparkMain {
    private static Integer numberOfColumns = 0;

    public void matrix_and(ArrayList<ArrayList<Boolean>> x, ArrayList<ArrayList<Boolean>> y){
        //Java passes non-basic data by reference rather than value compared to python, so working on x here is better.
        for(int i=0; i< x.size(); i++){
            for(int j=0; j < x.get(i).size(); j++){
                Boolean x_value = x.get(i).get(j);
                x.get(i).set(j, x_value && y.get(i).get(j));
            }
        }
    }

    public int sum_func(int accum, int n){
        return accum + n;
    }

    public static void main(String[] args) {
        String path = "TPCH";
        String cores = "2";
        SparkConf sc = new SparkConf().setMaster("local").setAppName("DDM");
        sc.set("spark.driver.memory", "15g");
        for(int i=0; i<args.length; i++){
            if(args[i].equals("--path")){
                path = args[i+1];
            }
            if(args[i].equals("--cores")){
                cores = args[i+1];
            }
        }
        sc.set("spark.deploy.defaultCores", cores);
        JavaSparkContext sparkContext = new JavaSparkContext(sc);
        ArrayList<Tuple2<Integer, String[]>> data = new ArrayList<Tuple2<Integer, String[]>>();
        ArrayList<String> fileHeaders = new ArrayList<String>();
        File[] files = new File(path).listFiles();
        for(File file : files) {
            if (file.getPath().endsWith(".csv")) {
                JavaRDD<Object> rdd = sparkContext.textFile(file.getPath()).map(new Function<String, Object>() {
                    public Object call(String line) throws Exception {
                        return line.substring(1, line.length() - 1).split("\";\"");
                    }
                });

                List<Object> file_data = Arrays.asList(rdd.collect().toArray());

                List<String> file_header = Arrays.asList((String[]) file_data.get(0));
                ArrayList<String[]> file_data_tmp = new ArrayList<String[]>();
                for(int i=1; i<file_data.size(); i++){
                    file_data_tmp.add((String[]) file_data.get(i));
                }
                ArrayList<Tuple2<Integer, String[]>> file_data_tuple = new ArrayList<Tuple2<Integer, String[]>>();
                for(int i=0; i<file_data_tmp.size(); i++){
                    String[] content = file_data_tmp.get(i);
                    file_data_tuple.add(new Tuple2<Integer, String[]>(numberOfColumns, content));
                }
                data.addAll(file_data_tuple);
                fileHeaders.addAll(file_header);
                numberOfColumns += file_header.size();
                System.out.println("Hello");
            }
        }
        JavaRDD<Tuple2<Integer, String[]>> full_rdds = sparkContext.parallelize(data);
    }
}
