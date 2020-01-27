package app;

import org.apache.spark.SparkConf;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class SparkMain {
    private static Integer numberOfColumns = 0;

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
            }
        }


        JavaRDD<Tuple2<Integer, String[]>> full_rdds = sparkContext.parallelize(data);
        JavaPairRDD<String, BitSet> valuesAsKey = JavaPairRDD.fromJavaRDD(full_rdds.flatMap(x -> {
        	int len = x._2.length;
        	ArrayList<Tuple2<String, BitSet>> result = new ArrayList<>();
        	for (int i = 0; i < len; i++) {
        		BitSet j = new BitSet(numberOfColumns);
        		j.clear();
        		j.set(x._1 + i, true);
        		Tuple2<String, BitSet> r = new Tuple2<String, BitSet>(x._2[i], j);
        		result.add(r);
        	}
        	return result.iterator();
        }));

        JavaPairRDD<String, BitSet> v = valuesAsKey.reduceByKey((a, x) -> {
            a.or(x);
            return a;
        });

        JavaPairRDD<BitSet, Integer> w = JavaPairRDD.fromJavaRDD(JavaRDD.fromRDD(JavaPairRDD.toRDD(v), v.classTag()).flatMap(x -> {
        	Tuple2<BitSet, Integer> r = new Tuple2<BitSet, Integer>(x._2, 0);
        	ArrayList<Tuple2<BitSet, Integer>> s = new ArrayList<Tuple2<BitSet, Integer>>();
        	s.add(r);
        	return s.iterator();
        })).reduceByKey((a, b) -> 0);

        JavaRDD<BitSet> w2 = JavaRDD.fromRDD(JavaPairRDD.toRDD(w), w.classTag()).map(x -> x._1);
        
        JavaRDD<BitSet[]> matrixes = w2.map(includeX -> {
        	BitSet[] result = new BitSet[numberOfColumns];
        	for (int i = 0; i < numberOfColumns; i++) {
        		result[i] = new BitSet(numberOfColumns);
                result[i].set(0, numberOfColumns);
        		for (int j = 0; j < numberOfColumns; j++) {
        			if (includeX.get(i) && !includeX.get(j)) {
        				result[i].set(j, false);
        			}
        		}
        	}
        	return result;
        });
        
        BitSet[] resultMatrix = matrixes.reduce((a, b) -> {
            for (int i = 0; i < numberOfColumns; i++) {
                a[i].and(b[i]);
        	}
        	return a;
        });
        
        assert(resultMatrix.length == numberOfColumns);
        ArrayList<String> output = new ArrayList<>();
        for (int i=0; i<numberOfColumns; i++){
            output.add("");
        }
    	for (int i = 0; i < numberOfColumns; i++) {
    		for (int j = 0; j < numberOfColumns; j++) {
    			if (i != j && resultMatrix[i].get(j)) {
    				if (output.get(j).length() > 0) {
    					output.set(j, output.get(j) + ", ");
    				}
    				output.set(j, output.get(j) + fileHeaders.get(i));
    			}
    		}
    	}
    	for (int i = 0; i < numberOfColumns; i++) {
    		if (output.get(i).length() > 0) {
    			System.out.println(fileHeaders.get(i) + " < " + output.get(i));
    		}
    	}

    	sparkContext.stop();
    }
}
