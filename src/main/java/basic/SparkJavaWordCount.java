package basic;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;

public final class SparkJavaWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

        // Take input file parameter from the command line arguments
        // Kill the application if no input_file or output_file parameters are given
        if (args.length < 2) {
            System.err.println("Usage: JavaWordCount <input_file> <output_file>");
            System.exit(1);
        }
        // Create a Spark Session (the new entry point that supersedes spark context)
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaWordCount")
                .getOrCreate();

        // Create a RDD by reading the input file
        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
        // Using Java 8 lambda functions
        // flatten the file structure and map over it
        // Split files by space create an RDD of strings from it
        JavaRDD<String> words = lines.flatMap(
                s -> Arrays.asList(SPACE.split(s)
                ).iterator());
        // Create a pair RDD (a tuple of String and Integer)
        // map function here: word and its count 1
        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
        // reduce function here: reduce the rdd by key (word)
        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
        // get result and save to a text file on HDFS
        counts.saveAsTextFile(args[1]);

        spark.stop();
    }
}