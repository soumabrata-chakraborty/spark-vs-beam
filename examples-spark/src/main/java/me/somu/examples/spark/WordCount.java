package me.somu.examples.spark;

import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {

    public static void main(String[] args) {

        final String inputDir = args[0];
        final String outputDir = args[1];

        final JavaSparkContext sparkContext = new JavaSparkContext();

        sparkContext
            .textFile(inputDir)                                            //Read all text files in directory into lines
            .flatMap(line -> Arrays.asList(line.split(" ")).iterator())    //Flat Map lines to words
            .map(word -> word.toLowerCase())                               //Convert words to lower case
            .map(word -> word.replaceAll("[^a-z]", ""))                    //Strip all special characters
            .map(word -> word.trim())                                      //Trim words
            .filter(word -> !word.isEmpty())                               //Remove empty words
            .mapToPair(word -> new Tuple2<>(word, 1))                      //Prepare for word count
            .reduceByKey((a, b) -> a + b)                                  //Perform word count
            .map(tuple -> tuple._1() + "__" + tuple._2())                  //Arrange as word followed by count
            .saveAsTextFile(outputDir);                                    //Save as text
    }
}
