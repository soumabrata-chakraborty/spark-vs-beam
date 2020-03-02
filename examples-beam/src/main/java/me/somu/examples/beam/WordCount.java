package me.somu.examples.beam;

import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;

import java.util.Arrays;

import static org.apache.beam.sdk.values.TypeDescriptors.strings;

public class WordCount {

    public static void main(String[] args) {

        final String inputDirPattern = args[0];
        final String outputPath = args[1];

        final PipelineOptions options = PipelineOptionsFactory.create();
        options.setRunner(SparkRunner.class);
        final Pipeline pipeline = Pipeline.create(options);

        pipeline
            .apply(TextIO.read().from(inputDirPattern))                                          //Read all text files into lines
            .apply(FlatMapElements.into(strings()).via(line -> Arrays.asList(line.split(" "))))  //Flat map lines to words
            .apply(MapElements.into(strings()).via(word -> word.toLowerCase()))                  //Convert words to lower case
            .apply(MapElements.into(strings()).via(word -> word.replaceAll("[^a-z]", "")))       //Strip all special characters
            .apply(MapElements.into(strings()).via(word -> word.trim()))                         //Trim words
            .apply(Filter.by(word -> !word.isEmpty()))                                           //Remove empty words
            .apply(Count.perElement())                                                           //Perform word count
            .apply(MapElements.into(strings()).via(kv -> kv.getKey() + "__" + kv.getValue()))    //Arrange as word followed by count
            .apply(TextIO.write().to(outputPath));                                               //Save as text

        pipeline.run().waitUntilFinish();
    }
}
