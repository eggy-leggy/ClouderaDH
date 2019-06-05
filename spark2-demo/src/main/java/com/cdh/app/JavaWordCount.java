package com.cdh.app;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public final class JavaWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {
        System.out.println(args.length + " ------ " + args[0]);
        if (args.length < 1) {
            System.err.println("Usage: JavaWordCount <file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaWordCount")
                .getOrCreate();
        System.out.println("SparkSession ------- JavaWordCount");
        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
        System.out.println("SparkSession ------- read file");
        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
        System.out.println("SparkSession ------- line flatMap");
        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
        System.out.println("SparkSession ------- words mapToPair");
        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
        System.out.println("SparkSession ------- ones reduceByKey");
        List<Tuple2<String, Integer>> output = counts.collect();
        System.out.println("SparkSession ------- counts collect");
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ":-----------------------: " + tuple._2());
        }
        spark.stop();
    }
}