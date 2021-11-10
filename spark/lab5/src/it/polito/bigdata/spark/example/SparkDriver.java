package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;

import org.apache.spark.SparkConf;

public class SparkDriver {
	
	public static void main(String[] args) {


		String inputPath;
		String outputPath;
		String prefix;


		inputPath=args[0];
		outputPath=args[1];
		prefix=args[2];

		
	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Lab #5");
		
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		// Read the content of the input file/folder
		// Each element/string of wordFreqRDD corresponds to one line of the input data 
		// (i.e, one pair "word\tfreq")  
		JavaRDD<String> wordFreqRDD = sc.textFile(inputPath);
		
		
		//filter using the given prefix
		JavaRDD<String> filteredWords = wordFreqRDD.filter(l -> l.indexOf(prefix)==0);
		
		//take only the frequencies
		JavaRDD<Long> values = filteredWords.map(l -> {
			String[] split = l.split("\t");
			Long num = Long.parseLong(split[1]);
			return num;
		});
		
		//find the max frequency
		Long max = values.reduce((e1, e2) -> {return Long.max(e1, e2);});
		
		//define the threshold
		Double threshold = 0.8D * (double) max;
		
		//filter using the threshold
		JavaRDD<String> overThreshold = filteredWords.filter(l -> {
			String[] split = l.split("\t");
			if(Long.parseLong(split[1])>=threshold)
				return true;
			else
				return false;
		});
		
		//count the remaining words
		long count = overThreshold.count();
		
		//remove frequencies in order to later save on files
		JavaRDD<String> noFrequencyWords = overThreshold.map(l -> {
			String[] split = l.split("\t");
			return split[0];
		});
			 	
		//write on the text file
		noFrequencyWords.saveAsTextFile(outputPath);
		
		//output on the standard output
		System.out.println("Max frequence among the filtered words: "+max);
		System.out.println("Number of words over 80% of the max frequency: "+count);
		
		// Close the Spark context
		sc.close();
	}
}
