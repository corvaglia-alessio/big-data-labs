package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Stream;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */ 
    		String[] split = value.toString().split(","); //split the string using comma
    		if(split.length>2) {
    			String[] products = Arrays.copyOfRange(split, 1, split.length); //remove the user id
    			String[] sortedproducts = Stream.of(products).sorted().toArray(String[]::new);
    			for(int i=0; i<sortedproducts.length; i++) {
    				for(int j=i+1; j<sortedproducts.length; j++) {
    					context.write(new Text(sortedproducts[i]+","+sortedproducts[j]), new IntWritable(1));
    				}
    			}
    		}
    }
}
