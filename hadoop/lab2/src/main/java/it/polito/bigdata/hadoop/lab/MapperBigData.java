package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import it.polito.bigdata.hadoop.lab.DriverBigData.COUNTERS;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData extends Mapper<
                    Text, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    Text> {// Output value type
    
    protected void map(
    		Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */
    		
    		String prefix = context.getConfiguration().get("prefix");
    		int prefix_length = prefix.length();
    	
    		String s = key.toString();
    		
    		if(s.length() >= prefix_length && s.substring(0, s.length()).toLowerCase().compareTo(prefix.toLowerCase())==0) {
    			context.write(key, value);
    			context.getCounter(COUNTERS.OK).increment(1);
    		}
    		else {
    			context.getCounter(COUNTERS.KO).increment(1);
    		}
    }
    
    protected void cleanup(Context context) {
    	System.out.println("Number of selected word: "+context.getCounter(COUNTERS.OK));
    	System.out.println("Number of discarded word: "+context.getCounter(COUNTERS.OK));
    }
}