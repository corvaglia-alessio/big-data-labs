package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                Text,    		// Input value type
                Text,           // Output key type
                DoubleWritable> {  // Output value type
	
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

		/* Implement the reduce method */
    	
    	HashMap<String, Integer> prod_score = new HashMap<String, Integer>();
    	int counter_user = 0;
    	int sum_scores = 0;
    	Double average_user = 0.0;
    	
    	for (Text t : values) {
    		counter_user++;
    		String[] split = t.toString().split(",");
    		int score = Integer.parseInt(split[1]);
    		sum_scores+=score;
    		prod_score.put(split[0], score);
    	}
        average_user = Double.valueOf(sum_scores) / Double.valueOf(counter_user);
    	
    	for (Entry<String, Integer> entry : prod_score.entrySet()) {
    		context.write(new Text(entry.getKey()), new DoubleWritable((Double.valueOf(entry.getValue())-average_user)));
    	}
    	
    }
}
