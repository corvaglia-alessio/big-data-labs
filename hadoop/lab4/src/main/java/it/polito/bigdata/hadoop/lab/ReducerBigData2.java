package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<
                Text,           // Input key type
                DoubleWritable,    // Input value type
                Text,           // Output key type
                DoubleWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<DoubleWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

		/* Implement the reduce method */
    	
    	int count=0;
    	Double sum=0.0;
    	
    	for(DoubleWritable d : values) {
    		count++;
    		sum+=Double.parseDouble(d.toString());
    	}
    	
    	Double avg = sum / Double.valueOf(count);
    	
    	context.write(key, new DoubleWritable(avg));
    }
}
