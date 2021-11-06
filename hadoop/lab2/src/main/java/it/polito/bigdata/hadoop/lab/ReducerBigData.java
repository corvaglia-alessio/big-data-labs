// NOT USED!! IT IS A MAP ONLY JOB

package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                LongWritable,    // Input value type
                Text,           // Output key type
                LongWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<LongWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

		/* Implement the reduce method */
    	
    }
}
