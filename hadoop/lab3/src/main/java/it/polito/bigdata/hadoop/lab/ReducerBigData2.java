package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<
                Text,           // Input key type
                IntWritable,    // Input value type
                Text,           // Output key type
                IntWritable> {  // Output value type
    
	TopKVector<WordCountWritable> top100 = new TopKVector<WordCountWritable>(100);

    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<IntWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

		/* Implement the reduce method */
    	int count=0;
    	for(IntWritable value : values) {
    		count+=Integer.parseInt(value.toString());
    	}
    	top100.updateWithNewElement(new WordCountWritable(key.toString(), count));  //add the pair in the top100
    }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	Vector<WordCountWritable> vector100 = top100.getLocalTopK();			//cleanup executed once at the end of all reduce() invocation in the reducer
    	for(WordCountWritable w : vector100) {									//print on the file the final top 100
    		context.write(new Text(w.getWord()), new IntWritable(w.getCount()));
    	}
    }
}
