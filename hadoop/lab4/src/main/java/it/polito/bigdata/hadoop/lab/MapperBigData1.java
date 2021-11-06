package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

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
                    Text> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */
    	
    		String[] split = value.toString().split(",");
    		
    		if(split[0].compareTo("Id")!=0) { //to remove the first line
    			context.write(new Text(split[2]), new Text(split[1]+","+split[6]));  //emit (key: userid, value: productid,score)
    		}
    }
}
