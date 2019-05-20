package state1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<LongWritable, Text, Text, LongWritable> {
		private long value;
		private int counter;
		
	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		value = Long.valueOf(ivalue.toString());
		counter++;
		context.write(new Text("Max"), new LongWritable(value));
		context.write(new Text("Min"), new LongWritable(value));
		context.write(new Text("Sum"), new LongWritable(value));
		context.write(new Text("Aver"), new LongWritable(value));
	//	context.write(new Text("Counter"), new LongWritable(counter));
	}
	
		

}
