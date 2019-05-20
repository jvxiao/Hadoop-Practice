package state3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<LongWritable, Text, Text, LongWritable> {
		private long value;
		private int counter;
		static int n = 1;
	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		value = Long.valueOf(ivalue.toString());
		counter++;
		context.write(new Text("file"+n+"#Max"), new LongWritable(value));
		context.write(new Text("file"+n+"#Min"), new LongWritable(value));
		context.write(new Text("file"+n+"#Sum"), new LongWritable(value));
		context.write(new Text("file"+n+"#Aver"), new LongWritable(value));
	//	context.write(new Text("Counter"), new LongWritable(counter));
	}
	@Override
	protected void cleanup(Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		
		super.cleanup(context);
		n++;
	}
	
		

}
