package state1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Reducer1 extends org.apache.hadoop.mapreduce.Reducer<Text, LongWritable, Text, LongWritable> {
	private long counter;
	private long Max=0;
	private long Min=Long.MAX_VALUE;
	private long Sum=0;
	private long Aver=0;
	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		// process values
switch(key.toString()) {
		
		case "Sum":
			for (LongWritable c : values) {
			Sum += Long.valueOf(c.toString());
			
			}
			break;
			
		case "Max":
			for (LongWritable c: values) {
				if(Long.valueOf(c.toString())>Max) {
					Max = Long.valueOf(c.toString());
				}
			}
			break;
			
		case "Min":
			for(LongWritable c:values) {
				if(Long.valueOf(c.toString())<Min){
					Min = Long.valueOf(c.toString());
				}
			}
			break;
			
		case "Aver":
			for(LongWritable c:values) {
				Aver+=Long.valueOf(c.toString());
				counter += 1;
			}
			break;
			
		case "initCounter":
			for(LongWritable c: values) {
				counter+=Long.valueOf(c.toString());
			}
			break;
		}
		
	
	}
	
	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Reducer<Text, LongWritable, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.cleanup(context);
		context.write(new Text("Max"), new LongWritable(Max));
		context.write(new Text("Min"), new LongWritable(Min));
		context.write(new Text("Sum"), new LongWritable(Sum));
		context.write(new Text("Aver"), new LongWritable(Aver/counter));
		//context.write(new Text("initCounter"), new LongWritable(counter));
	}
	
	

}
