package state2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Combiner1 extends Reducer<Text, LongWritable, Text, LongWritable> {
	private int counter1=0;
	private long Sum=0;
	private long Max=0;
	private long Min=Long.MAX_VALUE;
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
				counter1 += 1;
			}
			break;
			
		}
	
	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		context.write(new Text("Max"), new LongWritable(Max));
		context.write(new Text("Min"), new LongWritable(Min));
		context.write(new Text("Sum"), new LongWritable(Sum));
		context.write(new Text("Aver"), new LongWritable(Aver/counter1));
	//	context.write(new Text("initCounter"), new LongWritable(counter1));
		
	}
}
