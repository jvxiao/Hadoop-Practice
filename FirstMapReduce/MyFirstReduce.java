import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyFirstReduce extends Reducer<Text, Iterable<LongWritable>, Text, LongWritable> {
	static int counter = 0;
	static long sum = 0;

	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		// process values

		for (LongWritable c : values) {
			sum += Long.valueOf(key.toString());
			counter += 1;
		}
		// System.out.println("Sum of data:"+sum);
		// System.out.println("Average of Data"+sum/counter);

	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		context.write(new Text("Sum"), new LongWritable(sum));
	}
	
	

}
