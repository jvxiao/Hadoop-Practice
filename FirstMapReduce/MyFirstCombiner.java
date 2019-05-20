import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyFirstCombiner extends Reducer<LongWritable, LongWritable, LongWritable, NullWritable> {

	public void reduce(LongWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
		// process values
		context.write(key, NullWritable.get());
	}

}
