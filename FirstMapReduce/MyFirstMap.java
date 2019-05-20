import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyFirstMap extends Mapper<LongWritable, Text, LongWritable, NullWritable> {

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
			Long num = Long.valueOf(ivalue.toString());
			context.write(new LongWritable(num),NullWritable.get());
	}

}
