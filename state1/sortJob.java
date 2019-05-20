package state1;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class sortJob {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		long start=System.currentTimeMillis();
		Configuration conf = new Configuration();
		Job job = new Job(conf,"sortJob");
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setMapperClass(Mapper1.class);
		job.setReducerClass(Reducer1.class);
		
	//	job.setMapperClass(MyFirstMap.class);
	//	job.setReducerClass(MyFirstReduce.class);
		
	//	job.setCombinerClass(Combiner1.class);
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path("testData/3numberFiles/"));
		FileOutputFormat.setOutputPath(job, new Path("testData/output/out09"));
		job.waitForCompletion(true);
		long end=System.currentTimeMillis();
		System.out.println("Finish in "+ (end-start)+"ms");
	}


}
