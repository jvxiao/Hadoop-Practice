import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
public class FirstMapRed {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Path path = new Path("testData/Ha.txt");
		Configuration conf = new Configuration();
		Job job = new Job(conf, "FirstMapRed");
		job.setMapperClass( Mapper.class);
		job.setReducerClass(Reducer.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(3);     // ÉèÖÃReducer¸öÊý
		FileInputFormat.addInputPath(job, path);
		FileOutputFormat.setOutputPath(job, new Path("test/FirstMap8"));
		System.exit(job.waitForCompletion(true) ? 0 :1);
		
	}
	

}

