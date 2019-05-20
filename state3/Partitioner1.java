package state3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;

public class Partitioner1 extends Partitioner<Text,LongWritable>{

	
	@Override
	public int getPartition(Text key, LongWritable value, int numPartitions) {
		// TODO Auto-generated method stub
		int choosed = 0;
		String keys[]=key.toString().split("#");
		if(keys[0].equals("file1"))
			choosed=0;
		else if(keys[0].equals("file2"))
			choosed=1;
		else if(keys[0].equals("file3"))
			choosed=2;
		return choosed;
	}

	//@Override
	//public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	//}

		
}
