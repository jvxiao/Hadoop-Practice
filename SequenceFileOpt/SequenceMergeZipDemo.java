import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class SequenceMergeZipDemo {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String uri="testData/files/file1";
		Path inputDir = new Path(uri);
		Path outputfile = new Path("testData/output/allInOne.zip");
		
		IntWritable key = new IntWritable();
		Text value = new Text();
		SequenceFile.Writer writer = null;
		
		try {
		FileSystem local = FileSystem.getLocal(conf);
		FileStatus fileList[] = local.listStatus(inputDir);
		FSDataOutputStream out = local.create(outputfile);
		for(int i=0; i<fileList.length; i++) {
			System.out.println(fileList[i].getPath().getName());
			
			writer = SequenceFile.createWriter(local, conf, outputfile, key.getClass(), value.getClass());
		//	writer = SequenceFile.
			//FSDataInputStream in = local.open(fileList[i].getPath());
			byte buffer[] = new byte[256];
			int byteRead = 0;
		//	while((byteRead=in.read(buffer))>0) {
				
			}
		
			
		}
		finally{}
		
	}

}

