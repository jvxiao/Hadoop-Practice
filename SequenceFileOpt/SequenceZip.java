import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.ReflectionUtils;

public class SequenceZip {

	public static void main(String[] args) throws IOException{
		// TODO Auto-generated method stub
		File log = new File("index4SEQ");

		
		BufferedReader reader = null;
		Configuration conf = new Configuration();
		String uri="testData/files/";
		Path inputDir = new Path(uri);
		File file = null;
		FileSystem local = FileSystem.getLocal(conf);
		FileStatus fileList[] = local.listStatus(inputDir);
		Path outputfile = new Path("testData/output/allInOne2_NONE");
		FSDataOutputStream out = local.create(outputfile);
		
		Text key = new Text();
		Text value = new Text();
		SequenceFile.Writer writer = null;
		String tmp;
		int	j=0;
		try {
			FileWriter logwriter = new FileWriter(log);
			BufferedWriter logout = new BufferedWriter(logwriter);
			writer = SequenceFile.createWriter(local, conf, outputfile, key.getClass(),value.getClass(),CompressionType.NONE);
			for(int i=1; i<=10;i++) {
			j=0;
		
			file = new File(uri+"file"+i);
			reader =new  BufferedReader(new FileReader(file));
			logout.write(uri+"file"+i+"\n");
			while((tmp=reader.readLine())!=null) {
		
			
			key.set("file"+i+"#"+j++);
				value.set(tmp); 
			
				writer.append(key, value);
				
			}
			
			}
			System.out.println("finish  "+fileList.length);
		}
		finally {
			IOUtils.closeStream(writer);
		}
	}

}

