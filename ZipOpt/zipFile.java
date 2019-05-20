import java.io.IOException;
import java.net.URI;
//import org.apache.commons.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.io.IOUtils;
public class zipFile {

	public static void main(String[] args) throws ClassNotFoundException, IOException {
		// TODO Auto-generated method stub
		String codecClassname = args[0];
		Class<?> codecClass = Class.forName(codecClassname);
		Configuration conf = new Configuration();
		CompressionCodec codec = (CompressionCodec)
		ReflectionUtils.newInstance(codecClass, conf);
		
		CompressionOutputStream out = codec.createOutputStream(System.out);
		IOUtils.copyBytes(System.in, out,conf);
		out.finish();
		
		
		
	}

}

