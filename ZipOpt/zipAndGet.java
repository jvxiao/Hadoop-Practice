import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

public class zipAndGet {

	public static void main(String[] args)  {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String uri = "hdfs://192.168.179.129:9000/data";
		Path inputDir = new Path(uri);
		Path outfile = new Path("./getFile/merge.txt.zip");
		
		try{
			Class<?> codecClass = Class.forName("org.apache.hadoop.io.compress.GzipCodec");
			FileSystem hdfs = FileSystem.get(new URI(uri),conf);
			FileSystem local = FileSystem.getLocal(conf);
			
					
			FileStatus[] inputFiles = hdfs.listStatus(inputDir);
			FSDataOutputStream out = local.create(outfile);
			CompressionCodecFactory factory = new CompressionCodecFactory(conf);
			CompressionCodec codec = (CompressionCodec)
			ReflectionUtils.newInstance(codecClass, conf);
			
			
			if(codec==null) {
				System.out.println("No codec found for "+uri);
				System.exit(1);
		    }
		
			CompressionOutputStream outstream = codec.createOutputStream(out);
			
            		for (int i=0; i<inputFiles.length; i++) {
                	System.out.println(inputFiles[i].getPath().getName());
                	FSDataInputStream in = hdfs.open(inputFiles[i].getPath());
                	byte buffer[] = new byte[256];
                	int bytesRead = 0; 
                	while( (bytesRead = in.read(buffer)) > 0) {
                    	outstream.write(buffer, 0, bytesRead);
                	}
                	in.close();
            		}
            		outstream.close();
        	}	  
		catch (Exception e) {
            	e.printStackTrace();
          	}
	}

}
















