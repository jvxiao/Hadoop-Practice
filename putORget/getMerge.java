
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class getMerge{
	public static void main(String[] args) throws IOException{
		Configuration conf = new Configuration();
		String uri = "hdfs://47.102.133.61:8020/home/Exp3/testData";
		Path inputDir = new Path(uri);
		Path outfile = new Path("./getFile/merge");
				
		try{
			FileSystem hdfs = FileSystem.get(new URI(uri),conf);
			FileSystem local = FileSystem.getLocal(conf);
					
			FileStatus[] inputFiles = hdfs.listStatus(inputDir);
			FSDataOutputStream out = local.create(outfile);
         
            		for (int i=0; i<inputFiles.length; i++) {
                	System.out.println(inputFiles[i].getPath().getName());
                	FSDataInputStream in = hdfs.open(inputFiles[i].getPath());
                	byte buffer[] = new byte[256];
                	int bytesRead = 0; 
                	while( (bytesRead = in.read(buffer)) > 0) {
                    	out.write(buffer, 0, bytesRead);
                	}
                	in.close();
            		}
            		out.close();
        	}	  
		catch (Exception e) {
            	e.printStackTrace();
          	}
	}
}

