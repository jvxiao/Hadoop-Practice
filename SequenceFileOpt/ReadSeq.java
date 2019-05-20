import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.net.imap.IMAPClient.SEARCH_CRITERIA;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class ReadSeq {
	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String uri = "testData/output/allInOne2_NONE";
		Path path = new Path(uri);
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		SequenceFile.Reader reader = null;
		Searcher searcher = new Searcher();
		try {
			reader = new SequenceFile.Reader(fs, path, conf);
			Writable key = (Writable)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Writable value = (Writable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
			long position = reader.getPosition();

			searcher.searchFile("file9",reader);
			//searcher.searcherKey(4399, reader);
			//searcher.SearcherFileAndKey("file9", 16546, reader);
		}
		
		finally {
			IOUtils.closeStream(reader);
		}
		
	}
	
}

class Searcher {

	public void searchFile(String fileName, SequenceFile.Reader reader) throws IOException {
		Configuration conf = new Configuration();
		String uri = "testData/output/allInOne2_NONE";
		Path path = new Path(uri);
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		long start = 0;
		long startTime=System.currentTimeMillis();
		FileWriter result = new FileWriter("result.txt");
		BufferedWriter writer = new BufferedWriter(result);
		reader.sync(start);
		int sum = 0;
		String strArray[] = new String[2];
		String strArray2[] = new String[2];
		while (reader.next(key, value)) {
			String keyString = key.toString();
			strArray = keyString.split("#");
			String valueString = value.toString();
			strArray2 = valueString.split("\t");

			if (strArray[0].equals(fileName)) {
				System.out.printf("%s\t%s\t%s\n", key, strArray2[0], strArray2[1]);
				writer.write(value.toString()+"\n");
			}
			
		}
		long end=System.currentTimeMillis();
		System.out.println("Searching Option Costs  "+ (end-startTime)+"ms");
		writer.close();


	}

	public void searcherKey(int searchKey, SequenceFile.Reader reader) throws IOException {
		Configuration conf = new Configuration();
		String uri = "testData/output/allInOne2_NONE";
		Path path = new Path(uri);
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		long start = 0;
		long startTime=System.currentTimeMillis();
		reader.sync(start);
		String strArray[] = new String[2];
		String strArray2[] = new String[2];
		while (reader.next(key, value)) {
			// String keyString = key.toString();
			// strArray = keyString.split("#");
			String valueString = value.toString();
			strArray2 = valueString.split("\t");
			if (strArray2[0].equals(Integer.toString(searchKey))) {
				System.out.printf("%s\t%s\n", strArray2[0], strArray2[1]);
				
			}

		}
		long end=System.currentTimeMillis();
		System.out.println("Searching Option Cost  "+ (end-startTime)+"ms");

	}

	public void SearcherFileAndKey(String fileName, int searchkey, SequenceFile.Reader reader) throws IOException {
		Configuration conf = new Configuration();
		String uri = "testData/output/allInOne2_NONE";
		Path path = new Path(uri);
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		long start = 0;
		long startTime=System.currentTimeMillis();
		reader.sync(start);
		String strArray[] = new String[2];
		String strArray2[] = new String[2];
		while (reader.next(key, value)) {
			String keyString = key.toString();
			strArray = keyString.split("#");
			String valueString = value.toString();
			strArray2 = valueString.split("\t");
			if ((strArray[0].equals(fileName)) && (strArray2[0].equals(Integer.toString(searchkey)))) {
				System.out.printf("%s\t%s\n", key, value);
			}
		}
		long end=System.currentTimeMillis();
		System.out.println("Searching Option Cost  "+ (end-startTime)+"ms");

	}
}

	
