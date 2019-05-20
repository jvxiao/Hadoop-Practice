import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class cityInfo implements WritableComparable<cityInfo> {

	private DoubleWritable longitude;
	private DoubleWritable latitude;
	private Text city;
	private LongWritable population;

	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		longitude.readFields(in);
		latitude.readFields(in);
		city.readFields(in);
		population.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		longitude.write(out);;
		latitude.write(out);;
		city.write(out);
		population.write(out);;
		
	}
	

	@Override
	public int compareTo(cityInfo o) {
		// TODO Auto-generated method stub
		
		return (longitude.compareTo(o.longitude)!=0)
				?longitude.compareTo(o.longitude)
				:latitude.compareTo(o.latitude);
	}

}
