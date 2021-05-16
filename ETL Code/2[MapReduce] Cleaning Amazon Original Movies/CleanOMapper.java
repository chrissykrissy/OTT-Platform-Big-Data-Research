import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanOMapper extends Mapper<Object, Text, Text, NullWritable>{


    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      //Clean
    	String line = value.toString();
    	String[] splitL = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
    	
		String date = "";
		String mName = "";
		String notes = "";

    	if (splitL.length >= 2){
	    	date = splitL[0];
			mName = splitL[1];
    	}

      	context.write(new Text(mName+","+"Amazon"+","+1+","),NullWritable.get());
    }
}