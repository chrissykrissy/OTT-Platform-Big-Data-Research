import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper extends Mapper<Object, Text, Text, NullWritable>{

    // private final static IntWritable one = new IntWritable(1);
    // private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      //Clean
    	String line = value.toString();
    	String[] splitL = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
    	// System.println(splitL.length);
    	
		String mName = "";
		String lang = "";
		String rat = "";
		String runT = "";
		String year ="";
		String matRat ="";
		String plot ="";

    	if (splitL.length >= 6){
	    	mName = splitL[0];
			lang = splitL[1];
			rat = splitL[2];
			runT = splitL[3];
			year = splitL[4];
			matRat = splitL[5];
			// plot = splitL[6];
    	}
   //  	else{
   //  		mName = splitL[0];
			// lang = splitL[1];
			// rat = splitL[2];
			// runT = splitL[3];
			// year = splitL[4];
			// matRat = splitL[5];
   //  	}

		// String plot = splitL[6];
		// if (year <= 2019 && !mName.equals("")){
			context.write(new Text(mName+","+rat+","+year+","+matRat+","+"Amazon"+","), NullWritable.get());
		// }
    }
}