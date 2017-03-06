import java.lang.*;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.lang.Long;
import java.lang.Double;


public class TwitterContentAveReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {//Input key and value should be the same as Mapper's output key and value

	private IntWritable one = new IntWritable(1);			//Assign the new IntWritable to the reference variable result which is the output value of Reducer
	private Text result = new Text();

	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		double sum = 0;									//0 is assigned to an double type named sum for the following counting purpose 
												//which play as a role of calculating the input values from begin till the end
		int count = 0;									//0 is assigned ro an int type named count as a function of counting  
		for (IntWritable value:values){							//how many times the loop has been execuated				
			sum += value.get();
			count++;
		}
		result.set(Double.toString(sum/count));						//Because of previous variable type declaration (double)
		context.write(one,result);							//The format needs to be converted to string in order to meet the output value type
	}
}




