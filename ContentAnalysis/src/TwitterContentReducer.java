import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class TwitterContentReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {//Input key and value should be the same as Mapper's output key and value
													//Output key and value have been set to IntWritable for aggregating the result
    private IntWritable result = new IntWritable();							//Assign the new IntWritable to the reference variable result for the output value of Reducer

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)

              throws IOException, InterruptedException {

        int sum = 0;											//0 is assigned to an int type named sum for the following counting purpose

        for (IntWritable value : values) {								//A for loop has been created to add all numbers from input values<IntWritable> from 														//begin through the end of the list 

            sum+=value.get();											

        }

               result.set(sum);										//The value of sum has been set to the result

        context.write(key,result);										
}
}
