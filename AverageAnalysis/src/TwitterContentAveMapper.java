import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Arrays;
import java.lang.Math.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TwitterContentAveMapper extends Mapper<Object, Text, IntWritable, IntWritable> { 
    private final IntWritable one = new IntWritable(1);
    private IntWritable data = new IntWritable ();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();						//Input value(Text) is converted to String and assigned to String named line
    String[] fields = line.split(";");						//Line(String) is splited by ";" and assigned to an Array of string named fields
	if (fields.length == 4) {						//A condition has been set to extract valid twitter messages which should contain time;ID;tweets;device
		data.set(fields[2].replaceAll("[^a-zA-Z0-9]", "").length());	//The third part of line splititng by semicolumn is extracted and calculated the length by each
										//All tweets should contain standard characters including a-z, A-Z and number 0-9,
										//otherwise it would be replaced by whitespace. The reason why I chose to filter non-standard characters
										//is explained in report

		context.write(one ,data);					//The position of output key and value has been changed by default setting for the purpose of counting
		}								//average becuase the total number of characters are required for Reducer as an input value

	}
}	



       
