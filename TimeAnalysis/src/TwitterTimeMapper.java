import java.io.IOException;
import java.util.Date;						//The class-Date is imported allowing the formatting and parsing of date strings
import java.text.SimpleDateFormat;				//The class-SimpleDateFormat is imported for formatting and parsing dates in a locale-sensitive manner
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



    public class TwitterTimeMapper extends Mapper<Object, Text, Text, IntWritable> { 
    private final IntWritable one = new IntWritable(1);
    private Text data = new Text();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	String line = value.toString();							//Input value(Text) is converted to String and assigned to String named line
    	String[] fields = line.split(";");						//Line(String) is splited by ";" and assigned to an Array of string named fields 
	if (fields.length == 4) {  							//A condition has been set to extract valid twitter messages which contains 												//time;ID;tweets;device
		int x = 13;								//The variable x has been set to check if the length of the first part of line splititng by semicolumn 
		if(fields[0].length()==x){						//equals to 13 which means that the time format matches epoch_time exactly


								 
			SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");		//The date-time format is defined by "MM/dd/yyyy"
			Date tweetsdate = new Date(Long.parseLong(fields[0]));				//Assign the new Date to the reference variable tweetsdate initializing 
			String EachDay = formatter.format(tweetsdate);					//Convert the epoch_time format into "MM/dd/yyyy"
			data.set(EachDay);
			context.write(data,one);
			}
		}	
	}
}	


