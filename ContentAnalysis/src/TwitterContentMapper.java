import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Arrays;
import java.lang.Math.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TwitterContentMapper extends Mapper<Object, Text, IntWritable, IntWritable> {	//Output key has been set to IntWritable in order to present the outcome: <the number of bins,1>
												//both of types are int, so we need to change to IntWritable which is supported by Hadoop Java Data types
    private final IntWritable one = new IntWritable(1);
    private IntWritable data = new IntWritable ();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();						//Input value(Text) is converted to String and assigned to String named line 
    String[] fields = line.split(";");						//Line(String) is splited by ";" and assigned to an Array of string named fields 
	if (fields.length == 4) {						//A condition has been set to extract valid twitter messages which should contain time;ID;tweets;device
		String message = fields[2].replaceAll("[^a-zA-Z0-9]", "");	//The third part of line splititng by semicolumn is extracted 
										//which should contain standard characters including a-z, A-Z and number 0-9 only,
										//otherwise it would be replaced by whitespace. The reason why I chose to filter non-standard characters 											//is explained in report
										

		int realmessage = message.length();				//Each tweet length has been calculated and assigned to int named realmessage
		int bins = message.length()/5;					//Each tweet length is divided by 5 in order to group them to different bins with interval of 5 characters
		if (realmessage<5){						//The following commands shows how to set different length into different group by using resudial
			data.set(bins+1);}
		else if (realmessage%5==0){
			data.set(bins);}
		else if (realmessage%5==1){
			data.set(bins+1);}
		else if (realmessage%5==2){
			data.set(bins+1);}
		else if (realmessage%5==3){
			data.set(bins+1);}
		else if (realmessage%5==4){
			data.set(bins+1);}
		context.write(data,one);					//The output should be <the number of bins,1>
		}
	}

}
	

