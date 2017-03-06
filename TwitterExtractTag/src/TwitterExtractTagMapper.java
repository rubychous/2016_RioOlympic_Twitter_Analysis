import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.String;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Pattern;
import java.util.regex.Matcher;


public class TwitterExtractTagMapper extends Mapper<Object, Text, Text, IntWritable> { 
    private final IntWritable one = new IntWritable(1);
    private Text data = new Text();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	String line = value.toString();
	String[] field = line.split(";");
	if (field.length == 4) {
	String supportMsg = field[2].toLowerCase();
	
	Pattern tagPattern = Pattern.compile("#\\w+");
	Matcher tagMatcher = tagPattern.matcher(supportMsg);
		while (tagMatcher.find()){			
			data.set(tagMatcher.group().toString());
			context.write(data, one);
		}
	}
}	}
