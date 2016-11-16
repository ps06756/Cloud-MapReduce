import java.util.*;
import java.util.regex.*;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


class TwitterIndexMapper extends Mapper<Object, Text, Text, Text>{
	
	/** 
	 * Vector for each tweet is generated. 
	 * a key value pair consisting of a <term, <doc-freq>@<tweetId> - <square of euler length of tweet> > is generated once for the first occurence of each term in the tweet*/
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		System.out.println("Map value = " + value);
		
		String tweetId = value.toString().split("::")[0];
		value = new Text(value.toString().split("::")[1].trim());
		
		Set<String> termSet = new HashSet<String>();
		Hashtable<String, Integer> vec = new Hashtable<String, Integer>();
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			String term = itr.nextToken();
			if(!vec.containsKey(term))
			{
				vec.put(term, 1);
				termSet.add(term);
			}
			else{
				vec.put(term, vec.get(term) + 1);
			}
		}
		int length = 0;
		for(String term: termSet){
			length += (vec.get(term))*(vec.get(term));
		}
		for(String term: termSet){
			context.write(new Text(term), new Text(vec.get(term).toString()+"@"+tweetId+"-"+length));
		}
	}
}

class TwitterIndexReducer extends Reducer<Text,Text,Text,Text> {
	

	/**The list of <doc-freq>@<tweetId>-<square of Euler length of tweet> is generated ( list can't have multiple items with the same tweetId from mapper's construction.)*/
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		System.out.println("Reduce key = " + key);
		
		String finalStr = ":: ";
		for (Text doc : values) {
			finalStr += doc.toString()+" ";
		}
		context.write(key, new Text(finalStr));
	}
}

public class TwitterIndex{
	public static void main (String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "inverted index");
	    job.setJarByClass(TwitterIndex.class);
		
		job.setJobName("Inverted index creation");
		job.setMapperClass(TwitterIndexMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(TwitterIndexReducer.class);
		
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

	}
}
