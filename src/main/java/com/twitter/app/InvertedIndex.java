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


class InvertedIndexMapper extends Mapper<Object, Text, Text, Text>{

	private Text function = new Text();
	Text fileName = new Text();
	
	/** Mapper takes each line as input, searches for the pattern "function_name (" and 
	outputs a <key, value> pairs as <function_name, file_name> wherever a match is found.
	*/
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		//System.out.println("Map value = " + value);
		InvertedIndex.MAPPERS++;
		System.out.println("Length = " + context.getInputSplit().getLength());
		Pattern funcPattern = Pattern.compile("[a-zA-Z0-9_]+[\\s]*"+Pattern.quote("("));
		Matcher m = funcPattern.matcher(value.toString());
		while (m.find()) {
		 String match = m.group(0);
		 function.set(match.substring(0,match.length()-1).trim());
		 fileName.set(((FileSplit) context.getInputSplit()).getPath().getName().toString());
		 context.write(function, fileName);
		}
	}
}

class InvertedIndexReducer extends Reducer<Text,Text,Text,Text> {
	
	/**The list of file names is iterated to remove duplicates*/
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		//System.out.println("Reduce key = " + key);
		Set<String> hs = new HashSet<String>();
		for (Text file : values) {
			hs.add(file.toString());
		}
		String finalStr = "";
		for(String str : hs){
			finalStr += (" " + str);
		}
		context.write(key, new Text(finalStr));
	}
}

public class InvertedIndex{
	
	public static int MAPPERS;

	public static void main (String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "inverted index");
	    job.setJarByClass(InvertedIndex.class);
		
		FileInputFormat.setInputDirRecursive(job, true);
		job.setJobName("Inverted index creation");
		job.setMapperClass(InvertedIndexMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(InvertedIndexReducer.class);
		
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

		Integer[] nMappers  = new Integer[6];
		nMappers[0] =  10;
		nMappers[1] = 50;
		nMappers[2] = 100;
		nMappers[3] = 200;
		nMappers[4] = 500;
		nMappers[5] = 1000;
		final long directorySize = 63097;

		  BufferedWriter out = null;
	      try {
		 out = new BufferedWriter(new FileWriter( (new File("output.dat")).getAbsoluteFile()));
		 for(int mappers:nMappers){
			conf.setLong("mapred.min.split.size",  directorySize/mappers);
			MAPPERS = 0;
			//job.setNumMapTasks(mappers);
			long time1 = System.nanoTime();
			job.waitForCompletion(true);
			long time2 = System.nanoTime();
			System.out.println("Mappers: " + mappers + ", " + "Time taken: " + (time2 - time1) + " ns");
			out.write(mappers + " " + conf.get("mapred.tasktracker.map.tasks.maximum")  + " " + (time2 - time1) + "\n");
			out.flush();
		}
	      }finally {
		 if (out != null) {
		    out.close();
		 }
	      }
		
	}
}
