import java.util.*;
import java.util.regex.*;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

class CosineSimilarityMapper extends Mapper<Object, Text, Text, IntWritable>{
	
	/**Input value as the docs containing the key term. FOr Each possilbe pair of docs in the terms index output a key value pair with key as <docId1>-<square of euler length of doc1>::<docId2>-<square of euler length of doc2> and value as <term freq in doc1>*<term freq in doc2>*/

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		//System.out.println("Map value = " + value);
		
		String term = value.toString().split("::")[0].trim();
		String[] docs = value.toString().split("::")[1].trim().split(" ");
		
		for(int i=0; i<docs.length-1; i++){
			for(int j=i+1; j<docs.length; j++){
				int docId1 = Integer.parseInt(docs[i].split("@")[1].split("-")[0]);
				int docId2 = Integer.parseInt(docs[j].split("@")[1].split("-")[0]);
				if(docId1<docId2){
					context.write(
						new Text(docs[i].split("@")[1]+"::"+docs[j].split("@")[1]),
						new IntWritable( Integer.parseInt(docs[i].split("@")[0]) * Integer.parseInt(docs[j].split("@")[0]) )
					);
				}
				else{
					context.write(
						new Text(docs[j].split("@")[1]+"::"+docs[i].split("@")[1]), 
						new IntWritable( Integer.parseInt(docs[j].split("@")[0]) * Integer.parseInt(docs[i].split("@")[0]) )
					);
				}
			}
		}
	}
}

class CosineSimilarityCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {
	
	/**add the product of components(obtained from the mapper) for each doc pair with similarity*/
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int num = 0;
		for(IntWritable val : values){
			num += val.get();
		}
		context.write(key, new IntWritable(num));
	}
}

class CosineSimilarityReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	
/**add the product of components(obtained from the mapper) for each doc pair with similarity and divide by product of euclidean lengths of tweets to get cosine similarity. output doc pair and similarity if similarity greater than threshold*/
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int num = 0;
		for(IntWritable val : values){
			num += val.get();
		}
		int length1 = Integer.parseInt(key.toString().split("::")[0].split("-")[1]);
		int length2 = Integer.parseInt(key.toString().split("::")[1].split("-")[1]);
		int sim = (int) ((num*100.0) /Math.sqrt(length1*length2));
		
		String id1 = key.toString().split("::")[0].split("-")[0];
		String id2 = key.toString().split("::")[1].split("-")[0];
		
		if(sim >=70){
			context.write(new Text(id1+"-"+id2), new IntWritable(sim));
		}
	}
}

public class CosineSimilarity{
	public static void main (String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "inverted index");
	    job.setJarByClass(CosineSimilarity.class);
		
		job.setJobName("Inverted index creation");
		job.setMapperClass(CosineSimilarityMapper.class);
		job.setCombinerClass(CosineSimilarityCombiner.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setReducerClass(CosineSimilarityReducer.class);
		
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

	}
}
