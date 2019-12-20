import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SentimentAnalysis {
	public static class Map extends Mapper<Object, Text, Text, Text> {
		private HashMap<String, String> emotionDict = new HashMap<String, String>();

		@Override
		public void setup(Context context) throws IOException {
			Configuration configuration = context.getConfiguration();
			String dict = configuration.get("dict", "");
			BufferedReader reader = new BufferedReader(new FileReader(dict));
			String line = reader.readLine();
			while(line !=null){
				String[] word_emotion = line.split("\t");
				String word=word_emotion[0].trim().toLowerCase();
				String e = word_emotion[1].trim();
				emotionDict.put(word, e);
				line=reader.readLine();
			}
			reader.close();

		}

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

			String line = value.toString().trim();
			StringTokenizer tokenizer = new StringTokenizer(line);
			Text filename = new Text(fileName);
			while(tokenizer.hasMoreTokens()){
String word = tokenizer.nextToken().trim().toLowerCase();
			if(emotionDict.containsKey(word)){
context.write(filename, new Text(emotionDict.get(word)));
}	
			}

			// TODO : get file name
			// get emotion value for each word

			// output : (word, emotion)
		}
	}

	public static class Reduce extends
			Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// TODO : count frequecy for all kinds of emotion
		HashMap<String, Integer> map = new HashMap<>();
for(Text value: values){
String emotion = value.toString();
int count = map.containsKey(emotion)?map.get(emotion):0;
map.put(emotion, count+1);
}
StringBuilder builder = new StringBuilder();
Iterator<String> iter = map.keySet().iterator();
while(iter.hasNext()){
String emotion = iter.next();
int count = map.get(emotion);
builder.append(emotion).append("\t").append(count);
context.write(key,new Text( builder.toString()));
builder.setLength(0);
}

			// output : (filename, emotion frequecy)
		}
	}

	// args[0] : input path
	// args[1] : output path
	// args[2] : path of emotion dictionary.
	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();

		// args[2] : path of emotion dictionary.
		configuration.set("dict", args[2]);
		Job job = Job.getInstance(configuration);
		job.setJarByClass(SentimentAnalysis.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// args[0] : input path
		// args[1] : output path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
