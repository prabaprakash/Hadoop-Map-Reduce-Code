import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.gson.Gson;
public class cookTime {

	public static class TokenizerMapper
			extends
				Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		Gson gson = new Gson();
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Roo roo = gson.fromJson(value.toString(), Roo.class);
			if (roo.cookTime != null) {
				word.set(roo.cookTime);
			} else {
				word.set("none");
			}
			context.write(word, one);
		}
	}

	public static class IntSumReducer
			extends
				Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Recipe");

		job.setJarByClass(cookTime.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileInputFormat.addInputPath(job, new Path("hdfs://127.0.0.1:9000/in"));
		FileOutputFormat.setOutputPath(job, new Path(
				"hdfs://127.0.0.1:9000/cookTime"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		// job.submit();
	}
}
