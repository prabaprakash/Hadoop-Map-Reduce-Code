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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.NewAppWeightBooster;

import com.google.gson.Gson;
public class IngridentCheck {

	public static class TokenizerMapper
			extends
				Mapper<Object, Text, Text, Text> {

		private final static Text one = new Text("1");
		private Text word = new Text("none");
		Gson gson = new Gson();
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			Roo roo = gson.fromJson(value.toString(), Roo.class);
			String p = roo.ingredients.replace("\n", ";");
			String[] ia = roo.ingredients.split("\n");

			if (ia.length > 100) {
				one.set(p + "");
				word.set(roo.name + "  " + ia.length + " ");
				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			StringBuilder string = new StringBuilder();

			for (Text text : values) {
				string.append(text);
			}
			context.write(key, new Text(string.toString()));

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Recipe");

		job.setJarByClass(IngridentCheck.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileInputFormat.addInputPath(job, new Path("hdfs://127.0.0.1:9000/in"));
		FileOutputFormat.setOutputPath(job, new Path(
				"hdfs://127.0.0.1:9000/IngridentCheck"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		// job.submit();
	}
}
