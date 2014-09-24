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

public class DishSearch {
	public String item;

	public static class TokenizerMapper
			extends
				Mapper<Object, Text, Text, Text> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		Gson gson = new Gson();
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			Roo roo = gson.fromJson(value.toString(), Roo.class);

			try {
				if (roo.ingredients.contains("tomato")
						&& roo.ingredients.contains("onion")
						&& roo.ingredients.contains("salt")) {
					word.set(roo.name + "  " + roo.description);
				} else {
					word.set("none");
				}
			} catch (Exception e) {
				word.set("none");
			}

			context.write(new Text("1"), word);
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (Text val : values) {
				if (!val.toString().matches("none")) {
					context.write(new Text(), val);
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "cookTime");

		job.setJarByClass(DishSearch.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileInputFormat.addInputPath(job, new Path("hdfs://127.0.0.1:9000/in"));
		FileOutputFormat.setOutputPath(job, new Path(
				"hdfs://127.0.0.1:9000/DishSearch"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		// job.submit();
	}
}
