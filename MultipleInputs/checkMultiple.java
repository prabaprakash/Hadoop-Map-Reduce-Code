import java.io.IOException;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.AbstractMap.SimpleEntry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.gson.Gson;
public class Reviews {
	public static class TokenizerMappera
			extends
				Mapper<Object, Text, Text, Text> {

		private final static Text one = new Text("1");
		// private Text word = new Text();
		Gson gson = new Gson();
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// String[] string=value.toString().split("::");
			// for (String string2 : string) {
			context.write(one, new Text("1::" + value.toString()));
			// }
		}
	}
	public static class TokenizerMapperb
			extends
				Mapper<Object, Text, Text, Text> {

		private final static Text one = new Text("1");
		// private Text word = new Text();
		Gson gson = new Gson();
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// String[] string=value.toString().split("::");
			// for (String string2 : string) {
			context.write(one, new Text("2::" + value.toString()));
			// }
		}
	}
	public static class TokenizerMapperc
			extends
				Mapper<Object, Text, Text, Text> {

		private final static Text one = new Text("1");
		// private Text word = new Text();
		Gson gson = new Gson();
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// String[] string=value.toString().split("::");
			// for (String string2 : string) {
			context.write(one, new Text("3::" + value.toString()));
			// }
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
		// private Text result = new Text();

		// static ArrayList<Reviews.movies> movieList=new
		// ArrayList<Reviews.movies>();
		// ArrayList<users> userlist=new ArrayList<Reviews.users>();
		// ArrayList<ratings> ratingList=new ArrayList<Reviews.ratings>();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String[] value;
			ArrayList<Entry<Integer, String>> movielist = new ArrayList<Entry<Integer, String>>();
			ArrayList<Entry<Integer, String>> ratinglist = new ArrayList<Entry<Integer, String>>();
			ArrayList<Entry<Integer, String>> userlist = new ArrayList<Entry<Integer, String>>();

			for (Text val : values) {
				value = val.toString().split("::");
				if (value[0].equals("1")) {
					movielist.add(new SimpleEntry<Integer, String>(Integer
							.parseInt(key.toString()), val.toString()));
				}
				if (value[0].equals("2")) {
					ratinglist.add(new SimpleEntry<Integer, String>(Integer
							.parseInt(key.toString()), val.toString()));
				}
				if (value[0].equals("3")) {
					userlist.add(new SimpleEntry<Integer, String>(Integer
							.parseInt(key.toString()), val.toString()));
				}

			}
			Text aText = new Text();

			// ArrayList<Entry<Integer, String>> out = new
			// ArrayList<Entry<Integer, String>>();
			
			for (Entry<Integer, String> a : movielist) {

				aText.set(a.getValue());
				context.write(new Text("1"), aText);

			}
			for (Entry<Integer, String> a : ratinglist) {

				aText.set(a.getValue());
				context.write(new Text("2"), aText);

			}
			for (Entry<Integer, String> a : userlist) {

				aText.set(a.getValue());
				context.write(new Text("3"), aText);

			}

		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// String[] otherArgs = new GenericOptionsParser(conf, args)
		// .getRemainingArgs();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Reviews");

		job.setJarByClass(Reviews.class);
		job.setMapperClass(Reviews.TokenizerMappera.class);
		job.setMapperClass(Reviews.TokenizerMapperb.class);
		job.setMapperClass(Reviews.TokenizerMapperc.class);
		job.setCombinerClass(Reviews.IntSumReducer.class);
		job.setReducerClass(Reviews.IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		MultipleInputs.addInputPath(job, new Path(
				"hdfs://127.0.0.1:9000/Movie/movies.dat"),
				TextInputFormat.class, TokenizerMappera.class);
		MultipleInputs.addInputPath(job, new Path(
				"hdfs://127.0.0.1:9000/Movie/ratings.dat"),
				TextInputFormat.class, TokenizerMapperb.class);
		MultipleInputs.addInputPath(job, new Path(
				"hdfs://127.0.0.1:9000/Movie/users.dat"),
				TextInputFormat.class, TokenizerMapperc.class);

		FileOutputFormat.setOutputPath(job, new Path(
				"hdfs://127.0.0.1:9000/Reviews1"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		// /org.apache.hadoop.mapreduce.Counters aC=job.getCounters();
		// aC.findCounter(Match_COUNTER.a);
		// job.submit();
	}
	/*
	 * public class movies { public int id; public String name; public String[]
	 * genre; } // UserID::Gender::Age::Occupation::Zip-code public class users
	 * { public int id; public String gender; public int age; public int
	 * occupation; public double zipcode; } public class ratings { public int
	 * userid; public int movieid; public int rating; public String timestamp;
	 * 
	 * }
	 */

}
