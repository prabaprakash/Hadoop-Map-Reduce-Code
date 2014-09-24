 import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class checkMultiple
{
  public static class Map1 extends Mapper<LongWritable,Text,Text,IntWritable>
  {
              public void map(LongWritable k, Text v, Context con) throws IOException, InterruptedException
              {
                           String line=v.toString();
                           String[] words=line.split("\t");
                           String sex=words[1];
                           int sal=Integer.parseInt(words[2]);
                           con.write(new Text(sex), new IntWritable(sal));
              }
  }
  public static class Map2 extends Mapper<LongWritable,Text,Text,IntWritable>
  {
              public void map(LongWritable k, Text v, Context con) throws IOException, InterruptedException
              {
                           String line=v.toString();
                           String[] words=line.split(",");
                           String sex=words[2];
                           int sal=Integer.parseInt(words[3]);
                           con.write(new Text(sex), new IntWritable(sal));
              }
  }
  public static class Red extends Reducer<Text,IntWritable,Text,IntWritable>
  {
               public void reduce(Text sex, Iterable<IntWritable> salaries, Context con)
                throws IOException , InterruptedException
                {
                            int tot=0;
                            for(IntWritable sal:salaries)
                            {
                                    tot+=sal.get();
                            }
                            con.write(sex, new IntWritable(tot));
                       
                }
   }
  public static void main(String[] args) throws Exception
  {
              Configuration c=new Configuration();
              String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
             
              Job j = new Job(c,"multiple");
              j.setJarByClass(checkMultiple.class);
              j.setMapperClass(Map1.class);
              j.setMapperClass(Map2.class);
              j.setReducerClass(Red.class);
              j.setOutputKeyClass(Text.class);
              j.setOutputValueClass(IntWritable.class);
              MultipleInputs.addInputPath(j, new Path("hdfs://127.0.0.1:9000/Multiple/a.txt"), TextInputFormat.class, Map1.class);
              MultipleInputs.addInputPath(j,new Path("hdfs://127.0.0.1:9000/Multiple/b.txt"), TextInputFormat.class, Map2.class);
      FileOutputFormat.setOutputPath(j, new Path("hdfs://127.0.0.1:9000/Reviews"));
      System.exit(j.waitForCompletion(true) ? 0:1);
             
  }

}
