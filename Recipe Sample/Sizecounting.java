import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
public class Sizecounting

{
   public static class MapClass extends MapReduceBase implements       Mapper<LongWritable,Text,IntWritable,Text>
     {
          IntWritable one; 
          Text word=new Text();
          public void map(LongWritable key,Text value,OutputCollector     output,Reporter reporter) throws IOException

              {
                 String line=value.toString();
                 StringTokenizer st=new StringTokenizer(line," ");
                 while(st.hasMoreTokens())
                   {
                   String word=st.nextToken();
                   int size=word.length();
                   output.collect(new IntWritable(size),new Text(String.valueOf(one)));
                  }
              }
     }
  public static class ReduceClass extends MapReduceBase implements Reducer<IntWritable,Text,IntWritable,IntWritable>
     {
        public void reduce(IntWritable key,Iterator values,OutputCollector output,Reporter reporter) 
                            throws IOException
         {
             int sum=0;  
             while(values.hasNext())

              {
                 values.next();                                                                                                                                                       
                 sum++;
              }  
              output.collect(key, new IntWritable(sum));
         }

     }
  public static void main(String[] args)
  {
      JobClient client = new JobClient();

     //Used to distinguish Map and Reduce jobs from others
      JobConf conf = new JobConf(Sizecounting.class);

      //Specify key and value class for Mapper
      conf.setMapOutputKeyClass(IntWritable.class);
      conf.setMapOutputValueClass(Text.class);

     // Specify output types
     conf.setOutputKeyClass(IntWritable.class);
     conf.setOutputValueClass(IntWritable.class);

     // Specify input and output DIRECTORIES (not files)
     FileInputFormat.setInputPaths(conf, new Path("input"));

     FileOutputFormat.setOutputPath(conf, new Path("output"));

     //Specify input and output format
     conf.setInputFormat(TextInputFormat.class);
     conf.setOutputFormat(TextOutputFormat.class);

     //Specify Mapper and Reducer class
     conf.setMapperClass(MapClass.class);
     conf.setReducerClass(ReduceClass.class);
     client.setConf(conf);

      try 
         {
          JobClient.runJob(conf);
         } 
      catch (Exception e)
        {                                                                            
          e.printStackTrace();                                                               
        }
     }
}