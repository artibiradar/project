package question4;

	import java.io.IOException;
	import java.util.TreeMap;

	import org.apache.hadoop.conf.Configuration;
	import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.io.LongWritable;
	import org.apache.hadoop.io.NullWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Job;
	import org.apache.hadoop.mapreduce.Mapper;
	import org.apache.hadoop.mapreduce.Partitioner;
	import org.apache.hadoop.mapreduce.Reducer;
	import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
	import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
	import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
	import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
	public class DataEnggGrowth4 {
		public static class DataEngineerMapper extends Mapper<LongWritable,Text,Text,Text>	
		{
			 
			public void map(LongWritable key,Text values,Context context) throws IOException, InterruptedException,ArrayIndexOutOfBoundsException
			{  	
			  try
			  {
			
				   String str []=values.toString().split("\t");	  
				   String job_title=str[4];
		          	String year=str[7];
		          	
		          	{
		            context.write(new Text(job_title),new Text(year));
		          
					}
				  }
			  
			  catch (Exception e) 
			  {
				  System.out.println(e.getMessage());
			  }
		    }
		}
		public static class DataEngineerReducer extends Reducer < Text, Text, NullWritable, Text > 
		{
		     TreeMap < LongWritable,Text > Top5Employers = new TreeMap < LongWritable,
		    Text > ();
		    long sum = 0;
		    public void reduce(Text key, Iterable < LongWritable > values, Context context) throws IOException,
		    InterruptedException {
		        sum = 0;
		        for (LongWritable val: values) {
		            sum += val.get();
		        }
		        Top5Employers.put(new LongWritable(sum), new Text(key + "," + sum));
		        if (Top5Employers.size() > 5)
		            Top5Employers.remove(Top5Employers.firstKey());

		    }
		    public void cleanup(Context context) throws IOException,
		    InterruptedException {
		        for (Text t: Top5Employers.descendingMap().values())
		            context.write(NullWritable.get(), t);

		    }
		}
		public static class DataEnggGrowth extends Partitioner < Text, LongWritable > 
		{
		    public int getPartition(Text key, LongWritable value, int numReduceTasks) 
		    {
		        String[] str = key.toString().split("\t");
		        if (str[1].equals("2011"))
		        {
		            return 0;
		        }
		        else if (str[1].equals("2012"))
		        {
		            return 1;
		        }
		        else if (str[1].equals("2013"))
		        {
		            return 2;
		        }
		        else if (str[1].equals("2014"))
		        {
		            return 3;
		        }
		        else if (str[1].equals("2015"))
		        {
		            return 4;
		        }
		        else if (str[1].equals("2016"))
		        {
		            return 5;
		        }
		        else
		        {
		            return 6;
		        }
		    }
		}
		public static void main(String args[]) throws Exception
		{
		Configuration conf= new Configuration();
		Job job= new Job(conf,"Question 4");
		job.setJarByClass(DataEnggGrowth4.class);
		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));	
		job.setMapperClass(DataEngineerMapper.class);
		job.setPartitionerClass(DataEnggGrowth.class);
		job.setReducerClass(DataEngineerReducer.class);
		job.setNumReduceTasks(7);
		job.setInputFormatClass(TextInputFormat.class);	
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true)?1:0);
		}	
	}



