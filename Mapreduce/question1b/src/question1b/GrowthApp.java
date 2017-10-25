package question1b;

	import java.io.IOException;

    import java.util.TreeMap;
	import org.apache.hadoop.conf.Configuration;
	import org.apache.hadoop.fs.Path;
	//import org.apache.hadoop.io.IntWritable;
	import org.apache.hadoop.io.LongWritable;
	import org.apache.hadoop.io.NullWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Job;
	import org.apache.hadoop.mapreduce.Mapper;
	import org.apache.hadoop.mapreduce.Reducer;
	import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
	import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
	public class GrowthApp {
	
		public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
		{
		    public void map(LongWritable key, Text value, Context context)
		    {	  
		    	
		       try{
			            String[] str = value.toString().split("\t");
			          	String job_title=str[4];
			          	String year=str[7];
			          	
			          	{
			            context.write(new Text(job_title),new Text(year));
			          
		         
		         //context.write(key,value);
		       }
		       }
		       catch(Exception e)
		       {
		          System.out.println(e.getMessage());
		       }
		    }
		 }

		public static class ReduceClass extends Reducer<Text,Text,NullWritable,Text>
		{
			   //private LongWritable result = new LongWritable();
			    TreeMap<Double,Text>topMap=new TreeMap<Double,Text>();
			    public void reduce(Text inkey, Iterable<Text> inval,Context context) throws IOException, InterruptedException {
			     
			      int count2011=0;
			      int count2012=0;
			      int count2013=0;
			      int count2014=0;
			      int count2015=0;
			      int count2016=0;
			      long average=0;
			      long res1=0,res2=0,res3=0,res4=0,res5=0,res6=0;
			      
			    
					
			         for ( Text Val:inval)
			         {   
			        	 String str[]=Val.toString().split(" ");
			             if(str[7].equals("2011"))
			             {
			        	count2011++;
			             }
			             else if(str[7].equals("2012"))
			             {
			            	count2012++;
			             }
			             else if(str[7].equals("2013"))
			             {
			            	 count2013++;
			             }
			             else if(str[7].equals("2014"))
			             {
			            	 count2014++;
			             }
			             else if(str[7].equals("2015"))
			             {
			            	 count2015++;
			             }
			             else if(str[7].equals("2016"))
			             {
			            	 count2016++;
			             }
			         }
			         if(count2011!=0)
			         {
			        	 res1=(long)(count2012-2011)*100/(long)count2011;
			         }
			        	 else
			        	 {
			        		 res1=0;
			        	 }
			         
			        		 
			         
			         if(count2012!=0)
			         {
			        	 res2=(long)(count2013-2012)*100/(long)count2012;
			         }
			         else
			         {
			        	 res2=0;
			         }
			         if(count2013!=0)
			         {
			        	 res3=(long)(count2014-2013)*100/(long)count2013;
			         }
			         else
			         {
			        	 res3=0;
			         }
			         if(count2014!=0)
			         {
			        	 
			        	 res4=(long)(count2015-2014)*100/(long)count2014;
			         }
			         else
			         {
			        	 res4=0;
			         }
			         if(count2015!=0)
			         {
			        	 res5=(long)(count2016-2015)*100/(long)count2015;
			         }
			         else
			         {
			        	 res5=0;
			         }
			         average=(res1+res2+res3+res4+res5)/5;
			         String myvalue=inkey.toString();
			         
			         String resaverage =String.format("%2f",average);
			         String final2011=String.format("%d",count2011);
			         String final2012=String.format("%d",count2012);
			         String final2013=String.format("%d",count2013);
			         String final2014=String.format("%d",count2014);
			         String final2015=String.format("%d",count2015);
			         String final2016=String.format("%d",count2016);
			         
			         String finaloutput=myvalue+"\t"+final2012+"\t"+final2013+"\t"+final2014+"\t"+final2015+"\t"+final2016+"\t"+"resaverage";
			         topMap.put(new Double(average),new Text(myvalue));
			         if(topMap.size()>5)
			         {
			        	 topMap.remove(topMap.firstKey());
			         }
			    }
			    public void cleanup(Context context)throws IOException,InterruptedException
			    {
			    for(Text t:topMap.descendingMap().values())
			    {
			         context.write(NullWritable.get(),new Text(t));
		} 			
			     
			    }
		}
		public static void main(String[] args) throws IOException,ClassNotFoundException,InterruptedException {
		    Configuration conf = new Configuration();
		  
		    Job job = new Job (conf, "question1b");
		    job.setJarByClass(GrowthApp.class);
		    job.setMapperClass(MapClass.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		    //job.setNumReduceTasks(2);
		  job.setMapOutputKeyClass(Text.class);
		   job.setMapOutputValueClass(Text.class);
		    job.setOutputKeyClass(NullWritable.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
	}




