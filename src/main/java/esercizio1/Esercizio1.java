package esercizio1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Esercizio1 {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 2) {
            System.err.println("Usage: Esercizio1 <in> <out>");
            System.exit(2);
        }
        
        Job job = Job.getInstance(conf);
        job.setJobName("Esercizio1");

		job.setJarByClass(Esercizio1.class);
		
		job.setMapperClass(Esercizio1Mapper.class);
		job.setReducerClass(Esercizio1Reducer.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(SummaryWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		long startTime = System.currentTimeMillis();
		
        job.waitForCompletion(true);
        
        System.out.println("Job Finished in " 
        		+ (System.currentTimeMillis() - startTime) / 1000.0
        		+ " seconds");
	}
}
