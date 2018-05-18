package esercizio2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Esercizio2 {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 2) {
            System.err.println("Usage: Esercizio2 <in> <out>");
            System.exit(2);
        }
        
        Job job = Job.getInstance(conf);
        job.setJobName("Esercizio2");

		job.setJarByClass(Esercizio2.class);
		
		job.setMapperClass(Esercizio2Mapper.class);
		job.setReducerClass(Esercizio2Reducer.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ProductScoresWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		long startTime = System.currentTimeMillis();
		
        job.waitForCompletion(true);
        
        System.out.println("Job Finished in " 
        		+ (System.currentTimeMillis() - startTime) / 1000.0
        		+ " seconds");
	}
}
