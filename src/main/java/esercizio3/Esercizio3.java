package esercizio3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Esercizio3 {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        String OUTPUT_PrimoStep = "/user/primoStep";
        
        if (otherArgs.length != 2) {
            System.err.println("Usage: Esercizio3 <in> <out>");
            System.exit(2);
        }
        
        Job job1 = Job.getInstance(conf);
        job1.setJobName("Esercizio3 primo job");

        job1.setJarByClass(Esercizio3.class);
		
		job1.setMapperClass(Esercizio3Mapper1.class);
		job1.setReducerClass(Esercizio3Reducer1.class);

		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(OUTPUT_PrimoStep));

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		long startTime = System.currentTimeMillis();
		
        job1.waitForCompletion(true);
        
        System.out.println("Job1 Finished in " 
        		+ (System.currentTimeMillis() - startTime) / 1000.0
        		+ " seconds");
        
        Job job2 = Job.getInstance(conf);
        job2.setJobName("Esercizio3 secondo job");

        job2.setJarByClass(Esercizio3.class);
		
		job2.setMapperClass(Esercizio3Mapper2.class);
		job2.setReducerClass(Esercizio3Reducer2.class);

		FileInputFormat.addInputPath(job2, new Path(OUTPUT_PrimoStep));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		
        job2.waitForCompletion(true);
        
        System.out.println("Job2 Finished in " 
        		+ (System.currentTimeMillis() - startTime) / 1000.0
        		+ " seconds");
	}

}
