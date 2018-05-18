package esercizio3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Esercizio3Mapper2 extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] line = value.toString().split("\t");
		
		String products = line[0]; 
		String userId = line[1];
		
		context.write(new Text(products), new Text(userId));
		
	}
}