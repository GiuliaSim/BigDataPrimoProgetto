package esercizio3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import esercizio1.Constants;

public class Esercizio3Mapper1 extends
Mapper<LongWritable, Text, Text, Text>{

	private Text productId = new Text();
	private Text userId = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] line = value.toString().split(",");	

		if(line.length == 10) {
			String product = line[Constants.PRODUCT_ID];
			String user = line[Constants.USER_ID];

			if(product != null && !product.isEmpty() && user != null && !user.isEmpty()) {
				productId.set(product);
				userId.set(user);

				context.write(userId, productId);
			}
		}
	}
}

