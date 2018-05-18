package esercizio2;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import esercizio1.Constants;

public class Esercizio2Mapper extends
Mapper<LongWritable, Text, Text, ProductScoresWritable>{

	private Text productId = new Text();
	private ProductScoresWritable productScores = new ProductScoresWritable();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] line = value.toString().split(",");	

		if(line.length == 10) {
			String product = line[Constants.PRODUCT_ID];
			int year = getYear(line[Constants.TIME]);
			int score = isInteger(line[Constants.SCORE]) ? Integer.parseInt(line[Constants.SCORE]) : -1;

			if(year >= 2003 && year <= 2012  && product != null && !product.isEmpty() && score>-1) {
				productId.set(product);
				productScores.setScore(score);
				productScores.setYear(year);

				context.write(productId, productScores);
			}
		}
	}

	/*
	 * Trasforma unixformat in string  che rappresenta l'anno
	 */
	private int getYear(String input){ 
		if(input != null && !input.isEmpty() && isInteger(input)){
			int unixformat = Integer.parseInt(input);
			Date date = new Date((long)unixformat*1000);
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(date);
			int year = calendar.get(Calendar.YEAR);
			return year;
		}
		return 0;
	}

	public static boolean isInteger(String s) {
		try { 
			Integer.parseInt(s); 
		} catch(NumberFormatException e) { 
			return false; 
		} catch(NullPointerException e) {
			return false;
		}
		// only got here if we didn't return false
		return true;
	}

}