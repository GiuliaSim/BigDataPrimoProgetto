package esercizio1;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class Esercizio1Mapper extends
Mapper<LongWritable, Text, IntWritable, SummaryWritable>{

	private IntWritable year = new IntWritable();
	private SummaryWritable summaryCount = new SummaryWritable();
	private String tokens = "[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']";

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] line = value.toString().split(",");	

		if(line.length == 10) {
			String summary = line[Constants.SUMMARY];
			int date = getYear(line[Constants.TIME]);
			if(summary != null && !summary.isEmpty() && date != 0 && date >= 1999) {
				String cleanSummary = summary.toLowerCase().replaceAll(tokens, " ");
				StringTokenizer itr = new StringTokenizer(cleanSummary);

				year.set(date);
				while (itr.hasMoreTokens()) {
					String word = itr.nextToken().trim();
					summaryCount.setWord(word);
					summaryCount.setCount(1);
					context.write(year, summaryCount);
				}
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
