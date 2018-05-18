package esercizio2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Esercizio2Reducer extends
Reducer<Text, ProductScoresWritable, Text, Text>{

	//private Map<IntWritable,Map<IntWritable, DoubleWritable>> yearMap = new TreeMap<IntWritable,Map<IntWritable, DoubleWritable>>(); 
	
	@Override
	public void reduce(Text key, Iterable<ProductScoresWritable> values,
			Context context) throws IOException, InterruptedException {

		//prodScoreMap: map<idProd, listOfScores>
		Map<Integer, List<Integer>> prodScoreMap = new TreeMap<Integer, List<Integer>>();  //key:year, value: List of scores		

		
		for(ProductScoresWritable product : values) {
			if(!prodScoreMap.containsKey(product.getYear())){
				List<Integer> scores = new ArrayList<Integer>();
				scores.add(product.getScore());
				prodScoreMap.put(product.getYear(), scores);
			} else {
				prodScoreMap.get(product.getYear()).add(product.getScore());
			}
		}
		
		for(Integer year : prodScoreMap.keySet()){
			List<Integer> scores = prodScoreMap.get(year);

			String productAvgScore = year + " " + Double.toString(avg(scores));
			context.write(key, new Text(productAvgScore));	
		}
	}
	
	private double avg(List<Integer> number) {
		double count = 0;
		double sum = 0;
		for (Integer integer : number) {
			sum += integer;
			count++;
		}
		return (sum/count);
	}

	//@Override
	//protected void cleanup(Context context) throws IOException, InterruptedException {
	//}

}
