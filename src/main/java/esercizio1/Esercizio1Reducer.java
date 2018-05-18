package esercizio1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Esercizio1Reducer extends
		Reducer<IntWritable, SummaryWritable, IntWritable, Text>{
	
	private Map<Integer,Map<String, IntWritable>> yearMap = new TreeMap<Integer,Map<String, IntWritable>>(); 
	
	@Override
	public void reduce(IntWritable key, Iterable<SummaryWritable> values,
			Context context) throws IOException, InterruptedException {

				//summaryCountMap: map<word, count>
				/*Map<String, IntWritable> summaryCountMap = new TreeMap<String, IntWritable>();
				
				for(SummaryWritable summary : values) {
					int count = summaryCountMap.containsKey(summary.getWord()) ? summaryCountMap.get(summary.getWord()).get() : 0;
					summaryCountMap.put(summary.getWord(), new IntWritable(count + 1));
				}
				
				yearMap.put(key.toString(), summaryCountMap);*/
		
				//summaryCountMap: map<word, 1>
				Map<String, IntWritable> summaryCountMap = new TreeMap<String, IntWritable>();
				//summarySumMap: map<word, listOfCounters>
				Map<String, List<Integer>> summarySumMap = new TreeMap<String, List<Integer>>();		//key:idProduct, value: List of scores		

				for(SummaryWritable word : values) {
					if(!summarySumMap.containsKey(word.getWord().toString())){
						List<Integer> scores = new ArrayList<Integer>();
						scores.add(word.getCount());
						summarySumMap.put(word.getWord().toString(), scores);
					} else {
						summarySumMap.get(word.getWord().toString()).add(word.getCount());
					}
				}

				for(String word : summarySumMap.keySet()){
					List<Integer> count = summarySumMap.get(word);
					summaryCountMap.put(word, new IntWritable(count.size()));
				}
				
				yearMap.put(key.get(), summaryCountMap);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
        for(Integer singledata : yearMap.keySet()){
			Map<String, IntWritable> sortedMap = sortByValues(yearMap.get(singledata));			
			
			int counter = 0;
			String summaryCount = "";

			for (String word : sortedMap.keySet()) {
				if (counter++ == 10) {
					break;
				}
				summaryCount = word + " " + sortedMap.get(word);

				context.write(new IntWritable(singledata), new Text(summaryCount));			
			}

		}
	}
	
	/*
	 * sorts the map by values. Taken from slides
	 */
	@SuppressWarnings("rawtypes")
	private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
		List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

		Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {

			@SuppressWarnings("unchecked")
			public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}
		});

		//LinkedHashMap will keep the keys in the order they are inserted
		//which is currently sorted on natural ordering
		Map<K, V> sortedMap = new LinkedHashMap<K, V>();

		for (Map.Entry<K, V> entry : entries) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}

		return sortedMap;
	}
}
