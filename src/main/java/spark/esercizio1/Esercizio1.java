package spark.esercizio1;

import java.util.Collections;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;

import com.google.common.collect.Lists;

import esercizio1.Constants;
import scala.Tuple2;

public class Esercizio1 {
	private String inputFile;

	/**
	 * Costruttore.
	 * @param inputPath: Stringa con il path del file di input
	 */
	public Esercizio1(String inputPath) {
		this.inputFile = inputPath;
	}

	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("ERROR: Usage: Esercizio1 <inputPath> <outputPath>");
			System.exit(1);
		}

		SparkConf conf = new SparkConf().setAppName("Esercizio1");

		JavaSparkContext sc = new JavaSparkContext(conf);

		//Tempo di inizio del job
		long start = System.currentTimeMillis();

		//path file input e cartella di output
		String inputfile = args[0];
		String outputfile = args[1];

		Esercizio1 esercizio1 = new Esercizio1(inputfile);

		//Risultato del job
		JavaPairRDD<String, Iterable<String>> finalResult = esercizio1.executeJob(sc);

		//coalesce salva il risultato in un unico file
		finalResult.coalesce(1).saveAsTextFile(outputfile);
		//finalResult.saveAsTextFile(outputfile);

		System.out.println("Job Finished in "
				+ (System.currentTimeMillis() - start) / 1000.0
				+ " seconds");

		sc.stop();
	}

	/**
	 * readData: legge i dati dal file in input. Ritorna un JavaRDD di stringhe.
	 * @param JavaSparkContext sc
	 * @return JavaRDD
	 */
	private JavaRDD<String> readData(JavaSparkContext sc) {
		return sc.textFile(this.inputFile);						//crea un RDD dal file di input
	}

	/**
	 * computeIntermediateResult: genera un JavaPairRDD con chiave uguale a year+word e contatore
	 * @param JavaSparkContext sc
	 * @return JavaPairRDD
	 */
	private JavaPairRDD<String, Integer> computeIntermediateResult(JavaSparkContext sc){
		JavaRDD<String> data = readData(sc);
		String tokens = "[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']";

		//key=year+word value=1
		JavaPairRDD<String, Integer> pairs = data
				.filter(s -> {
					String[] line = s.split(",");

					String year = line[Constants.TIME].trim();
					String summary = line[Constants.SUMMARY];
					String cleanSummary = !summary.isEmpty()? summary.replaceAll(tokens, " ").trim() : summary;
					return line.length == 10 && !year.isEmpty() && !cleanSummary.isEmpty() && !year.equals("Time");
				})
				.mapToPair(s -> {
					String[] line = s.split(",");

					String year = line[Constants.TIME];
					String summary = line[Constants.SUMMARY];
					String cleanSummary = !summary.isEmpty()? summary.replaceAll(tokens, " ") : summary;
					String[] words = cleanSummary.split(" ");
					String yearClean = year.split("-")[0];
					for (String word : words) {
						String key = yearClean + " " + word.trim();
						return new Tuple2<String, Integer>(key,1);
					}
					return new Tuple2<String, Integer>("",0);
				});

		//key=year+word , values=1
		JavaPairRDD<String, Iterable<Integer>> shuffled = pairs.groupByKey();				

		//viene fatto il count per ogni chiave "year+word"
		JavaPairRDD<String, Integer> yearWordCount = shuffled.mapValues(values -> {			
			List<Integer> list = Lists.newArrayList(values.iterator());
			return list.size();		
		}).sortByKey();	

		return yearWordCount;
	}


	/**
	 * executeJob: esegue il job generando un nuovo JavaPairRDD con chiave pari all'anno (YYYY) e  
	 * valore lista delle 10 parole del campo summary con frequenza maggiore.
	 * @param JavaSparkContext sc
	 * @return JavaPairRDD<String, Iterable<String>>
	 */
	public JavaPairRDD<String, Iterable<String>> executeJob(JavaSparkContext sc){

		JavaPairRDD<String, Integer> yearWordCount = computeIntermediateResult(sc);

		//key=year, Value=count+" "+word
		JavaPairRDD<String, String> result = yearWordCount.mapToPair(tuple -> {
			String key = tuple._1();
			String[] keySplit = key.split(" ");
			Integer count = tuple._2;

			String year = keySplit[0];
			String word = keySplit.length == 2 ? keySplit[1] : "";
			return new Tuple2<String, String>(year, String.valueOf(count)+"-"+word);
		});

		JavaPairRDD<String, Iterable<String>> orderedResult = result.groupByKey().sortByKey()
				.mapValues(countWord -> {
					List<String> list = Lists.newArrayList(countWord.iterator());
					Collections.sort(list);
					Collections.reverse(list);
					//vengono prese solo le prime 10 word più frequenti
					if(list.size() > 10)
						return list.subList(0, 10);
					return list;
				});

		return orderedResult;
	}
}