package spark.esercizio2;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;

import com.google.common.collect.Lists;

import esercizio1.Constants;
import scala.Tuple2;

public class Esercizio2 {
	private String inputFile;

	/**
	 * Costruttore.
	 * @param inputPath: Stringa con il path del file di input
	 */
	public Esercizio2(String inputPath) {
		this.inputFile = inputPath;
	}

	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("ERROR: Usage: Esercizio2 <inputPath> <outputPath>");
			System.exit(1);
		}

		SparkConf conf = new SparkConf().setAppName("Esercizio2");

		JavaSparkContext sc = new JavaSparkContext(conf);

		//Tempo di inizio del job
		long start = System.currentTimeMillis();

		//path file input e cartella di output
		String inputfile = args[0];
		String outputfile = args[1];

		Esercizio2 esercizio2 = new Esercizio2(inputfile);

		//Risultato del job
		JavaPairRDD<String, Iterable<String>> finalResult = esercizio2.executeJob(sc);

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
		return sc.textFile(this.inputFile);
	}

	/**
	 * computeIntermediateResult: genera un JavaPairRDD con chiave uguale a productId+year e score del prodotto
	 * @param JavaSparkContext sc
	 * @return JavaPairRDD
	 */
	private JavaPairRDD<String, Double> computeIntermediateResult(JavaSparkContext sc){
		JavaRDD<String> data = readData(sc);

		//key=productId+year , values=score
		JavaPairRDD<String, Integer> pairs = data.mapToPair(s -> {
			String[] line = s.split(",");

			String product = line[Constants.PRODUCT_ID];
			String year = line[Constants.TIME];
			if(!year.isEmpty() && !product.isEmpty()) {
				int yearClean = year.split("-").length > 2 ?  Integer.valueOf(year.split("-")[0]) : 0;
				int score = yearClean > 0 ? Integer.valueOf(line[Constants.SCORE].trim()):-1;

				if(yearClean >= 2003 && yearClean <= 2012  && product != null && !product.isEmpty() && score>-1) {
					String key = product + " " + String.valueOf(yearClean);
					return new Tuple2<String, Integer>(key,score);
				}
			}
			return new Tuple2<String, Integer>("null",-1);
		});


		//key=year+word , values=lista of scores
		JavaPairRDD<String, Iterable<Integer>> shuffled = pairs.groupByKey()
				.filter(tupla -> {  //Filtra i prodotti con score negativo
					HashSet<Integer> products = new HashSet<Integer>();
					for(Integer product : tupla._2()){
						if(product > -1) {
							products.add(product);
						}
					}
					return products.size() > 0;
				});

		//viene fatta la media di tutti gli score per ogni chiave "productId-year"
		JavaPairRDD<String, Double> productYearAvg = shuffled
				.mapValues(scores -> {			
					Double count = 0.0;
					Double sum = 0.0;
					for (Integer integer : scores) {
						sum += Double.valueOf(integer);
						count = count+1.;
					}
					//if(count>0.) {
						return sum/count;
					//}
					//return 0.0;
				}).sortByKey();

		return productYearAvg;

	}


	/**
	 * executeJob: esegue il job generando un nuovo JavaPairRDD con chiave pari al productId e  
	 * valore la media degli score del prodotto per ogni anno compreso tra 2003 e 2012
	 * @param JavaSparkContext sc
	 * @return JavaPairRDD<String, Iterable<String>>
	 */
	public JavaPairRDD<String, Iterable<String>> executeJob(JavaSparkContext sc){

		JavaPairRDD<String, Double> productYearAvg = computeIntermediateResult(sc);

		//key=year, Value=productId+" "+mediaScores
		JavaPairRDD<String, String> result = productYearAvg.mapToPair(tuple -> {
			String key = tuple._1();
			String[] keySplit = key.split(" ");
			Double avg = tuple._2;

			String year = keySplit.length == 2 ? keySplit[1] : "";
			String productId = keySplit.length == 2 ? keySplit[0] : "";
			return new Tuple2<String, String>(productId, year+"-"+String.valueOf(avg));
		});

		JavaPairRDD<String, Iterable<String>> orderedResult = result.groupByKey().sortByKey()
				.mapValues(countWord -> {
					List<String> list = Lists.newArrayList(countWord.iterator());
					Collections.sort(list);
					Collections.reverse(list);

					return list;
				});

		return orderedResult;
	}
}