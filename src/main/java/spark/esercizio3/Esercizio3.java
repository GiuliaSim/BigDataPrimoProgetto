package spark.esercizio3;

import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Lists;

import esercizio1.Constants;
import scala.Tuple2;

public class Esercizio3 {
	private String inputFile;

	/**
	 * Costruttore.
	 * @param file: Stringa con il path del file di input
	 */
	public Esercizio3(String inputPath) {
		this.inputFile = inputPath;
	}

	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("ERROR: Usage: Esercizio3 <inputPath> <outputPath>");
			System.exit(1);
		}

		SparkConf conf = new SparkConf().setAppName("Esercizio3");

		JavaSparkContext sc = new JavaSparkContext(conf);

		//Tempo di inizio del job
		long start = System.currentTimeMillis();

		//path file input e output
		String inputfile = args[0];
		String outputfile = args[1];

		Esercizio3 esercizio3 = new Esercizio3(inputfile);

		//Risultato del job
		JavaPairRDD<String, Integer> finalResult = esercizio3.executeJob(sc);

		//coalesce salva il risultato in un unico file
		finalResult.coalesce(1).saveAsTextFile(outputfile);
		//finalResult.saveAsTextFile(outputfile);

		System.out.println("Job Finished in "
				+ (System.currentTimeMillis() - start) / 1000.0
				+ " seconds");

		sc.stop();

	}

	/**
	 * readData: legge i dati dal file in input.
	 * @param JavaSparkContext sc
	 * @return JavaPairRDD: key=userId value=lista di prodotti dell'utente
	 */
	private JavaPairRDD<String, Iterable<String>> readData(JavaSparkContext sc) {
		JavaRDD<String> rdd = sc.textFile(this.inputFile);
		JavaPairRDD<String, Iterable<String>> pairRDD = rdd.mapToPair(s -> {
			String[] line = s.split(",");
			String userid = line[Constants.USER_ID];
			String productid = line[Constants.PRODUCT_ID];
			return new Tuple2<String, String>(userid, productid);
		}).groupByKey();

		return pairRDD;	
	}

	/**
	 * Ritorna JavaPairRDD con userId e lista di coppie di prodotti recensiti dall'utente
	 * @param sc: JavaSparkContext
	 * @return JavaPairRDD
	 */
	private JavaPairRDD<String, Iterable<String>> intermediateResult(JavaSparkContext sc) {
		JavaPairRDD<String, Iterable<String>> userProducts = readData(sc);
		JavaPairRDD<String, Iterable<String>> joinedRDD = userProducts.join(userProducts)
				.mapValues(tupla -> {
					List<String> list1 = Lists.newArrayList(tupla._1().iterator()); 
					List<String> list2 = Lists.newArrayList(tupla._2().iterator());
					List<String> result = new ArrayList<String>();
					for(String product1 : list1){
						for(String product2: list2){
							if(!product1.equals(product2)){
								if(product1.compareTo(product2)<0)//(evita duplicati)
									result.add(product1 + "-" + product2);
							}
						}
					}
					return result;
				});
		return joinedRDD;
	}

	/**
	 * Ritorna un JavaPairRDD con coppiaprodotti e count degli utenti in comune.
	 * @param sc
	 * @return JavaPairRDD
	 */
	private JavaPairRDD<String, Integer> executeJob(JavaSparkContext sc) {
		JavaPairRDD<String, Iterable<String>> intermediateResult = intermediateResult(sc)
				.filter(tupla -> {
					HashSet<String> coppie = new HashSet<String>();
					for(String coppia : tupla._2()){
						if(!coppia.isEmpty()) {
							coppie.add(coppia);
						}
					}
					return coppie.size() > 0;
				});

		//ogni elemento è una stringa con <coppiaprodotti>+<userId>
		/*JavaPairRDD<String, Iterable<String>> result = intermediateResult.flatMap(tupla -> {
			List<String> list = new ArrayList<>();		//tupla._2() è la lista delle coppie di prodotti
			for(String s : tupla._2()){ 		//s è la coppia di prodotti
				list.add(s+" "+tupla._1());	    //tupla._1() è l'utente
			}
			return list;
		}).mapToPair(s -> {
			String[] line = s.split(" ");
			String products = line[0];
			String user = line[1];
			return new Tuple2<String, String>(products, user);
		}).groupByKey().sortByKey();*/

		JavaPairRDD<String, Iterable<String>> result = intermediateResult.mapToPair(tupla -> {
			//tupla._2() è la lista delle coppie di prodotti
			for(String s : tupla._2()){ 		//s è la coppia di prodotti
				return new Tuple2<String, String>(s, tupla._1());
			}
			return new Tuple2<String, String>("", "");
		}).groupByKey().sortByKey();


		JavaPairRDD<String, Integer> yearWordCount = result.mapValues(values -> {			
			List<String> list = Lists.newArrayList(values.iterator());
			return list.size();		
		}).sortByKey();	

		return yearWordCount;

	}
}