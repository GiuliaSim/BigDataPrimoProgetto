package esercizio3;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Esercizio3Reducer1 extends Reducer<Text, Text, Text, Text>{

	@Override
	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {	

		//key: utente
		//values: lista di prodotti recensiti dall'utente
		
		List<String> products = new LinkedList<>(); 
		for(Text product : values){
			products.add(product.toString());
		}


		for(String product1 : products){			
			for(String product2: products){
				if(!(product1.equals(product2))){
					if(product1.compareTo(product2) < 0) { //Ordina e evita coppie già esistenti simmetriche
						context.write(new Text(product1+" "+product2), new Text(key));
					}
				}

			}
		}
	}
}
