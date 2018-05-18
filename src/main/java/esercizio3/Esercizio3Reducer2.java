package esercizio3;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class Esercizio3Reducer2 extends Reducer<Text, Text, Text, IntWritable>{

	@Override
	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {	

		HashSet<String> users = new HashSet<String>();

		for(Text user : values){
			users.add(user.toString());		//add sul set non aggiunge duplicati
		}

		if(users.size()>=1){
			int count = users.size();
			context.write(key, new IntWritable(count));
		}
	}

}
