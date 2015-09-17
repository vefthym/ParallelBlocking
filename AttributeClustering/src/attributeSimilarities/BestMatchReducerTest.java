package attributeSimilarities;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class BestMatchReducerTest extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	
	/**
	 * emit the key with the value having the highest score
	 * also output 2 extra files in the HDFS:
	 */
	public void reduce(Text _key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {	

		double bestScore = -1; //even if bestMatch is 0, this qualifies
		String bestPredicate = "";		
		while (values.hasNext()) {
			 String value = values.next().toString();
			 reporter.progress();
			 String[] valueParams = value.split(";;;");
			 reporter.progress();
			 double score = Double.parseDouble(valueParams[2]);
			 if (score > bestScore) {
				 bestScore = score;  
				 bestPredicate = new String(valueParams[0]+";;;"+valueParams[1]);
			 }
		}	
		
		if (bestScore > 0) {		
			output.collect(_key, new Text(bestPredicate)); //output in part-00000
		} 		
		
	}

}
