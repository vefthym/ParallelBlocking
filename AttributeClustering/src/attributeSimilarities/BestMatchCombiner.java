package attributeSimilarities;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class BestMatchCombiner extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	
	static enum InputData { NOT_A_VALID_SCORE };
	
	public void reduce(Text _key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {	
		double bestScore = -1; //even if bestMatch is 0, this qualifies. 
		String bestPredicate = "";		
		//System.out.println("Combiner input: "+_key);
		while (values.hasNext()) {
			 String value = values.next().toString();			 
			 //System.out.println(value);
			 String[] valueParams = value.split(";;;");
			 if (valueParams.length != 3) {
				reporter.incrCounter(InputData.NOT_A_VALID_SCORE, 1);
				System.out.println("Not a valid score: "+value);
				return;
			 }
			 double score = Double.parseDouble(valueParams[2]);
			 if (score > bestScore) {
				 bestScore = score;
				 bestPredicate = new String(value);				 
			 }
		}		
		output.collect(_key, new Text(bestPredicate));
		//System.out.println("combiner output: "+_key+"\t"+bestPredicate);
	}
	

}
