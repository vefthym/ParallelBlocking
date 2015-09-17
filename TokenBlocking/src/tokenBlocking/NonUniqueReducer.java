package tokenBlocking;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class NonUniqueReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	
	static enum OutputData {CLEAN_COMPARISONS, DIRTY_COMPARISONS};
	
	public void reduce(Text _key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {	
		long D1counter = 0;
		long D2counter = 0;
		
		while (values.hasNext()) {
			Text value = values.next();			
			if (value.toString().charAt(0) == '0') { //dbpedia URIs start with "0;;;URI"				
				D1counter++;
			} else {
				D2counter++;
			}
			output.collect(_key, value);
		}
		//clean-clean
		reporter.incrCounter(OutputData.CLEAN_COMPARISONS, D1counter*D2counter);
		
		//dirty
		long sum = D1counter + D2counter;
		reporter.incrCounter(OutputData.DIRTY_COMPARISONS, (sum*(sum-1))/2);
	}

}
