package prefixInfixBlockingNew;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class PrefixInfixReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	static enum OutputData {CLEAN_COMPARISONS, DIRTY_COMPARISONS};
	/**
	 * It just removes duplicate entries (same entity more than once in the same block)
	 * If no duplicates exist, it can be replaced by IdentityReducer
	 */
	public void reduce(Text _key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		
		long D1counter = 0;
		long D2counter = 0;
		
		Set<VIntWritable> buf = new HashSet<>();
		reporter.setStatus("Reducing: "+_key);
		while (values.hasNext()) {
			reporter.progress();
			String val = values.next().toString();
			//VIntWritable vIntVal = new VIntWritable(Integer.parseInt(val.split(";;;")[1])); //D2 and D3
			VIntWritable vIntVal = new VIntWritable(val.hashCode()); //D2 and D3
			if (!buf.contains(vIntVal)) {
				output.collect(_key, new Text(val));
				if (val.charAt(0) == '0') {			//corresponding to BTCdbpedia	
					D1counter++;
				} else {
					D2counter++;
				}
				buf.add(vIntVal);
			}
		}
		
		//clean-clean
		reporter.incrCounter(OutputData.CLEAN_COMPARISONS, D1counter*D2counter);
		
		//dirty
		long sum = D1counter + D2counter;
		reporter.incrCounter(OutputData.DIRTY_COMPARISONS, (sum*(sum-1))/2);
	}

}
