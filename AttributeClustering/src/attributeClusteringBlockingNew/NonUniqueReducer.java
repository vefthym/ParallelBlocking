package attributeClusteringBlockingNew;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class NonUniqueReducer extends MapReduceBase implements Reducer<Text, VIntWritable, Text, Text> {
	
	static enum OutputData {CLEAN_COMPARISONS, DIRTY_COMPARISONS};
	
	public void reduce(Text _key, Iterator<VIntWritable> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {	
		long D1counter = 0;
		long D2counter = 0;
		
		StringBuilder contents = new StringBuilder("[");		
		
		while (values.hasNext()) {
			int eid = values.next().get();			
			if (eid >= 0 ) {				
				D1counter++;
			} else {
				D2counter++;
			}
			contents.append(eid).append(", ");
		}
		long comp = D1counter*D2counter;
		//if (comp > 0 ) {
		if (D1counter+D2counter > 1) {
			output.collect(_key, new Text(contents.substring(0, contents.length()-2)+"]"));
			//clean-clean
		//	reporter.incrCounter(OutputData.CLEAN_COMPARISONS, D1counter*D2counter);
		}
	}

}
