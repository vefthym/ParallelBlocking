package prefixInfixBlockingNewIds;

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

public class Combiner extends MapReduceBase implements Reducer<Text, VIntWritable, Text, VIntWritable> {
	
	public void reduce(Text _key, Iterator<VIntWritable> values,
			OutputCollector<Text, VIntWritable> output, Reporter reporter) throws IOException {
		Set<VIntWritable> buf = new HashSet<>();
		reporter.setStatus("Combining...");
		while (values.hasNext()) {
			reporter.progress();
			VIntWritable val = values.next();			
			if (buf.add(val)) { //add returns true if val did not previously exist
				output.collect(_key, val);				
			}
		}
	}

}
