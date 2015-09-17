package infixExtractionNewIds;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class InfixExtractionCombiner extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	
	/**
	 * @param _key second token of a URI (after http)
	 * @param values list of URI###entity having this token as second
	 * @param output the output (key,value) pairs, discarding duplicates of the input 	
	 */
	public void reduce(Text _key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		List<String> buf = new ArrayList<>();
		reporter.setStatus("Combining...");
		while (values.hasNext()) {
			String val = values.next().toString();
			reporter.progress();
			if (!buf.contains(val)) {
				output.collect(_key, new Text(val));
				buf.add(val);
			}
		}
	}

}
