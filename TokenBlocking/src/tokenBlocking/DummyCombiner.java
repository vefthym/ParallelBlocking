package tokenBlocking;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class DummyCombiner extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	
	/**
	 * output: candidate pairs for this block
	 */
	public void reduce(Text _key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {		
		reporter.setStatus("Reducing block "+_key);
		
		
		//find unary blocks
		if (values.hasNext()) {
			String s = values.next().toString();
			if (values.hasNext()) {
				return;
			}			
			output.collect(new Text(_key), new Text(s));
		}		
	}

}
