package tokenBlocking;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class DummyReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	
	/**
	 * output: candidate pairs for this block
	 */
	public void reduce(Text _key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {		
		reporter.setStatus("Reducing block "+_key);
		
		
		//find unary blocks
		/*
		if (values.hasNext()) {
			values.next();
			if (!values.hasNext()) {
				output.collect(new Text(_key), new Text("1"));
			}
		}
		*/
		//find stopwords
		long counter = 0;
		while (values.hasNext()) {	
			values.next();
			counter ++;				
			reporter.progress();
			if (counter > 300000) {		
				output.collect(new Text(_key), new Text(""));
				return;
				//System.out.println("token:"+_key+" Entities:"+counter);
			}
		}
		/*
		if (counter > 300000) {		
			output.collect(new Text(_key), new Text(Long.toString(counter)));			
			//System.out.println("token:"+_key+" Entities:"+counter);
		}*/
		
		
	}

}
