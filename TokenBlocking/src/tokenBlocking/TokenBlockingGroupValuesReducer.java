package tokenBlocking;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class TokenBlockingGroupValuesReducer extends MapReduceBase implements Reducer<Text, VLongWritable, Text, Text> {
		
	//static enum OutputData {PURGED_BLOCKS}; //Reduce input groups - reduce output records
	
	public void reduce(Text _key, Iterator<VLongWritable> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		
		final int PURGING_THRESHOLD = 10000; //10K (^2 = 100M) TODO: set threshold
		
		reporter.setStatus("reducing "+_key);		
		StringBuffer toEmit = new StringBuffer();
		boolean unary = true;
		int sizeCounter = 0;
		while (values.hasNext()) {			
			toEmit.append(","+values.next().get());
			reporter.progress();			
			if (values.hasNext()) { unary = false; }
			if (++sizeCounter > PURGING_THRESHOLD) { return; }
		}
		if (!unary) {
			output.collect(_key, new Text(toEmit.toString().substring(1))); //substring(1) to remove the first ','
		}
	}

}
