package evaluation;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class InfixesEvalMapper extends MapReduceBase implements Mapper<Text, Text, Text, VIntWritable> {
	
	
	/**
	 * almost an identity mapper, but in reverse order
	 * this will group input by entities
	 * 
	 */
	public void map(Text key, Text value,
			OutputCollector<Text, VIntWritable> output, Reporter reporter) throws IOException {

		VIntWritable infixHash = new VIntWritable(key.toString().hashCode());		
		output.collect(value, infixHash); //value is did;;;entityHash, key is infix		
	}

}
