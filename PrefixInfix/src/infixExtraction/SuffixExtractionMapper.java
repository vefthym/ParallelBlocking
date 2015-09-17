package infixExtraction;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class SuffixExtractionMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {

	static enum InputData { MALFORMED_INPUT, NULL_INFIX, NULL_LAST_INFIX_TOKEN};
	
	
	//used for Prefix-Infix(-Suffix)
	
	/** 
	 * input value: URI \t infix \t cluster
	 * output:
	 * key: cluster_last token of infix 
	 * value: URI
	 */
	public void map(Text key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		/* uncompressed
		String line = value.toString();
		String[] UriInfixCluster = line.split("\t");		
		
		if (UriInfixCluster.length != 3) { 	
			reporter.incrCounter(InputData.MALFORMED_INPUT, 1);
			System.err.println("Malformed input: "+value);
			return;
		}
		String URI = UriInfixCluster[0]; //if it finishes with /		
		String infix = UriInfixCluster[1];		
		*/
		String URI = key.toString();
		String infix = value.toString().split("\t")[0];
		
		if (infix == null || infix.equals("")) { 	
			reporter.incrCounter(InputData.NULL_INFIX, 1);
			System.out.println("Null Infix: "+value);
			return;
		}
		
		//String cluster = UriInfixCluster[2];
		String cluster = value.toString().split("\t")[1];
		
		//String[] tokenizedInfix = infix.split("[^a-z0-9-]+");
		String[] tokenizedInfix = infix.split("[^a-z0-9-]+");
		if (tokenizedInfix.length == 0) {
			reporter.incrCounter(InputData.NULL_LAST_INFIX_TOKEN, 1);
			System.out.println("Last token of infix is null:"+infix);
			return;
		}
		String lastToken = tokenizedInfix[tokenizedInfix.length-1];
		output.collect(new Text(cluster+"_"+lastToken), new Text(infix+"\t"+URI));
	}

}
