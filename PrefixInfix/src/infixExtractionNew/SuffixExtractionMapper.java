package infixExtractionNew;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class SuffixExtractionMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {

	static enum InputData { MALFORMED_INPUT, NULL_INFIX, NULL_LAST_INFIX_TOKEN, ALL_SUFFIX};
	
	
	//used for Prefix-Infix(-Suffix)
	
	/** 
	 * @param key infix
	 * @param value cluster +"\t"+ an entity (hashCode) having this infix
	 * @param output: (key, value) pairs, where 
	 * key: cluster###last token of infix 
	 * value: infix \t dID;;;an entity having this infix (the same as the input entity) (hashCode)
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
		String infix = key.toString();
		String[] valueElems = value.toString().split("\t");		
				
		if (infix == null || infix.equals("")) { 	
			reporter.incrCounter(InputData.NULL_INFIX, 1);
			//System.out.println("Null Infix: "+value);
			return;
		}
		
		if (valueElems.length != 2) {
			reporter.incrCounter(InputData.MALFORMED_INPUT, 1);
			return;
		}
		
		//String cluster = UriInfixCluster[2];
		String cluster = valueElems[0];
		reporter.progress();
		
		//TODO: check if it speeds up the process
		if (cluster.equals("dbp") || cluster.equals("fb")) { //already processed
			output.collect(new Text(cluster), new Text(infix+"\t"+valueElems[1]));
			return;
		}
		
		
		//String[] tokenizedInfix = infix.split("[^a-z0-9-]+");
		if (infix.matches(".*[\\W]$")) {		           
			infix = infix.substring(0,infix.length()-1);		           
	    }		
		if (infix.endsWith("/all")) { //special case
			infix = infix.substring(0, infix.lastIndexOf("/all"));	
			reporter.incrCounter(InputData.ALL_SUFFIX, 1);
		}
		String[] tokenizedInfix = infix.split("[\\./#]+");
		reporter.progress();
		if (tokenizedInfix.length == 0 || infix.equals("")) {
			reporter.incrCounter(InputData.NULL_LAST_INFIX_TOKEN, 1);
			//System.out.println("Last token of infix is null:"+infix);
			return;
		}		
		String lastToken = tokenizedInfix[tokenizedInfix.length-1];
				
		
		output.collect(new Text(cluster+"###"+lastToken), new Text(infix+"\t"+valueElems[1]));
		//System.out.println("output key:"+cluster+"###"+lastToken);
	}

}
