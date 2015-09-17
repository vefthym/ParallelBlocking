package infixExtraction;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import util.Tools;

public class InfixExtractionMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {

	static enum InputData { NOT_AN_ENTITY, MALFORMED_PAIRS, SUBJECT_INFIX};
	
	
	//used for Prefix-Infix(-Suffix)
	
	/** 
	 * @param key an entity id (subject of a triple) in the form of datasourceID;;;URI
	 * @param value the data of an entity (attribute-value pairs) separated by "###"
	 * @param output the output (key,value) pairs, where
	 * key: second token (after http) of a URI (tokenized by special characters)
	 * value: the whole URI and the input key (entity identifier), separated by ###
	 */
	public void map(Text key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		/* uncompressed
		//String inputEntity = value.toString();
		//String []entityParams = inputEntity.split("\t");
		
		if (entityParams.length != 2) {
			reporter.incrCounter(InputData.NOT_AN_ENTITY, 1);
			System.err.println("Malformed input:"+inputEntity);
			return;
		}
		
		//String s = entityParams[0];		
		//String[] rawValues = entityParams[1].split("###");		  
		*/
		String s = key.toString();
		String[] elements = value.toString().split("###");
		
		//String[] elements = entity.split("###");
		if (elements.length < 2 || elements.length % 2 != 0) {
			reporter.incrCounter(InputData.MALFORMED_PAIRS, 1);
			System.out.println("Malformed: "+s);
			return;
		}
		
		String[] subject = s.split(";;;"); //the first part is the datasource id
		if (Tools.isURI(subject[1])) { //not a blank node
			output.collect(new Text(getPrefix(subject[1])), new Text(subject[1]));
			reporter.incrCounter(InputData.SUBJECT_INFIX, 1);
		}
		
		//use this block ONLY IF entities contain predicates in their values
		String[] rawValues = new String[elements.length/2];
		for (int i =1, j = 0; i < elements.length; i = i + 2) {
			rawValues[j++] = elements[i];
		}
				
		for (String val: rawValues) {
			//FIXME: keep only the infixes within the first 100 "tokens" (1 URI counts as a token)?
			if (Tools.isURI(val)) {
				output.collect(new Text(getPrefix(val)), new Text(val));
			}
		}
	}
	
	
	/**
	 * returns the second token of a URI, or the whole URI 
	 * The first is always "http". 
	 * The third could be part of the infix.
	 * @param URI the URI to be parsed
	 * @return the second token of a URI, if URI's has > 2 tokens, or the whole URI, else
	 */
	private String getPrefix(String URI) {
		/*
		String dbpediaPrefix = "dbpedia.org/resource";
		if (URI.startsWith("http://"+dbpediaPrefix)) {				
			return dbpediaPrefix; //this is neccessary, as otherwise, the reduce task of dbpedia would do all the work
		}
		*/
		String[] tokens = URI.split("[\\W_]+");
		if (tokens.length <= 2) {
			return URI;
		}	
		
		if (!tokens[1].equalsIgnoreCase("www")) {
			return tokens[1]; //tokens[0] is always http
		} else {
			return tokens[1]+URI.charAt(7+tokens[1].length())+tokens[2];
		}
	}

}
