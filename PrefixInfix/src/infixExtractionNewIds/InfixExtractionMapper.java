package infixExtractionNewIds;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import util.Tools;


public class InfixExtractionMapper extends MapReduceBase implements Mapper<VIntWritable, Text, Text, Text> {
	
	static enum InputData { NOT_AN_ENTITY, MALFORMED_PAIRS, SUBJECT_INFIX};
	
	Text keyToEmit = new Text();
	Text valueToEmit = new Text();
	
	/** 
	 * @param key an entity id (subject of a triple) in the form of datasourceIDURI
	 * @param value the data of an entity (attribute-value pairs) separated by "###"
	 * @param output the output (key,value) pairs, where: <br/>
	 * key: second token (after http) of a URI (tokenized by special characters) <br/>
	 * value: the whole URI###eid (input key)
	 */
	public void map(VIntWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
				
		String[] elements = value.toString().split("###");
		reporter.setStatus("values are split");
		if (elements.length < 2 || elements.length % 2 != 0) {
			reporter.incrCounter(InputData.MALFORMED_PAIRS, 1);
			System.out.println("Malformed: "+key);
			return;
		}		
		
		//use this block ONLY IF entities contain predicates in their values
		String[] rawValues = new String[elements.length/2];
		for (int i =1, j = 0; i < elements.length; i = i + 2) {
			rawValues[j++] = elements[i];
		}
		reporter.setStatus("got correct values");

		Set<String> values = new HashSet<>(); // to remove duplicates
		for (String val: rawValues) {
			if (val.length() > 1) {
				if (val.startsWith("<") && Tools.isURI(val.substring(1)) && val.contains(">")) {					
					values.add(URLDecoder.decode(val.substring(1,val.lastIndexOf(">")),"UTF-8"));					
				}
			}
		}
			
		for (String val: values) {		
			keyToEmit.set(getPrefix(val));
			valueToEmit.set(val+Tools.DELIMITER+key);
			output.collect(keyToEmit, valueToEmit);			
		}
		reporter.setStatus("mapping finished, ready to combine");
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
		if (URI.startsWith("dbp:")) {
			return "dbp";
		} else if (URI.startsWith("dbo:")) {
			return "dbo";
		} else if (URI.startsWith("fb:")) {
			return "freebase";
		}
		String[] tokens = URI.split("[\\W_]+");
		if (tokens.length <= 2) {
			return URI;
		}	
		
		if (tokens[1].equalsIgnoreCase("www") ||
			tokens[1].equalsIgnoreCase("news") ||
			tokens[1].equalsIgnoreCase("data") ||
			tokens[1].equalsIgnoreCase("en")) {
			return URI.substring(URI.indexOf(tokens[1]), URI.indexOf(tokens[2],tokens[0].length()+tokens[1].length())+tokens[2].length());
		} else {
			return tokens[1]; //tokens[0] is always http			
		}
	}

}
