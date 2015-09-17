package infixExtractionNew;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

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
	 * value: the whole URI###dID;;;entity's hashCode
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
		reporter.setStatus("values are split");
		//String[] elements = entity.split("###");
		if (elements.length < 2 || elements.length % 2 != 0) {
			reporter.incrCounter(InputData.MALFORMED_PAIRS, 1);
			System.out.println("Malformed: "+s);
			return;
		}
		
		String[] subject = s.split(";;;"); //the first part is the datasource id
		int entityHash = subject[1].hashCode();
		reporter.setStatus("subject is split");
		if (Tools.isURI(subject[1])) { //not a blank node
			String subjectPrefix = getPrefix(subject[1]);
			//System.out.println("The prefix of "+subject[1]+" is "+subjectPrefix+". key:"+key);			
			output.collect(new Text(subjectPrefix), new Text(subject[1]+"###"+subject[0]+";;;"+entityHash));
			reporter.incrCounter(InputData.SUBJECT_INFIX, 1);
		}
		
		//use this block ONLY IF entities contain predicates in their values
		String[] rawValues = new String[elements.length/2];
		for (int i =1, j = 0; i < elements.length; i = i + 2) {
			rawValues[j++] = elements[i];
		}
		reporter.setStatus("got correct values");

		
		//Set<String> values = new HashSet<>(Arrays.asList(rawValues)); // to remove duplicates
		Set<String> values = new HashSet<>(100); // to remove duplicates
		for (String val: rawValues) {	
			if (Tools.isURI(val)) {
				values.add(val);
			} else {			
				val = val.replaceAll("[^a-z0-9 ]+"," "); //remove special characters, keep white space
				reporter.progress();
				val = val.replaceAll("[ ]+", " "); //keep only one white space if more exist
				String [] vals = val.split(" ");
				reporter.progress();
				values.addAll(Arrays.asList(vals)); //to remove duplicate tokens
			}
			if (values.size() > 100) break; //TODO: change parameter			
		}
		
		int counter = 0;
		for (String val: values) {
			if (counter++ == 100) break;
			if (Tools.isURI(val)) {
				//System.out.println("The prefix of "+val+" is "+getPrefix(val)+". key:"+key);
				output.collect(new Text(getPrefix(val)), new Text(val+"###"+subject[0]+";;;"+entityHash));
			}
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
		String[] tokens = URI.split("[\\W_]+");
		if (tokens.length <= 2) {
			return URI;
		}	
		
		if (tokens[1].equalsIgnoreCase("www")) {
			return tokens[1]+URI.charAt(7+tokens[1].length())+tokens[2];
		} else if (tokens[1].equalsIgnoreCase("news")) {
			return tokens[1]+URI.charAt(7+tokens[1].length())+tokens[2];
		} else if (tokens[1].equalsIgnoreCase("data")) {
			return tokens[1]+URI.charAt(7+tokens[1].length())+tokens[2];
		} else if (tokens[1].equalsIgnoreCase("en")) {
			return tokens[1]+URI.charAt(7+tokens[1].length())+tokens[2];
		} else {
			return tokens[1]; //tokens[0] is always http			
		}
	}

}
