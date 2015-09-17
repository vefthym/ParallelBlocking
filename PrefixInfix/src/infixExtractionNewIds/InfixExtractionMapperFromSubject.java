package infixExtractionNewIds;

import java.io.IOException;
import java.net.URLDecoder;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import util.Tools;


public class InfixExtractionMapperFromSubject extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

	
	/** 
	 * @param key lineNumber (this is from TextInputFormat
	 * @param value a URI subject and is eid mapping
	 * @param output the output (key,value) pairs, where: <br/>
	 * key: second token (after http) of a URI (tokenized by special characters) <br/>
	 * value: the whole URI###eid (input key)
	 */
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		
		String mapping = value.toString();
		String[] parts = mapping.split("\t");
        parts[0] = parts[0].substring(1,parts[0].lastIndexOf(">")).replaceAll("\\*", "\\\\*");
        int eid = Integer.parseInt(parts[1]);
        
		String subject = URLDecoder.decode(parts[0],"UTF-8");		
		
		reporter.setStatus("subject is split");
		
		String subjectPrefix = getPrefix(subject);				
		output.collect(new Text(subjectPrefix), new Text(subject+Tools.DELIMITER+eid));
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
