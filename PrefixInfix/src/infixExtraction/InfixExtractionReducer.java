package infixExtraction;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class InfixExtractionReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	Map<String, Set<String>> prefixes;
	//Map<String, Set<String>> infixes;
	
	static enum InputData {NOT_VALID_URI, TOO_LONG_URI};
	static enum OutputData {DBPEDIA_URI, UPLOAD_URI, WIKIPEDIA_URI};
	
	private final int MAX_URI_LENGTH = 150;
	
	public void configure (JobConf conf) {
		prefixes = new HashMap<>();
		//infixes = new HashMap<>();
	}	
	
	/**
	 * input key: second token of a URI (after http)
	 * value: list of URIs having this token as second
	 * output:
	 * 	key: URI
	 *  value: Infix of the URI \t cluster
	 */
	public void reduce(Text _key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		Map<String, String[]> URIs = new HashMap<>();
		//System.out.println("finding the prefix of "+_key);
		
		reporter.setStatus("Storing the values of key:"+_key);		
		
		while (values.hasNext()) {		
			String URI = (String) values.next().toString();		
			if (URI.length() < 8 + _key.toString().length()) {
				reporter.incrCounter(InputData.NOT_VALID_URI, 1);
				//System.err.println("URI:"+URI+" key:"+_key);
				continue;				
			} 			
			if (URI.length() > MAX_URI_LENGTH) {
				reporter.incrCounter(InputData.TOO_LONG_URI, 1);
				//System.err.println("Too long URI:"+URI);
				continue;
			}	
			
			//special case for dbpedia
			String dbpedia = "dbpedia";
			String dbpediaPrefix = "dbpedia.org/resource";
			if (_key.toString().equals(dbpedia)) { //this "if" saves a hell of a time
				if (URI.toString().length() > 8+dbpediaPrefix.length()) {
					reporter.incrCounter(OutputData.DBPEDIA_URI, 1);
					String dbpediaInfix = URI.substring(8+dbpediaPrefix.length());//8 for "http://" and "/"
					output.collect(new Text(URI), new Text(dbpediaInfix+"\t"+dbpedia));
					continue;
				} else if (URI.toString().length() > 14){
					reporter.incrCounter(OutputData.DBPEDIA_URI, 1);
					String dbpediaInfix = URI.substring(14);//14 for "http://dbpedia." //TODO: check/fix
					output.collect(new Text(URI), new Text(dbpediaInfix+"\t"+dbpedia));
					continue;
				}				
			}
			
			//special case for upload
			String upload = "upload";
			String uploadPrefix = "upload.wikimedia.org/wikipedia/commons/thumb";
			if (_key.toString().equals(upload)) { //this "if" saves a hell of a time
				if (URI.toString().length() > 8+uploadPrefix.length()) {
					reporter.incrCounter(OutputData.UPLOAD_URI, 1);
					String uploadInfix = URI.substring(8+uploadPrefix.length());//8 for "http://" and "/"
					output.collect(new Text(URI), new Text(uploadInfix+"\t"+upload));
					continue;
				} else if (URI.toString().length() > 15){
					reporter.incrCounter(OutputData.UPLOAD_URI, 1);
					String uploadInfix = URI.substring(15);//15 for "http://upload." //FIXME: check/fix
					output.collect(new Text(URI), new Text(uploadInfix+"\t"+upload));
					continue;
				}				
			}
			
			//special case for wikipedia
			String wikipedia = "en";
			String wikipediaPrefix = "en.wikipedia.org/wiki";
			if (_key.toString().equals(wikipedia)) { //this "if" saves a hell of a time
				if (URI.toString().length() > 8+wikipediaPrefix.length()) {
					reporter.incrCounter(OutputData.WIKIPEDIA_URI, 1);
					String wikipediaInfix = URI.substring(8+wikipediaPrefix.length());//8 for "http://" and "/"
					output.collect(new Text(URI), new Text(wikipediaInfix+"\t"+wikipedia));
					continue;
				} else if (URI.toString().length() > 24){
					reporter.incrCounter(OutputData.WIKIPEDIA_URI, 1);
					String wikipediaInfix = URI.substring(24);//24 for "http://en.wikipedia.org/" //FIXME: check/fix
					output.collect(new Text(URI), new Text(wikipediaInfix+"\t"+wikipedia));
					continue;
				}				
			}
						
			String[] tokenizedPrefixes = URI.substring(8+(_key.toString().length())).split("[\\./#]");	//7 is the length of the string "http://" +1 for the special character
			URIs.put(URI, tokenizedPrefixes);
			//find the set of (distinct) next tokens			
			String prefixBuilder = "http://"+_key;			
			
			
			for (int i = 0; i < tokenizedPrefixes.length; ++i) {
				prefixBuilder += URI.charAt(prefixBuilder.length())+tokenizedPrefixes[i];				
				Set<String> distinctNextTokens;
				if (prefixes.containsKey(prefixBuilder)) {
					distinctNextTokens = prefixes.get(prefixBuilder);
				} else {
					distinctNextTokens = new HashSet<>();							
				}
				if (i == tokenizedPrefixes.length - 2 && URI.charAt(prefixBuilder.length()) == '.') {
					break;
				} else if (i+1 < tokenizedPrefixes.length) {
					distinctNextTokens.add(tokenizedPrefixes[i+1]);						
				} 
				prefixes.put(prefixBuilder, distinctNextTokens);
			}		
		}
		
		reporter.setStatus("Stored all values. Now checking all possible prefixes.");
		
		for (String URI : URIs.keySet()) {
			int maxNextTokens = 0;			
			String infix = "";
			String[] tokenizedPrefixes = URIs.get(URI);
			String prefixBuilder = "http://"+_key;
			for (String candidate : tokenizedPrefixes) {
				prefixBuilder += URI.charAt(prefixBuilder.length())+candidate;
				if (!prefixes.containsKey(prefixBuilder)) continue;
				int currNextTokensSize = prefixes.get(prefixBuilder).size();
				if (currNextTokensSize >= maxNextTokens) {
					maxNextTokens = currNextTokensSize;			
					if (URI.length() > prefixBuilder.length()) {
						infix = URI.substring(prefixBuilder.length()+1); //+1 to skip the special character
					} else {
						continue; //infix is null
					}
				}
			}
			
			infix = infix.replaceFirst("/$", ""); //if it finishes with /
			infix = infix.replaceFirst("\\.[a-z0-9]{1,6}$", ""); //if it finishes with .text remove it
			if (!infix.equals("")) {
				output.collect(new Text(URI), new Text(infix+"\t"+_key));
			} else {
				output.collect(new Text(URI), new Text(_key+"\t"+_key)); //set the domain as infix
			}
		}
	}

}
