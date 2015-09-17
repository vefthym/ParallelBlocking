package infixExtractionNewIds;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import util.Tools;

public class InfixExtractionReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	Map<String, Set<String>> prefixes; //key: a prefix, value: the set of next tokens
	
	static enum InputData {NOT_VALID_URI, TOO_LONG_URI};
	static enum OutputData {DBPEDIA_URI, FREEBASE_URI, UPLOAD_URI, WIKIPEDIA_URI, BOOKS_URI, YOUTUBE_URI, W3_URI, BBC_URI};
	
	Text infix = new Text();	
	Text toEmit = new Text();
	
	private final int MAX_URI_LENGTH = 150;
	
	/**
	 * @param _key second token of a URI (after http)
	 * @param values list of URI###entityId having this token as second
	 * @param output the output (key,value) pairs, where
	 * 	key: infix of a URI
	 *  value: cluster \t dID;;;entityId having this URI
	 */
	public void reduce(Text _key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		Map<String, String[]> URIs = new HashMap<>(); //key: all URIs of this cluster, value: all possible prefixes of each URI
		Map<String, Set<String>> URIentities = new HashMap<>(); //the set of entities having this URI
		prefixes = new HashMap<>();			
		reporter.setStatus("Reducing:"+_key);
		
		String keyString = _key.toString();
		
		while (values.hasNext()) {		
			String value = values.next().toString();			
			String[] valueElems = value.split(Tools.DELIMITER);
			final String URI = valueElems[0];
			String entity = valueElems[1];			
			
			if (URI.length() < 3 + keyString.length()) {
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
			String dbpedia = "dbp";			
			if (keyString.equals(dbpedia)) { //this "if" saves a hell of a time
				String dbpediaPrefix = "dbp:";				
				reporter.incrCounter(OutputData.DBPEDIA_URI, 1);
				infix.set(URI.substring(dbpediaPrefix.length()));
				toEmit.set(dbpedia+"\t"+entity);
				output.collect(infix, toEmit);					
				continue;
			}
			String dbpediaOnto = "dbo";			
			if (keyString.equals(dbpediaOnto)) { //this "if" saves a hell of a time
				String dbpediaPrefix = "dbo:";				
				reporter.incrCounter(OutputData.DBPEDIA_URI, 1);
				infix.set(URI.substring(dbpediaPrefix.length()));
				toEmit.set(dbpediaOnto+"\t"+entity);
				output.collect(infix, toEmit);
				continue;
			}
					
			//special case for freebase
			String freebase = "freebase";			
			if (keyString.equals(freebase)) { //this "if" saves a hell of a time
				String freebasePrefix = "fb:";				
				reporter.incrCounter(OutputData.FREEBASE_URI, 1);
				infix.set(URI.substring(freebasePrefix.length()));
				toEmit.set(freebase+"\t"+entity);
				output.collect(infix, toEmit);				
				continue;
			}			
			freebase = "www.freebase";			
			if (keyString.equals(freebase)) { //this "if" saves a hell of a time
				String freebasePrefix2 = "www.freebase.com/view/m/";
				if (URI.length() > 8+freebasePrefix2.length()) {
					reporter.incrCounter(OutputData.FREEBASE_URI, 1);
					infix.set(URI.substring(8+freebasePrefix2.length()));//8 for "http://" and "/"
					toEmit.set(freebase+"\t"+entity);
					output.collect(infix, toEmit);
					continue;
				} else if (URI.length() > 23){
					reporter.incrCounter(OutputData.FREEBASE_URI, 1);
					infix.set(URI.substring(23));//23 for "http://www.freebase.com/" //TODO: check/fix
					toEmit.set(freebase+"\t"+entity);
					output.collect(infix, toEmit);
					continue;
				}				
			}
			
			//special case for upload
			String upload = "upload";			
			if (keyString.equals(upload)) { //this "if" saves a hell of a time
				String uploadPrefix = "upload.wikimedia.org/wikipedia/commons/thumb";
				if (URI.length() > 8+uploadPrefix.length()) {
					reporter.incrCounter(OutputData.UPLOAD_URI, 1);
					infix.set(URI.substring(8+uploadPrefix.length()));//8 for "http://" and "/"
					toEmit.set(upload+"\t"+entity);
					output.collect(infix, toEmit);					
					continue;
				} else if (URI.length() > 15){
					reporter.incrCounter(OutputData.UPLOAD_URI, 1);
					infix.set(URI.substring(15));//15 for "http://upload." //FIXME: check/fix
					toEmit.set(upload+"\t"+entity);
					output.collect(infix, toEmit);
					continue;
				}				
			}
			
			//special case for wikipedia
			String wikipedia = "en.wikipedia";			
			if (keyString.equals(wikipedia)) { //this "if" saves a hell of a time
				String wikipediaPrefix = "en.wikipedia.org/wiki";
				if (URI.length() > 8+wikipediaPrefix.length()) {
					reporter.incrCounter(OutputData.WIKIPEDIA_URI, 1);
					infix.set(URI.substring(8+wikipediaPrefix.length()));//8 for "http://" and "/"
					toEmit.set(wikipedia+"\t"+entity);
					output.collect(infix, toEmit);					
					continue;
				} else if (URI.length() > 24){
					reporter.incrCounter(OutputData.WIKIPEDIA_URI, 1);
					infix.set(URI.substring(24));//24 for "http://en.wikipedia.org/" //FIXME: check/fix
					toEmit.set(wikipedia+"\t"+entity);
					output.collect(infix, toEmit);
					continue;
				}				
			}
			
			//special case for books			
			String books = "books";			
			if (keyString.equals(books)) { //this "if" saves a hell of a time
				String booksPrefix = "books.google.com/books?id=";
				if (URI.length() > 8+booksPrefix.length()) {
					reporter.incrCounter(OutputData.BOOKS_URI, 1);
					infix.set(URI.substring(8+booksPrefix.length()));//8 for "http://" and "/"
					toEmit.set(books+"\t"+entity);
					output.collect(infix, toEmit);					
					continue;
				} else if (URI.length() > 24){
					reporter.incrCounter(OutputData.BOOKS_URI, 1);
					infix.set(URI.substring(24));//24 for "http://books.google.com/" //FIXME: check/fix
					toEmit.set(books+"\t"+entity);
					output.collect(infix, toEmit);
					continue;
				}				
			}
			
			//special case for youtube	
			String youtube = "www.youtube";			
			if (keyString.equals(youtube)) { //this "if" saves a hell of a time
				String youtubePrefix = "www.youtube.com/";
				if (URI.length() > 8+youtubePrefix.length()) {
					reporter.incrCounter(OutputData.YOUTUBE_URI, 1);
					infix.set(URI.substring(8+youtubePrefix.length()));//8 for "http://" and "/"
					toEmit.set(youtube+"\t"+entity);
					output.collect(infix, toEmit);					
					continue;
				}
			}
			
			//special case for w3	
			String w3 = "www.w3";			
			if (keyString.equals(w3)) { //this "if" saves a hell of a time
				String w3Prefix = "www.w3.org/";
				if (URI.length() > 8+w3Prefix.length()) {
					reporter.incrCounter(OutputData.W3_URI, 1);
					infix.set(URI.substring(8+w3Prefix.length()));//8 for "http://" and "/"
					toEmit.set(w3+"\t"+entity);
					output.collect(infix, toEmit);					
					continue;
				} 			
			}
			
			//special case for bbc
			String bbc = "www.bbc";				
			if (keyString.equals(bbc)) { //this "if" saves a hell of a time
				String bbcPrefix = "www.bbc.co.uk/music/artists/";
				if (URI.startsWith("http://"+bbcPrefix, 0)) {
					reporter.incrCounter(OutputData.BBC_URI, 1);
					infix.set(URI.substring(8+bbcPrefix.length()));//8 for "http://" and "/"
					toEmit.set(bbc+"\t"+entity);
					output.collect(infix, toEmit);					
					continue;
				} 			
			}
			
			
			//the set of tokens, starting from the end of the input token (key)
			String[] tokenizedPrefixes = URI.substring(URI.indexOf(keyString)+keyString.length()).split("[\\./#]+"); //'+' for potentially more than one special char	
			reporter.progress();
			
			
			//for all URIs in the cluster do (Line 5.)
			URIs.put(URI, tokenizedPrefixes); //all possible prefixes, other than the input Key
			Set<String> entities = URIentities.get(URI); //the set of entities having this URI
			if (entities == null) {
				entities = new HashSet<>();
			} 
			entities.add(entity);
			URIentities.put(URI, entities);
			
			
			//The first part of the URI, until the keyString (including)
			StringBuilder prefixBuilder = new StringBuilder(
					URI.substring(0, URI.indexOf(keyString))).
					append(keyString);			//...including
			
			//in the first case, the candidate prefix is only the input key
			String prefixBuilderString = prefixBuilder.toString();
			Set<String> distinctNextTokens = prefixes.get(prefixBuilderString);			
			if (distinctNextTokens == null) {
				distinctNextTokens = new HashSet<>();							
			}
			if (tokenizedPrefixes.length > 1) {
				distinctNextTokens.add(tokenizedPrefixes[0]);						
			} 
			prefixes.put(prefixBuilderString, distinctNextTokens);
			
			
			//for all possible prefixes do (Line 6.)
			for (int i = 0; i < tokenizedPrefixes.length-1; ++i) { //the last token cannot be an infix
				if (tokenizedPrefixes[i].trim().isEmpty()) {
					continue;
				}
				reporter.progress();
				prefixBuilderString = prefixBuilder.toString();
				if (!(URI.length() > prefixBuilderString.length())) {
					break;
				}
				
				prefixBuilder.append(URI.substring( //append the delimiters between the tokens
						prefixBuilderString.length(),
						URI.indexOf(tokenizedPrefixes[i],prefixBuilderString.length())) //index of next token, starting from the end of the existing prefix
						).
						append(tokenizedPrefixes[i]); //append the next token
				
				prefixBuilderString = prefixBuilder.toString();
				
				//find the set of (distinct) next tokens T (Line 7.)
				distinctNextTokens = prefixes.get(prefixBuilderString);
				
				if (distinctNextTokens == null) {
					distinctNextTokens = new HashSet<>();							
				}
				/*if (i == tokenizedPrefixes.length - 2 && URI.charAt(prefixBuilderString.length()) == '.') {
					break;
				} else */
				
				distinctNextTokens.add(tokenizedPrefixes[i+1]);						
				 
				prefixes.put(prefixBuilderString, distinctNextTokens); //key: prefix, value: set of nextTokens
			}		
		}
		
		
		
		reporter.setStatus("Stored all values of "+keyString);
		
		
		
		//for all URIs in the cluster do (Line 10.)
		for (String URI : URIs.keySet()) {
			reporter.setStatus("finding the infixes for "+keyString);
			reporter.progress();
			int maxNextTokens = 0;			
			String infixString = "";	
			
			StringBuilder prefixBuilder = new StringBuilder(URI.substring(0, URI.indexOf(keyString))).append(keyString);			
			String prefixBuilderString = prefixBuilder.toString();
			//examine the input key as prefix
			prefixBuilderString = prefixBuilder.toString();
			if (!prefixes.containsKey(prefixBuilderString)) continue;
			int currNextTokensSize = prefixes.get(prefixBuilderString).size();
			reporter.progress();
			if (currNextTokensSize >= maxNextTokens) {
				maxNextTokens = currNextTokensSize;			
				if (URI.length() > prefixBuilderString.length()) {
					infixString = URI.substring(prefixBuilderString.length()+1); //+1 to skip the special character
				} else {
					continue; //infix is null
				}
			}
			
			//examine the rest candidate prefixes
			for (String candidate : URIs.get(URI)) {
				reporter.progress();
				if (!(URI.length() > prefixBuilder.length())) {
					break;
				}
				prefixBuilderString = prefixBuilder.toString();
				
				//set as prefix the one with the largest |T| (Line 11.)
				prefixBuilder.append(URI.substring(
						prefixBuilderString.length(),
						URI.indexOf(candidate,prefixBuilderString.length()))
						).
						append(candidate);				
				
				prefixBuilderString = prefixBuilder.toString();
				if (!prefixes.containsKey(prefixBuilderString)) continue;
				currNextTokensSize = prefixes.get(prefixBuilderString).size();
				reporter.progress();
				if (currNextTokensSize >= maxNextTokens) {
					maxNextTokens = currNextTokensSize;			
					if (URI.length() > prefixBuilderString.length()) {
						infixString = URI.substring(prefixBuilderString.length()+1); //+1 to skip the special character
					} else {
						continue; //infix is null
					}
				}
			}
			
			infixString = infixString.replaceFirst("/$", ""); //if it finishes with /
			infixString = infixString.replaceFirst("\\.[a-z0-9]{1,6}$", ""); //if it finishes with .text remove it
			reporter.setStatus("Ready to reduce for "+_key);
			//String entitiesToEmit = "";
			if (!infixString.isEmpty()) {	
				for (String entity: URIentities.get(URI)) {
					infix.set(infixString);
					toEmit.set(_key+"\t"+entity);
					output.collect(infix, toEmit);					
				}
			} else {
				for (String entity: URIentities.get(URI)) {
					toEmit.set(_key+"\t"+entity);
					output.collect(_key, toEmit); //set the domain as infix					
				}
			}	
		}
	}

}
