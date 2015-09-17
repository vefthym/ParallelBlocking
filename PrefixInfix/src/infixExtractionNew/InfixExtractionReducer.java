package infixExtractionNew;

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
	
	static enum InputData {NOT_VALID_URI, TOO_LONG_URI};
	static enum OutputData {DBPEDIA_URI, FREEBASE_URI, UPLOAD_URI, WIKIPEDIA_URI, BOOKS_URI, YOUTUBE_URI, W3_URI, BBC_URI};
	
	private final int MAX_URI_LENGTH = 150;
	
	/**
	 * @param _key second token of a URI (after http)
	 * @param values list of URI###dID;;;entity (hashCode) having this token as second
	 * @param output the output (key,value) pairs, where
	 * 	key: infix of a URI
	 *  value: cluster \t dID;;;an entity (hashCode) having this URI
	 */
	public void reduce(Text _key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		Map<String, String[]> URIs = new HashMap<>();
		Map<String, Set<String>> URIentities = new HashMap<>();
		prefixes = new HashMap<>();			
		reporter.setStatus("Reducing:"+_key);		
		
		while (values.hasNext()) {		
			String value = values.next().toString();			
			String[] valueElems = value.split("###");
			String URI = valueElems[0];
			String entity = valueElems[1];			
			
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
			if (_key.toString().equals(dbpedia)) { //this "if" saves a hell of a time
				String dbpediaPrefix = "dbpedia.org/resource";
				if (URI.toString().length() > 8+dbpediaPrefix.length()) {
					reporter.incrCounter(OutputData.DBPEDIA_URI, 1);
					String dbpediaInfix = URI.substring(8+dbpediaPrefix.length());//8 for "http://" and "/"
					output.collect(new Text(dbpediaInfix), new Text(dbpedia+"\t"+entity));					
					continue;
				} else if (URI.toString().length() > 14){
					reporter.incrCounter(OutputData.DBPEDIA_URI, 1);
					String dbpediaInfix = URI.substring(14);//14 for "http://dbpedia." //TODO: check/fix
					output.collect(new Text(dbpediaInfix), new Text(dbpedia+"\t"+entity));					
					continue;
				}				
			}
					
			//special case for freebase
			String freebase = "rdf";			
			if (_key.toString().equals(freebase)) { //this "if" saves a hell of a time
				String freebasePrefix = "rdf.freebase.com/ns/m.";
				if (URI.toString().length() > 8+freebasePrefix.length()) {
					reporter.incrCounter(OutputData.FREEBASE_URI, 1);
					String freebaseInfix = URI.substring(8+freebasePrefix.length());//8 for "http://" and "/"
					output.collect(new Text(freebaseInfix), new Text(freebase+"\t"+entity));
					continue;
				} else if (URI.toString().length() > 23){
					reporter.incrCounter(OutputData.FREEBASE_URI, 1);
					String freebaseInfix = URI.substring(23);//23 for "http://rdf.freebase.com/" //TODO: check/fix
					output.collect(new Text(freebaseInfix), new Text(freebase+"\t"+entity));
					continue;
				}				
			}
			freebase = "www.freebase";			
			if (_key.toString().equals(freebase)) { //this "if" saves a hell of a time
				String freebasePrefix2 = "www.freebase.com/view/m/";
				if (URI.toString().length() > 8+freebasePrefix2.length()) {
					reporter.incrCounter(OutputData.FREEBASE_URI, 1);
					String freebaseInfix = URI.substring(8+freebasePrefix2.length());//8 for "http://" and "/"
					output.collect(new Text(freebaseInfix), new Text(freebase+"\t"+entity));
					continue;
				} else if (URI.toString().length() > 23){
					reporter.incrCounter(OutputData.FREEBASE_URI, 1);
					String freebaseInfix = URI.substring(23);//23 for "http://www.freebase.com/" //TODO: check/fix
					output.collect(new Text(freebaseInfix), new Text(freebase+"\t"+entity));
					continue;
				}				
			}
			
			//special case for upload
			String upload = "upload";			
			if (_key.toString().equals(upload)) { //this "if" saves a hell of a time
				String uploadPrefix = "upload.wikimedia.org/wikipedia/commons/thumb";
				if (URI.toString().length() > 8+uploadPrefix.length()) {
					reporter.incrCounter(OutputData.UPLOAD_URI, 1);
					String uploadInfix = URI.substring(8+uploadPrefix.length());//8 for "http://" and "/"
					output.collect(new Text(uploadInfix), new Text(upload+"\t"+entity));
					continue;
				} else if (URI.toString().length() > 15){
					reporter.incrCounter(OutputData.UPLOAD_URI, 1);
					String uploadInfix = URI.substring(15);//15 for "http://upload." //FIXME: check/fix
					output.collect(new Text(uploadInfix), new Text(upload+"\t"+entity));
					continue;
				}				
			}
			
			//special case for wikipedia
			String wikipedia = "en.wikipedia";			
			if (_key.toString().equals(wikipedia)) { //this "if" saves a hell of a time
				String wikipediaPrefix = "en.wikipedia.org/wiki";
				if (URI.toString().length() > 8+wikipediaPrefix.length()) {
					reporter.incrCounter(OutputData.WIKIPEDIA_URI, 1);
					String wikipediaInfix = URI.substring(8+wikipediaPrefix.length());//8 for "http://" and "/"
					output.collect(new Text(wikipediaInfix), new Text(wikipedia+"\t"+entity));
					continue;
				} else if (URI.toString().length() > 24){
					reporter.incrCounter(OutputData.WIKIPEDIA_URI, 1);
					String wikipediaInfix = URI.substring(24);//24 for "http://en.wikipedia.org/" //FIXME: check/fix
					output.collect(new Text(wikipediaInfix), new Text(wikipedia+"\t"+entity));
					continue;
				}				
			}
			
			//special case for books			
			String books = "books";			
			if (_key.toString().equals(books)) { //this "if" saves a hell of a time
				String booksPrefix = "books.google.com/books?id=";
				if (URI.toString().length() > 8+booksPrefix.length()) {
					reporter.incrCounter(OutputData.BOOKS_URI, 1);
					String booksInfix = URI.substring(8+booksPrefix.length());//8 for "http://" and "/"
					output.collect(new Text(booksInfix), new Text(books+"\t"+entity));
					continue;
				} else if (URI.toString().length() > 24){
					reporter.incrCounter(OutputData.BOOKS_URI, 1);
					String booksInfix = URI.substring(24);//24 for "http://books.google.com/" //FIXME: check/fix
					output.collect(new Text(booksInfix), new Text(books+"\t"+entity));
					continue;
				}				
			}
			
			//special case for youtube	
			String youtube = "www.youtube";			
			if (_key.toString().equals(youtube)) { //this "if" saves a hell of a time
				String youtubePrefix = "www.youtube.com/";
				if (URI.toString().length() > 8+youtubePrefix.length()) {
					reporter.incrCounter(OutputData.YOUTUBE_URI, 1);
					String youtubeInfix = URI.substring(8+youtubePrefix.length());//8 for "http://" and "/"
					output.collect(new Text(youtubeInfix), new Text(youtube+"\t"+entity));
					continue;
				}
			}
			
			//special case for w3	
			String w3 = "www.w3";			
			if (_key.toString().equals(w3)) { //this "if" saves a hell of a time
				String w3Prefix = "www.w3.org/";
				if (URI.toString().length() > 8+w3Prefix.length()) {
					reporter.incrCounter(OutputData.W3_URI, 1);
					String w3Infix = URI.substring(8+w3Prefix.length());//8 for "http://" and "/"
					output.collect(new Text(w3Infix), new Text(w3+"\t"+entity));
					continue;
				} 			
			}
			
			//special case for bbc
			String bbc = "www.bbc";				
			if (_key.toString().equals(bbc)) { //this "if" saves a hell of a time
				String bbcPrefix = "www.bbc.co.uk/music/artists/";
				if (URI.toString().startsWith("http://"+bbcPrefix, 0)) {
					reporter.incrCounter(OutputData.BBC_URI, 1);
					String bbcInfix = URI.substring(8+bbcPrefix.length());//8 for "http://" and "/"
					output.collect(new Text(bbcInfix), new Text(bbc+"\t"+entity));
					continue;
				} 			
			}						
			
			
			String[] tokenizedPrefixes = URI.substring(8+(_key.toString().length())).split("[\\./#]");	//7 is the length of the string "http://" +1 for the special character
			reporter.progress();
			URIs.put(URI, tokenizedPrefixes);	
			Set<String> entities = URIentities.get(URI);
			if (entities == null) {
				entities = new HashSet<>();
			} 
			entities.add(entity);
			URIentities.put(URI, entities);
			//find the set of (distinct) next tokens			
			String prefixBuilder = "http://"+_key;			
			
			
			for (int i = 0; i < tokenizedPrefixes.length; ++i) {
				reporter.progress();
				prefixBuilder += URI.charAt(prefixBuilder.length())+tokenizedPrefixes[i];				
				Set<String> distinctNextTokens = prefixes.get(prefixBuilder);				
				if (distinctNextTokens == null) {
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
		
		reporter.setStatus("Stored all values of "+_key);
		
		for (String URI : URIs.keySet()) {
			reporter.setStatus("finding the infixes for "+_key);
			reporter.progress();
			int maxNextTokens = 0;			
			String infix = "";			
			String prefixBuilder = "http://"+_key;
			for (String candidate : URIs.get(URI)) {
				reporter.progress();
				prefixBuilder += URI.charAt(prefixBuilder.length())+candidate;
				if (!prefixes.containsKey(prefixBuilder)) continue;
				int currNextTokensSize = prefixes.get(prefixBuilder).size();
				reporter.progress();
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
			reporter.setStatus("Ready to reduce for "+_key);
			//String entitiesToEmit = "";
			if (!infix.equals("")) {	
				for (String entity: URIentities.get(URI)) {
					//entitiesToEmit += entity+"###";
					output.collect(new Text(infix), new Text(_key+"\t"+entity));					
				}
			} else {
				for (String entity: URIentities.get(URI)) {					
					output.collect(new Text(_key), new Text(_key+"\t"+entity)); //set the domain as infix					
				}
			}
			
			/*			
			reporter.setStatus("Reducing for "+_key);
			entitiesToEmit = entitiesToEmit.substring(0, entitiesToEmit.length()-3); //-3 for the last ###
			if (!infix.equals("")) {					
				output.collect(new Text(infix), new Text(_key+"\t"+entitiesToEmit));
			} else {
				output.collect(new Text(_key), new Text(_key+"\t"+entitiesToEmit)); //set the domain as infix
			}
			*/
		}
	}

}
