package infixExtractionNew;

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

public class SuffixExtractionReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	
	Map<String, Set<String>> suffixes;
		
	/**
	 * @param _key cluster###last token of infix 
	 * @param values list of <infix \t entity(hashCode)> having this infix
	 			* @see old_value: list of infix\tURI from the same cluster with the same last token
	 * output:
	 * 	key: infix (with suffix removed)
	 *  value: dID;;;an entity having this infix (hashCode)
	 */
	public void reduce(Text _key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		Map<String, String[]> infixes = new HashMap<>();
		Map<String, Set<String>> infixEntities = new HashMap<>();
		suffixes = new HashMap<>();
		//System.out.println(_key);
		String lastToken = _key.toString().split("###")[1];		

		String reverseLastToken = new StringBuilder(lastToken).reverse().toString(); //do the same as for the infix extraction
		
		//System.out.println("finding the suffix of "+_key);		
		while (values.hasNext()) {			
			String[] value = values.next().toString().split("\t");
			String infix = value[0];	
			String entity = value[1]; //this is a hashCode
			
			if (lastToken.equalsIgnoreCase(infix) || lastToken.length() == infix.length()) {
				output.collect(new Text(infix), new Text(entity));
				continue;
			}
			
			if (infix.endsWith(".jpg")) {
				output.collect(new Text(infix.substring(0, infix.lastIndexOf(".jpg"))), new Text(entity));
				continue;
			}	
			if (infix.endsWith(".png")) {
				output.collect(new Text(infix.substring(0, infix.lastIndexOf(".png"))), new Text(entity));
				continue;
			}			
			

			reporter.setStatus("Reducing the suffix of:"+infix);
			
			if (infix.matches(".*[\\W]+$")) {		           
				infix = infix.substring(0,infix.length()-1);		           
		    }
			
			//do this again, since the infix has changed
			if (lastToken.equalsIgnoreCase(infix) || lastToken.length() == infix.length()) {				
				output.collect(new Text(infix), new Text(entity));
				continue;
			}
						
			String reverseInfix = new StringBuilder(infix).reverse().toString(); //do the same as for the infix extraction
			
			
			//String[] tokenizedSuffixes = reverseInfix.substring(reverseLastToken.length()).split("[\\./#]");
			//String[] tokenizedSuffixes = reverseInfix.substring(reverseLastToken.length()).split("[\\W_]+");
			String[] tokenizedSuffixes = reverseInfix.substring(reverseLastToken.length()+1).split("[\\./#]+");
			
			if (tokenizedSuffixes.length <= 1) { 
				output.collect(new Text(infix), new Text(entity));
				continue;
			} else if (tokenizedSuffixes[0].equals("")){ 
				output.collect(new Text(infix), new Text(entity));
				continue;
			}
			
			infixes.put(reverseInfix, tokenizedSuffixes);
			Set<String> entities = infixEntities.get(reverseInfix);
			if (entities == null) {
				entities = new HashSet<>();
			}
			entities.add(entity);
			infixEntities.put(reverseInfix, entities);
			//find the set of (distinct) next tokens			
			String suffixBuilder = reverseLastToken;
			
	//		System.out.println("reverseInfix:"+reverseInfix);
			//System.out.println("reverseInfix:"+reverseInfix+":"+tokenizedSuffixes.length);
	//		System.out.println(Arrays.toString(tokenizedSuffixes));
									
			for (int i = 0; i < tokenizedSuffixes.length; ++i) {
				if (tokenizedSuffixes[i].equals("")) break;
	//			System.out.println("suffixBuilder:"+suffixBuilder);				
				suffixBuilder += reverseInfix.charAt(suffixBuilder.length())+tokenizedSuffixes[i];	
		//		System.out.println("newsuffixBuilder:"+suffixBuilder);
				Set<String> distinctNextTokens;
				if (suffixes.containsKey(suffixBuilder)) {
					distinctNextTokens = suffixes.get(suffixBuilder);
				} else {
					distinctNextTokens = new HashSet<>();							
				}
				if (i == tokenizedSuffixes.length - 2 && reverseInfix.charAt(suffixBuilder.length()) == '.') {
					break;
				} else if (i+1 < tokenizedSuffixes.length) {
					distinctNextTokens.add(tokenizedSuffixes[i+1]);						
				} 
				suffixes.put(suffixBuilder, distinctNextTokens);
			}		
		}	
	
		reporter.setStatus("Stored all values. Now checking all possible suffixes.");
		
		for (String reverseInfix : infixes.keySet()) {
			int maxNextTokens = 0;			
			String reverseSuffix = "";
			String[] tokenizedSuffixes = infixes.get(reverseInfix);
			reporter.progress();
			String suffixBuilder = reverseLastToken;
			for (String candidate : tokenizedSuffixes) {
				if (candidate.equals("")) break;
				suffixBuilder += reverseInfix.charAt(suffixBuilder.length())+candidate;
				if (!suffixes.containsKey(suffixBuilder)) continue;
				int currNextTokensSize = suffixes.get(suffixBuilder).size();
				if (currNextTokensSize >= maxNextTokens) {
					maxNextTokens = currNextTokensSize;			
					if (reverseInfix.length() > suffixBuilder.length()) {
						reverseSuffix = reverseInfix.substring(suffixBuilder.length()); //+1 to skip the special character
					} else {
						continue; //infix is null
					}
				}
			}
			String initialReverseInfix = reverseInfix;
			reverseSuffix = reverseSuffix.replaceFirst("/$", ""); //if it finishes with /
			reverseSuffix = reverseSuffix.replaceFirst("\\.[a-z0-9]{1,6}$", ""); //if it finishes with .text remove it			
//			String entityToEmit = "";		
			
//			entityToEmit = infixEntities.get(initialReverseInfix);
			
			Set<String> entitiesToEmit = infixEntities.get(initialReverseInfix);
			
			String finalInfix = new StringBuilder(reverseSuffix).reverse().toString();
			
			if (!finalInfix.equals("")) {					
				//System.out.println("changed infix\t\t:"+new StringBuilder(initialReverseInfix).reverse());
				//System.out.println("into\t\t\t:"+new StringBuilder(reverseSuffix).reverse());	
				if (!Character.isLetterOrDigit(finalInfix.charAt(0)) && finalInfix.length() > 1) {
					finalInfix = finalInfix.substring(1);
				}
				if (!Character.isLetterOrDigit(finalInfix.charAt(finalInfix.length()-1)) && finalInfix.length() > 1) {
					finalInfix = finalInfix.substring(0, finalInfix.length()-1);
				}
				for (String entity:entitiesToEmit) {
					output.collect(new Text(finalInfix), new Text(entity));
				}
			} else {
				String initialInfix = new StringBuilder(initialReverseInfix).reverse().toString();
				if (!Character.isLetterOrDigit(initialInfix.charAt(0)) && initialInfix.length() > 1) {
					initialInfix = initialInfix.substring(1);
				}
				if (!Character.isLetterOrDigit(initialInfix.charAt(initialInfix.length()-1)) && initialInfix.length() > 1) {
					initialInfix = initialInfix.substring(0, initialInfix.length()-1);
				}
				for (String entity:entitiesToEmit) {
					output.collect(new Text(initialInfix), new Text(entity)); //set the domain as infix
				}
				
			}
		}
	}

}
