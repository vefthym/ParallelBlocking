package infixExtractionNewIds;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class SuffixExtractionReducer extends MapReduceBase implements Reducer<Text, Text, Text, VIntWritable> {
	
	Map<String, Set<String>> suffixes;
	
	Text infixToEmit = new Text();
	VIntWritable eid = new VIntWritable();
		
	/**
	 * @param _key cluster###last token of infix 
	 * @param values list of <infix \t entityId having this infix>
	 * output:
	 * 	key: infix (with suffix removed)
	 *  value: an entity having this infix (id)
	 */
	public void reduce(Text _key, Iterator<Text> values,
			OutputCollector<Text, VIntWritable> output, Reporter reporter) throws IOException {
		
		if (_key.toString().equals("dbp")|| _key.toString().equals("fb")) { //special cases
			while (values.hasNext()) {
				String[] value = values.next().toString().split("\t");				
				infixToEmit.set(value[0]);
				String entity = value[1];
				if (entity.startsWith("#")) entity = entity.substring(1); //don't know why this appears
				if (StringUtils.isNumeric(entity) && !entity.isEmpty()) {
					eid.set(Integer.parseInt(entity));
				} else {
					continue;
				}
				output.collect(infixToEmit, eid);
			}
			return;
		}
		
		
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
			String entity = value[1]; //this is an entity id			
			if (entity.startsWith("#")) entity = entity.substring(1); //don't know why this appears
			if (StringUtils.isNumeric(entity) && !entity.isEmpty()) {
				eid.set(Integer.parseInt(entity));
			} else {
				continue;
			}
			
			if (lastToken.equalsIgnoreCase(infix) || lastToken.length() == infix.length()) {
				infixToEmit.set(infix);				
				output.collect(infixToEmit, eid);
				continue;
			}
			
			if (infix.endsWith(".jpg")) {
				infixToEmit.set(infix.substring(0, infix.lastIndexOf(".jpg")));				
				output.collect(infixToEmit, eid);
				continue;
			}	
			if (infix.endsWith(".png")) {
				infixToEmit.set(infix.substring(0, infix.lastIndexOf(".png")));
				output.collect(infixToEmit, eid);
				continue;
			}			
			

			reporter.setStatus("Reducing the suffix of:"+infix);
			
			if (infix.matches(".*[\\W]+$")) {		           
				infix = infix.substring(0,infix.length()-1);		           
		    }
			
			//do this again, since the infix has changed
			if (lastToken.equalsIgnoreCase(infix) || lastToken.length() == infix.length()) {
				infixToEmit.set(infix);				
				output.collect(infixToEmit, eid);
				continue;
			}
						
			String reverseInfix = new StringBuilder(infix).reverse().toString(); //do the same as for the infix extraction
			
			
			//String[] tokenizedSuffixes = reverseInfix.substring(reverseLastToken.length()).split("[\\./#]");
			//String[] tokenizedSuffixes = reverseInfix.substring(reverseLastToken.length()).split("[\\W_]+");
			String[] tokenizedSuffixes = reverseInfix.substring(reverseLastToken.length()+1).split("[\\./#]+");
			
			if (tokenizedSuffixes.length <= 1) { 
				infixToEmit.set(infix);
				output.collect(infixToEmit, eid);
				continue;
			} else if (tokenizedSuffixes[0].equals("")){ 
				infixToEmit.set(infix);		
				output.collect(infixToEmit, eid);
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
				infixToEmit.set(finalInfix);
				for (String entity:entitiesToEmit) {
					eid.set(Integer.parseInt(entity));
					output.collect(infixToEmit, eid);
				}
			} else {
				String initialInfix = new StringBuilder(initialReverseInfix).reverse().toString();
				if (!Character.isLetterOrDigit(initialInfix.charAt(0)) && initialInfix.length() > 1) {
					initialInfix = initialInfix.substring(1);
				}
				if (!Character.isLetterOrDigit(initialInfix.charAt(initialInfix.length()-1)) && initialInfix.length() > 1) {
					initialInfix = initialInfix.substring(0, initialInfix.length()-1);
				}
				infixToEmit.set(initialInfix);
				for (String entity:entitiesToEmit) {
					eid.set(Integer.parseInt(entity));
					output.collect(infixToEmit, eid); //set the domain as infix					
				}
				
			}
		}
	}

}
