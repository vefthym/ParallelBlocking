package infixExtraction;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class SuffixExtractionReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	
	Map<String, Set<String>> suffixes;
		
	/**
	 * input key:  cluster_last token of infix 
	 * value: list of infix\tURI from the same cluster with the same last token
	 * output:
	 * 	key: URI
	 *  value: Infix of the URI \t cluster
	 */
	public void reduce(Text _key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		Map<String, String> URIs = new HashMap<>();		
		suffixes = new HashMap<>();
		String lastToken = _key.toString().split("_")[1];
		
		//System.out.println("finding the suffix of "+_key);
		int counter = 0; //counter for the last token
		while (values.hasNext()) {		
			counter++;
			String[] value = values.next().toString().split("\t");
			String infix = value[0];			
			String URI = value[1];
			
			if (infix.endsWith(".jpg")) {
				output.collect(new Text(URI), new Text(infix.substring(0, infix.lastIndexOf(".jpg"))));
				continue;
			}	
			if (infix.endsWith(".png")) {
				output.collect(new Text(URI), new Text(infix.substring(0, infix.lastIndexOf(".png"))));
				continue;
			}
			
			URIs.put(URI,infix);
						
			reporter.setStatus("Reducing infix: "+infix);
			
			//some URIs end with special chars...
			Pattern p = Pattern.compile("[^a-z0-9 ]+$", Pattern.CASE_INSENSITIVE);
			Matcher m = p.matcher(infix);
			boolean b = m.find();
			int ending = b ? m.end()-m.start() : 0; //how many consecutive special characters?			
			String[] tokenizedInfix = infix.substring(0, infix.length()-ending).split("[^a-z0-9-]+");

			//String[] tokenizedInfix = infix.substring(0, infix.length()-lastToken.length()).split("[^a-z0-9-]+");

			//find the set of (distinct) previous tokens			
			String suffixBuilder =  lastToken;
								
			for (int i = tokenizedInfix.length-2; i > 0; --i) {
				//System.out.println("SuffixBuilder = "+suffixBuilder);
				p = Pattern.compile("[^a-z0-9 ]+$", Pattern.CASE_INSENSITIVE);
				int endIndex = URI.length()-ending-suffixBuilder.length();
				int startIndex = endIndex - tokenizedInfix[i].length();
//				
//				System.out.println("firstRoundtokenized["+i+"]:"+tokenizedInfix[i]);
//				System.out.println("suffix:"+suffixBuilder);
//				System.out.println("start:"+startIndex);
//				System.out.println("end:"+endIndex);
				
				m = p.matcher(URI.substring(startIndex, endIndex));
				b = m.find();
				int newEnding = b ? m.end()-m.start() : 1; //how many consecutive special characters?
				String delims = "";
				for (int j = 0; j < newEnding; ++j) {
					char delim = infix.charAt(infix.length()-ending-suffixBuilder.length()-j);					
					delims = delim + delims;
				}				

				suffixBuilder = tokenizedInfix[i] + delims + suffixBuilder;				
				Set<String> distinctPrevTokens;
				if (suffixes.containsKey(suffixBuilder)) {
					distinctPrevTokens = suffixes.get(suffixBuilder);
				} else {
					distinctPrevTokens = new HashSet<>();							
				}
				if (i > 0) {
					//System.out.println("Adding "+tokenizedInfix[i-1]+" to the previous tokens of "+suffixBuilder);
					distinctPrevTokens.add(tokenizedInfix[i-1]);						
				}
				suffixes.put(suffixBuilder, distinctPrevTokens);
			}		
		}
				
		
		for (String URI : URIs.keySet()) {			
			String infix = URIs.get(URI);
			reporter.setStatus("Removing the suffix of "+infix);
			int maxPrevTokens = 1;			
			//some URIs end with special chars...
			Pattern p = Pattern.compile("[^a-z0-9 ]+$", Pattern.CASE_INSENSITIVE);
			Matcher m = p.matcher(infix);			
			int ending = m.find() ? m.end()-m.start() : 0; //how many consecutive special characters?			
			//String[] tokenizedInfix = URI.substring(0, URI.length()-ending).split("[^a-z0-9-]+");
			String[] tokenizedInfix = infix.substring(0, infix.length()-ending).split("[^a-z0-9-]+");
			String suffixBuilder = lastToken;
			boolean changed = false;
			//System.out.println("URI:"+URI.length());
			//System.out.println("infix:"+infix);
			for (int i = tokenizedInfix.length-2; i > 0; --i) { //there is no point at removing the first token of the infix --> null infix
				reporter.progress();
				p = Pattern.compile("[^a-z0-9 ]+$", Pattern.CASE_INSENSITIVE);
				int endIndex = URI.length()-ending-suffixBuilder.length();
				int startIndex = endIndex - tokenizedInfix[i].length();
				
				m = p.matcher(URI.substring(startIndex, endIndex));				
				int newEnding = m.find() ? m.end()-m.start() : 1; //how many consecutive special characters?
				String delims = "";
				for (int j = 0; j < newEnding; ++j) {
//					System.out.println("tokenizedInfix:"+tokenizedInfix[i]);
//					System.out.println("suffix:"+suffixBuilder);					
//					System.out.println("infixLength:"+infix.length());					
//					System.out.println("URILength:"+URI.length());
//					System.out.println("ending:"+ending);
//					System.out.println("j:"+j);					
					char delim = URI.charAt(URI.length()-ending-suffixBuilder.length()-j);
//					System.out.println("delim:"+delim);
					delims = delim + delims;
				}
				suffixBuilder = tokenizedInfix[i] + delims + suffixBuilder;
				if (!suffixes.containsKey(suffixBuilder)) continue;
				int currPrevTokensSize = suffixes.get(suffixBuilder).size();
				//System.out.println("Suffix:"+suffixBuilder+" has "+currPrevTokensSize+" previous tokens");
				if (currPrevTokensSize > maxPrevTokens && counter > 1) {
					maxPrevTokens = currPrevTokensSize;			
					if (infix.length() > suffixBuilder.length() + ending) {
						//System.out.println("Infix:"+infix+" -- length = "+infix.length());
						//System.out.println("Suffix:"+suffixBuilder+" -- length = "+suffixBuilder.length());
						infix = infix.substring(0,infix.length()-ending-suffixBuilder.length()); //-1 to skip the special character
						changed = true;
						//System.out.println("infix changed to:"+infix);
					} else {
						continue; //suffix is null
					}
				}
			}
			
			//TODO:check the following block
			if (!changed && counter > 1 && infix.length() > lastToken.length() + ending) {				
				p = Pattern.compile("[^a-z0-9 ]+$", Pattern.CASE_INSENSITIVE);
				int endIndex = infix.length()-ending-lastToken.length();				
				m = p.matcher(infix.substring(0, endIndex));				
				int newEnding = m.find() ? m.end()-m.start() : 1; //how many consecutive special characters?
				//System.out.println("infix:"+infix);
				//System.out.println("First token of infix:"+infix.substring(0, endIndex));
				String delims = "";
				for (int j = 0; j < newEnding; ++j) {//							
					char delim = URI.charAt(URI.length()-ending-lastToken.length()-j);
					delims = delim + delims;
				}				
				String lastTokenWithDelims = delims + lastToken;				
				
				//System.out.println("lastTokenWithDelims:"+lastTokenWithDelims);
				
				infix = infix.substring(0,infix.length()-ending-lastTokenWithDelims.length());
			}			
			
			//infix = infix.replaceFirst("\\.[a-z0-9]{1,6}$", ""); //if it finishes with .text remove it
			output.collect(new Text(URI), new Text(infix));			
		}
	}

}
