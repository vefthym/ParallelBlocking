package infixExtraction;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InfixExtraction {
	
	final static Charset ENCODING = StandardCharsets.UTF_8;	
		
	public static boolean isURI(String string) {			
		return string.startsWith("<http://") && !string.contains("www");			
	}
	
	public static Set<String> updateURIs(Path path, Set<String> URIs) {
		int counter = 0;
		try (Scanner scanner =  new Scanner(path, ENCODING.name())){
	      while (scanner.hasNextLine()){
	    	counter++;
	    	String triple = scanner.nextLine();
	    	String[] spo = triple.split("> "); //split s p o  //end of a uri (subject or pred)
	    	
	    	if (counter % 1000000 == 0) {
	    		System.out.println("Read "+counter/1000000+"M triples...");
	    	}
	    	
	    	if (spo.length > 4 || spo.length < 3) { //set counter for malformed input (n-triples)
	    		//if (spo.length != 4) { //set counter for malformed input (n-quads) 4th elem is datasource		
				boolean malformed = true;
				for (int i = 3; i < spo.length; ++i) {
					String spo2 = spo[i].toLowerCase();				
					if (spo2.endsWith("<sup") 
							|| spo2.endsWith("</sup")
							|| spo2.endsWith("<br/")
							|| spo2.endsWith("<br /")
							|| spo2.endsWith("<br\\\\")
							|| spo2.endsWith("</br")
							|| spo2.endsWith("<br")) { //minor cases				
					malformed = false;
					}
					spo[2] += " "+spo2;
				}
							
				if (malformed) {			
					continue;
				}			
	    	}
	    	
	    	String s = spo[0].substring(1).toLowerCase();
	    	if (isURI(s)) { //excluding blank nodes
	    		URIs.add(s);
	    	}
	    	
	    	String o = spo[2].toLowerCase();	  
	    	if (isURI(o)) {
	    		URIs.add(o.substring(1));
	    	}	    	
	      }      
	    } catch (IOException e) {			
			System.err.println(e.toString());
		}
		return URIs;
	}
	
	public static void main(String[] args) {
		String test = "http://dbpedia.org/resource/john_stewart_(died,!_1796)#";
		String infix = "john_stewart_(died,!_1796)#";
		//System.out.println(Arrays.toString(test.split("[^a-z0-9-]+")));
		Pattern p = Pattern.compile("[^a-z0-9 ]+$", Pattern.CASE_INSENSITIVE);
		Matcher m = p.matcher(infix);
		boolean b = m.find();
		int ending = b ? m.end()-m.start() : 1; //how many consecutive special characters?
		
		String[] tokenizedInfix = infix.substring(0, infix.length()-ending).split("[^a-z0-9-]+");
		System.out.println(Arrays.toString(tokenizedInfix));
		String suffixBuilder =  "1796";	
		//System.out.println(infix);
		for (int i = tokenizedInfix.length-2; i >= 0; --i) {
			//System.out.println("SuffixBuilder = "+suffixBuilder);

			p = Pattern.compile("[^a-z0-9 ]+$", Pattern.CASE_INSENSITIVE);
			int endIndex = test.length()-ending-suffixBuilder.length();
			int startIndex = endIndex - tokenizedInfix[i].length();
			m = p.matcher(test.substring(startIndex, endIndex));
			b = m.find();
			int newEnding = b ? m.end()-m.start() : 1; //how many consecutive special characters?
			String delims = "";
			for (int j = 1; j <= newEnding; ++j) {
				char delim = infix.charAt(infix.length()-ending-suffixBuilder.length()-j);
				//System.out.println(delim);
				delims = delim + delims;
			}
			//System.out.println(infix.charAt((infix.length()-ending)));
			suffixBuilder = tokenizedInfix[i] + delims + suffixBuilder;
		}
	}
	
	public static void main2(String [] args) {
		Set<String> URIs = new TreeSet<>();	
		
		System.out.println("Reading...");
		//STEP 1: Sort URIs (TreeSet orders them)
		//Path path = Paths.get("/home/vefthym/Desktop/DATASETS/34infobox_en.nt");
		Path path = Paths.get("/home/hduser/Documents/data-0.nq-2");
		URIs = updateURIs(path, URIs);
		System.out.println("Finished reading!");
		
		//path = Paths.get("/home/vefthym/Desktop/DATASETS/39raw_infobox_properties_en.nt");
		//URIs = updateURIs(path, URIs);
		
		Map<String, List<String>> clusters = new HashMap<>();
		for (String URI : URIs) {
			//System.out.println(URI);
			//STEP 2: tokenize URIs at all special characters
			String[] tokens = URI.split("[\\W_]+");
			if (tokens.length <= 2) {
				continue;
			}			

			//STEP 3: cluster URIs according to the first n tokens
			String prefix = tokens[1]; //tokens[0] is always http		
			List<String> URIsInCluster;
			if (clusters.containsKey(prefix)) {
				URIsInCluster = clusters.get(prefix);				
			} else {		
				URIsInCluster = new ArrayList<>();				
			}			
			URIsInCluster.add(URI);
			clusters.put(prefix, URIsInCluster);
		}
		
		//STEP 4: for all clusters do 
		for (Map.Entry<String, List<String>> cluster : clusters.entrySet()) {
			//System.out.println("cluster:"+cluster.getKey()+": "+cluster.getValue().size());			
			Map<String, Set<String>> prefixes = new HashMap<>();			
			//STEP 5: for all URIs in the cluster do
			for (String URI : cluster.getValue()) {
				//System.out.println("URI:"+URI);
				//STEP 6: for all possible prefixes do				
				String[] tokenizedPrefixes = URI.substring(8+(cluster.getKey().length())).split("[^a-z0-9-]+");	//7 is the length of the string "http://" +1 for the special character

				//STEP 7: find the set of (distinct) next tokens			
				String prefixBuilder = "http://"+cluster.getKey();							
				
				for (int i = 0; i < tokenizedPrefixes.length; ++i) {
					prefixBuilder += URI.charAt(prefixBuilder.length())+tokenizedPrefixes[i];
					//System.out.println("prefix: "+prefixBuilder);
					Set<String> distinctNextTokens;
					if (prefixes.containsKey(prefixBuilder)) {
						distinctNextTokens = prefixes.get(prefixBuilder);
					} else {
						distinctNextTokens = new HashSet<>();							
					}
					if (i == tokenizedPrefixes.length - 2) {
						if (URI.charAt(prefixBuilder.length()) == '.') {
							System.out.println("DOT FOUND!");							
						}
					} else if (i+1 < tokenizedPrefixes.length) {
						distinctNextTokens.add(tokenizedPrefixes[i+1]);						
					} 
					prefixes.put(prefixBuilder, distinctNextTokens);					
				}				
			}
			//STEP 8: for all URIs in the cluster do			
			for (String URI : cluster.getValue()) {
				int maxNextTokens = 0;
				String prefix  = "";
				String infix = "";
				String[] tokenizedPrefixes = URI.substring(8+(cluster.getKey().length())).split("[^a-z0-9-]+");
				String prefixBuilder = "http://"+cluster.getKey();
				for (String candidate : tokenizedPrefixes) {
					prefixBuilder += URI.charAt(prefixBuilder.length())+candidate;
					int currNextTokensSize = prefixes.get(prefixBuilder).size();
					if (currNextTokensSize >= maxNextTokens) {
						maxNextTokens = currNextTokensSize;
						prefix = prefixBuilder+URI.charAt(prefixBuilder.length());
						infix = URI.substring(prefixBuilder.length()+1); //+1 to skip the special character
					}
				}
				System.out.println("Prefix("+URI+") = "+prefix);
				System.out.println("Infix("+URI+") = "+infix);
			}
		}		
	}


}
