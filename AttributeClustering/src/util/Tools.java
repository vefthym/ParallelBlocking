package util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Tools {
	
	private static String[] stop3gramsArray = {"the","and","tha","ent","ing","ion","tio","for","nde","has","nce","edt","tis","oft","sth","men", "www", "com", "org", "rdf", "xml", "owl"};
	private static Set<String> stop3grams = new HashSet<>(Arrays.asList(stop3gramsArray));
		
	public static double jaccard (String values1, String values2) {
		String[] v1tokens = values1.split(" ");
		String[] v2tokens = values2.split(" ");
		
		final Set<String> allTokens = new HashSet<String>();
		allTokens.addAll(Arrays.asList(v1tokens));
		final int termsInString1 = allTokens.size();
		final Set<String> secStrToks = new HashSet<String>();
			
		secStrToks.addAll(Arrays.asList(v2tokens));
		final int termsInString2 = secStrToks.size();
				
		//now combine the sets
		allTokens.addAll(secStrToks);
		final int commonTerms = (termsInString1 + termsInString2) - allTokens.size();
		
		//return JaccardSimilarity
		return (double) (commonTerms) / (double) (allTokens.size());
	} 
	
	/**
	 * overloaded method that takes String Sets as input, instead of Strings
	 * @param values1
	 * @param values2
	 * @return jaccard similarity of String Sets values1 and values2
	 */
	public static double jaccard (Set<String> values1, Set<String> values2) {	
		
		final int termsInString1 = values1.size();		
		final int termsInString2 = values2.size();
				
		//now combine the sets
		values1.addAll(values2);
		final int commonTerms = (termsInString1 + termsInString2) - values1.size();
		
		//return JaccardSimilarity
		return (double) (commonTerms) / (double) (values1.size());
	}
	
	/**
	 * returns a set of trigrams from a string
	 * @param value the string to be trigramized
	 * @return a set of trigrams from the value  
	 */
	public static Set<String> getTrigrams(String value) { //linear complexity: O(value.length) 
		Set<String> trigrams = new HashSet<>(value.length());
		for (int i =0; i < value.length() - 2; ++i) {
			trigrams.add(value.substring(i,i+3)); //excluding i+3			
		}		
		return trigrams;
	}
	
	/**
	 * returns a set of trigrams from a string, excluding stop-trigrams
	 * @param value the string to be trigramized
	 * @return a set of trigrams from the value, excluding stop-trigrams
	 */
	public static Set<String> getNonStopTrigrams(String value) {
		Set<String> trigrams = new HashSet<>(value.length());
		for (int i =0; i < value.length() - 2; ++i) {
			String trigram = value.substring(i,i+3);
			if (!Tools.stop3grams.contains(trigram)) { //this excludes stop-trigrams
				trigrams.add(value.substring(i,i+3));
			}
		}		
		return trigrams;
	}
	
	/**
	 * @return a set of trigrams as a whitespace-separated string
	 */
	public static String getTrigramsString(Set<String> trigrams) {
		String result = "";
		for (String trigram : trigrams) {
			result += " "+trigram;
		}
		return result.trim();
	}	
	
	public static boolean isURI(String string) {
		//starts with http
		//does not contain www
		//is bigger than "http://"
		//return string.startsWith("http://") && !string.contains("www") && string.length() > 7;
		return string.startsWith("http://") && string.length() > 7;
	}
	
	/*
	public static void main (String[] args) {
		String arg1 = "A A A";
		String arg2 = "A A A";
		System.out.println(Tools.jaccard(arg1, arg2));
	}
	*/

}
