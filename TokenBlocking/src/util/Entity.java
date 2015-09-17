package util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Entity {
	private String id;
	private Set<String> trigrams;		
	
	public Entity(String id, Set<String> trigrams) {			
		this.id = id;
		this.trigrams = trigrams;
	}
	
	/**
	 * 
	 * @param id
	 * @param trigrams a string with whitespace-separated trigrams
	 */
	public Entity(String id, String trigrams) {			
		this.id = id;
		String[] trigramsArray = trigrams.split(" ");
		this.trigrams = new HashSet<>(Arrays.asList(trigramsArray));
	}
	
	public String getId() {
		return id;
	}
	
	public Set<String> getTrigrams() {
		return trigrams;
	}
	
	/**
	 * @return the set of trigrams as whitespace-separated strings
	 */
	public String getTrigramsString() {
		String result = "";
		for (String trigram : this.getTrigrams()) {
			result += " "+trigram;
		}
		return result.trim();
	}				
}
