package tokenBlocking;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class TokenBlockingMatchingMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
	
	static enum InputData {NOT_AN_ENTITY, NULL_PREFIX_ID, MALFORMED_PAIRS};
	
	Set<String> stopWords;
	private Path[] localFiles;
	
	
	public void configure(JobConf job){	
		
		stopWords = new HashSet<>();
		
		BufferedReader SW;
		try {
			localFiles = DistributedCache.getLocalCacheFiles(job); //for the cluster version			
			SW = new BufferedReader(new FileReader(localFiles[0].toString())); //for the cluster version
			stopWords.addAll(Arrays.asList(SW.readLine().split(",")));			
		    SW.close();
		} catch (FileNotFoundException e) {
			System.err.println(e.toString());
		} catch (IOException e) {
			System.err.println(e.toString());
		}		
	}
	
	
	/**
	 * maps an input entity description into (key, value) pair(s)
	 * the value is always the whole entity
	 * the key is determined as follows:
	 * if the value of the description is a URI, then the key is the infix of the URI
	 * otherwise, the keys are the tokens of the value
	 * also emit a (key, value) pair for the infix of the subject of the input entity (entity id) 
	 */
	public void map(Text key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		//parsing the input
		//String inputEntity = value.toString();
		//String []entityParams = inputEntity.split("\t");
				
		
		//String s = entityParams[0];
		//String[] rawValues = value.toString().split("###");	
		String s = key.toString();
		String[] elements = value.toString().split("###");
		reporter.progress();
			
		if (elements.length < 2 || elements.length % 2 != 0) {
			reporter.incrCounter(InputData.MALFORMED_PAIRS, 1);
			System.out.println("Malformed: "+elements);
			return;
		}
		
		//use this block ONLY IF entities contain predicates in their values
		String[] rawValues = new String[elements.length/2];
		for (int i =1, j = 0; i < elements.length; i = i + 2) {
			reporter.progress();
			rawValues[j++] = elements[i];
		}
		
		//start the processing		
		Set<String> values = new HashSet<>(); //set to remove duplicates
		for (String val: rawValues) {			
			val = val.replaceAll("[^a-z0-9 ]+"," "); //remove special characters, keep white space
			reporter.progress();
			val = val.replaceAll("[ ]+", " "); //keep only one white space if more exist
			String [] vals = val.split(" ");
			reporter.progress();
			values.addAll(Arrays.asList(vals)); //to remove duplicate tokens
			if (values.size() > 100) break; //TODO: change parameter			
		}		
		
		Set<String> newValues = new HashSet<>();
		//for (String val: values) {
		Iterator<String> it = values.iterator();
		int counter = 0;
		while (it.hasNext()) {
			if (counter++ == 100) break; //TODO: change parameter
			String val = it.next();
			if (val.length() > 1 && !stopWords.contains(val)){
				newValues.add(val);
				//output.collect(new Text(val), toEmit);
			}
		}
		
		String[] valArray = new String[newValues.size() > 100 ? 100 : newValues.size()];
		//end of #tokens restriction block
		
		String valueString = Arrays.toString(newValues.toArray(valArray)); //this prints [a, b, ..., m]
		//produce comma-separated tokens as valueString
		valueString = valueString.substring(1, valueString.lastIndexOf(']')); //this removes '[' and ']'		
		Text toEmit = new Text(s+";;;"+valueString);
		
		for (String val: newValues) {
			output.collect(new Text(val), toEmit);
		}
		
	}

}
