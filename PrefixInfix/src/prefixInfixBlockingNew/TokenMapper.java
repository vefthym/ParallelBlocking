package prefixInfixBlockingNew;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import util.Tools;

public class TokenMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
	
	static enum InputData {NOT_AN_ENTITY, NULL_PREFIX_ID, INFIX_USED, NO_INFIX_USED, MALFORMED_PAIRS};
	
	Set<String> stopWords;
	private Path[] localFiles;
	
	public void configure(JobConf job){
		stopWords = new HashSet<>(116);
		
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
		//System.out.println("Loaded "+stopWords.size()+" stopwords!");		
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
		/*
		//parsing the input (uncompressed)
		String inputEntity = value.toString();
		String []entityParams = inputEntity.split("\t");
		
		if (entityParams.length != 2) {
			reporter.incrCounter(InputData.NOT_AN_ENTITY, 1);
			System.err.println("Malformed input:"+inputEntity);
			return;
		}
		
		String s = entityParams[0];
		String[] rawValues = entityParams[1].split("###");		
		*/
		String s = key.toString();		
		//String[] rawValues = value.toString().split("###");
		
		//use this block ONLY IF entities contain predicates in their values
		String[] elements = value.toString().split("###");
		
		if (elements.length < 2 || elements.length % 2 != 0) {
			reporter.incrCounter(InputData.MALFORMED_PAIRS, 1);
			//System.out.println("Malformed: "+elements);
			return;
		}
		
		
		String[] rawValues = new String[elements.length/2];
		for (int i =1, j = 0; i < elements.length; i = i + 2) {
			rawValues[j++] = elements[i];
		}		
		
		
		//start the processing
		
		Set<String> values = new HashSet<>(100); //set to remove duplicates
		for (String val: rawValues) {
			if (Tools.isURI(val)) {			//infix profile blocking
				values.add(val);			//this case is handled by the other mapper
			} else { 						//token blocking
				val = val.replaceAll("[^a-z0-9 ]+"," "); //remove special characters, keep white space
				reporter.progress();
				val = val.replaceAll("[ ]+", " "); //keep only one white space if more exist				
				String [] vals = val.split(" ");
				reporter.progress();
				values.addAll(Arrays.asList(vals)); //to remove duplicate tokens
			}
			if (values.size() > 100) break; //TODO: change parameter		
		}
		
		int counter = 0;
		for (String val: values) {
			if (counter++ == 100) return; //TODO: change parameter
			if (val.length() > 1 && !stopWords.contains(val) && !Tools.isURI(val)) {
				//System.out.println("Emitting pair: ("+val+","+s+")");
				String[] subject = s.split(";;;");
				output.collect(new Text(val), new Text(subject[0]+";;;"+Integer.toString((subject[1].hashCode()))));
			}
		}
	}

}
