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
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class TokenBlockingMapper extends MapReduceBase implements Mapper<VLongWritable, Text, Text, VLongWritable> {
	
	static enum InputData {NOT_AN_ENTITY, NULL_PREFIX_ID, MALFORMED_PAIRS};
	/*
	Set<String> stopWords;
	private Path[] localFiles;
	
	
	public void configure(JobConf job){	
		
		stopWords = new HashSet<>();
		
		BufferedReader SW;
		try {
			localFiles = DistributedCache.getLocalCacheFiles(job); 			
			SW = new BufferedReader(new FileReader(localFiles[0].toString())); 
			stopWords.addAll(Arrays.asList(SW.readLine().split(",")));			
		    SW.close();
		} catch (FileNotFoundException e) {
			System.err.println(e.toString());
		} catch (IOException e) {
			System.err.println(e.toString());
		}		
		
	}*/
	
	
	/**
	 * maps an input entity description into (key, value) pair(s)
	 * the value is always the whole entity
	 * the key each time is a token of the values
	 * @param key: the entity id
	 * @param value: the attribute-value pairs of the entity,separated by "###"
	 */
	public void map(VLongWritable key, Text value,
			OutputCollector<Text, VLongWritable> output, Reporter reporter) throws IOException {

		//String s = key.toString(); //eid
		String[] elements = value.toString().split("###"); //att-value pairs
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
		Set<String> values = new HashSet<>(); //Set to remove duplicates
		for (String val: rawValues) {			
			val = val.replaceAll("[^a-z0-9 ]+"," "); //remove special characters, keep white space
			reporter.progress();
			val = val.replaceAll("[ ]+", " "); //keep only one white space if more exist
			String [] vals = val.split(" ");
			reporter.progress();
			values.addAll(Arrays.asList(vals)); //to remove duplicate tokens
			//if (values.size() > 100) break; //TODO: uncomment to make sure it runs			
		}
		
		Iterator<String> it = values.iterator();
		//int counter = 0;
		while (it.hasNext()) {
			//if (counter++ == 100) return; //TODO: uncomment to make sure it runs
			String val = it.next();
			if (val.length() > 1){
				output.collect(new Text(val), key);
			}
		}
		
	}

}
