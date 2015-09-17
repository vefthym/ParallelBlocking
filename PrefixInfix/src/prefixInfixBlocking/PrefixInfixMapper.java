package prefixInfixBlocking;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import util.Tools;

public class PrefixInfixMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
	
	static enum InputData {NOT_AN_ENTITY, NULL_PREFIX_ID, INFIX_USED, NO_INFIX_USED};
	
	private Map<VIntWritable,String> infixes;
	Set<String> stopWords;
	private Path[] localFiles;
	
	public void configure(JobConf job){
		infixes = new HashMap<>(7000000);
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
		
		BufferedReader SW2;		
		//for (Path file : localFiles) {						
			//if (file.toString().contains("part")) {
		try {
			localFiles = DistributedCache.getLocalCacheFiles(job);
			long infixCounter = 0;
			SW2 = new BufferedReader(new FileReader(localFiles[1].toString())); 
			String line = "";
			while ((line = SW2.readLine()) != null) {
				String [] UriInfix = line.split("\t");
				if (UriInfix.length == 2) {
					if (!UriInfix[0].startsWith("http://dbpedia.org")) { //debugging
					infixes.put(new VIntWritable(UriInfix[0].hashCode()), UriInfix[1]); //else continue
					if (++infixCounter % 100000 == 0) {
						System.out.println("Read "+infixCounter+" infixes");
					}
					}
				}
			}
			SW2.close();
		} catch (FileNotFoundException e) { 
			System.err.println(e.toString()); 
		} catch (IOException e) { 
			System.err.println(e.toString()); 
		}
			//}		    
		//}
		System.out.println("Loaded "+stopWords.size()+" stopwords!");	
		System.out.println("Loaded "+infixes.size()+" infixes!");
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
		String[] rawValues = new String[elements.length/2];
		for (int i =1, j = 0; i < elements.length; i = i + 2) {
			rawValues[j++] = elements[i];
		}
		
		//start the processing

		String[] subject = s.split(";;;");
		//infix blocking
		if (Tools.isURI(subject[1])) { //not a blank node
			String infix = infixes.get(new VIntWritable(subject[1].hashCode()));
			if (infix != null) {								
				output.collect(new Text(infix), new Text(s));
			}			
		}
		
		Set<String> values = new HashSet<>(); //set to remove duplicates
		for (String val: rawValues) {
			if (Tools.isURI(val)) {			//infix profile blocking
				String infix = infixes.get(new VIntWritable(val.hashCode()));
				if (infix != null) {								
					output.collect(new Text(infix), new Text(s));
					reporter.incrCounter(InputData.INFIX_USED, 1);
				} else {
					val = val.replaceAll("[^a-z0-9 ]+"," "); //remove special characters, keep white space
					val = val.replaceAll("[ ]+", " "); //keep only one white space if more exist
					String [] vals = val.split(" ");
					values.addAll(Arrays.asList(vals)); //to remove duplicate tokens
					reporter.incrCounter(InputData.NO_INFIX_USED, 1);
				}
			} else { //token blocking
				val = val.replaceAll("[^a-z0-9 ]+"," "); //remove special characters, keep white space
				val = val.replaceAll("[ ]+", " "); //keep only one white space if more exist
				String [] vals = val.split(" ");
				values.addAll(Arrays.asList(vals)); //to remove duplicate tokens
			}
			if (values.size() > 100) break; //TODO: change parameter		
		}
		
		int counter = 0;
		for (String val: values) {
			if (counter++ == 100) return; //TODO: change parameter
			if (val.length() > 1 && !stopWords.contains(val)) {
				output.collect(new Text(val), new Text(s));
			}
		}
	}

}
