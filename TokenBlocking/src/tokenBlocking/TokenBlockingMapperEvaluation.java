package tokenBlocking;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class TokenBlockingMapperEvaluation extends MapReduceBase implements Mapper<Text, Text, Text, VIntArrayWritable> {
	
	static enum InputData {NOT_AN_ENTITY, NULL_PREFIX_ID, UNARIES, MALFORMED_PAIRS};	
		
	Set<String> stopWords;
	//Set<String> unaryTokens;
	private Path[] localFiles;
	
	public void configure(JobConf job){	
		stopWords = new HashSet<>(116); //initial capicity 116 ~= 155 * 0.75 (stopwords * hash load factor)
		//unaryTokens = new HashSet<>(4252793); //initial capacity 4252793 ~= 5670391 * 0.75
		
		BufferedReader SW;
		try {
			localFiles = DistributedCache.getLocalCacheFiles(job); //for the cluster version			
			SW = new BufferedReader(new FileReader(localFiles[0].toString())); //for the cluster version
			stopWords.addAll(Arrays.asList(SW.readLine().split(",")));			
		    SW.close();
		} catch (IOException e) {
			System.err.println(e.toString());
		}		
		/*
		try {
			localFiles = DistributedCache.getLocalCacheFiles(job); //for the cluster version			
			SW = new BufferedReader(new FileReader(localFiles[1].toString())); //for the cluster version
			unaryTokens.addAll(Arrays.asList(SW.readLine().split(",")));			
		    SW.close();
		} catch (FileNotFoundException e) {
			System.err.println(e.toString());
		} catch (IOException e) {
			System.err.println(e.toString());
		}
		*/	
	}
		
	
	/**
	 * maps an input entity description into (key, value) pair(s)
	 * the value is always the whole entity
	 * the key is determined as follows:
	 * if the value of the description is a URI, then the key is the infix of the URI
	 * otherwise, the keys are the tokens of the value
	 * also emit a (key, value) pair for the infix of the subject of the input entity (entity id) 
	 * output:
	 * 	key: token (Text)
	 * 	value: Array a of hashes, where a[0] = dID, a[1] = subject hash and the rest are the previous tokens hashes
	 */
	public void map(Text key, Text value,
			OutputCollector<Text, VIntArrayWritable> output, Reporter reporter) throws IOException {
		//uncompressed
		//String s = value.toString().split("\t")[0];
		//String[] rawValues = value.toString().split("\t")[1].split("###");				
		
		//compressed
		String s = key.toString();
		String[] elements = value.toString().split("###");		
		reporter.progress();
		
		if (elements.length < 2 || elements.length % 2 != 0) {
			reporter.incrCounter(InputData.MALFORMED_PAIRS, 1);
			//System.out.println("Malformed: "+rawValues);
			return;
		}
		
		//use this block ONLY IF entities contain predicates in their values
		String[] rawValues = new String[elements.length/2];
		for (int i =1, j = 0; i < elements.length; i = i + 2) {
			reporter.progress();
			rawValues[j++] = elements[i];
		}

		
		//start the processing		
		Set<String> values = new HashSet<>(); //set to remove duplicates (ordered Set is not needed)
		
		for (String val: rawValues) {
			val = val.replaceAll("[^a-z0-9 ]+"," "); //remove special characters, keep white space
			reporter.progress();
			val = val.replaceAll("[ ]+", " "); //keep only one white space if more exist
			for (String stringVal : val.split(" ")) {
				if (stringVal.length() > 1 && !stopWords.contains(stringVal)) {
					values.add(stringVal);
				}
				if (values.size() > 100) break; //FIXME: remove/change this line
			}
			reporter.progress();
			if (values.size() > 100) break; //FIXME: remove/change this line
		}

		//VIntWritable for less space consumption
		VIntWritable[] allValues = new VIntWritable[values.size()];
		int i = 0;
		for (String val: values) {
			allValues[i++] = new VIntWritable(val.hashCode());
		}
		
		String[] subject = s.split(";;;");
		//System.out.println("emitting the values of "+subject[1].hashCode()+":"+subject[1]);		
		int counter = 1;
		for (String val: values) {
			VIntWritable[] prevKeys = new VIntWritable[++counter]; //stores the previous keys
			prevKeys[0] = new VIntWritable(Integer.parseInt(subject[0])); //first is datasource iD 
			prevKeys[1] = new VIntWritable(subject[1].hashCode()); //second in the array is subject (entity id)
			System.arraycopy(allValues, 0, prevKeys, 2, counter-2);		
		//	System.out.println(new VIntArrayWritable(prevKeys).toString());
			output.collect(new Text(val), new VIntArrayWritable(prevKeys));		
		}
	}

}