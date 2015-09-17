package attributeCreation;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class AttributeGrouper extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	Set<String> stopWords;
	private Path[] localFiles; //for the cluster version
		
	public void configure(JobConf job){
		stopWords = new LinkedHashSet<>();
        BufferedReader SW;
		try {
			localFiles = DistributedCache.getLocalCacheFiles(job); //for the cluster version
			SW = new BufferedReader(new FileReader(localFiles[0].toString())); //for the cluster version
			//SW = new BufferedReader(new FileReader("stopwords.txt")); //for the local version
			
			stopWords.addAll(Arrays.asList(SW.readLine().split(",")));			
		    SW.close();
		} catch (FileNotFoundException e) {
			System.err.println(e.toString());
		} catch (IOException e) {
			System.err.println(e.toString());
		}        
	}
	
	public void reduce(Text _key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {		
		
		Set<String> valuesSet = new HashSet<>();
		while (values.hasNext()) { //this is the object of one triple (many words)
			String[] tokens = values.next().toString().split("[\\W_]"); //split the value field into tokens
			for (String token: tokens) {
				String trimmed = token.trim();
				if (!stopWords.contains(trimmed) && trimmed.length() > 1) { //stopword removal
					valuesSet.add(trimmed);
				}
			}
			reporter.progress();
		}
		
		if (valuesSet.isEmpty()) {
			return;
		}
		
		String toEmit = "";
		for (String value : valuesSet) {
			toEmit += " "+value;
		}
		toEmit.trim();
		output.collect(_key, new Text(toEmit));
	}

}
