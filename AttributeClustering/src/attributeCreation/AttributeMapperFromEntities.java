package attributeCreation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class AttributeMapperFromEntities extends MapReduceBase implements Mapper<Text, Text, Text, Text> {

	static enum InputData { NOT_AN_ENTITY, MALFORMED_PAIRS };
	
	Set<String> stopWords;	
	private Path[] localFiles;
	
	public void configure(JobConf job){	
		stopWords = new HashSet<>(116); //initial capicity 116 ~= 155 * 0.75 (stopwords * hash load factor)		
		
		BufferedReader SW;
		try {
			localFiles = DistributedCache.getLocalCacheFiles(job); //for the cluster version			
			SW = new BufferedReader(new FileReader(localFiles[0].toString())); //for the cluster version
			stopWords.addAll(Arrays.asList(SW.readLine().split(",")));			
		    SW.close();
		} catch (IOException e) {
			System.err.println(e.toString());
		}	
	}
	
	/**
	 * input value: a line describing an entity: dID;;;Eid\tdID;;;name###value###dID;;;name###value
	 * output:
	 * key: predicate of the input triple (prefixed by the data source id)
	 * value: object of the input triple (many words)
	 */
	public void map(Text key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		String entity = value.toString();
		/*String[] split = entity.split("\t"); //split id and att-values pairs
				
		if (split.length != 2) { //set counter for malformed input (entities)
			reporter.incrCounter(InputData.NOT_AN_ENTITY, 1);
			System.out.println("Not an entity: "+value);
			return;
		}
		
		String pairs = split[1];
		String[] elements = pairs.split("###");
		*/
		String[] elements = entity.split("###");
		if (elements.length < 2 || elements.length % 2 != 0) {
			reporter.incrCounter(InputData.MALFORMED_PAIRS, 1);
			System.out.println("Malformed: "+entity);
			return;
		}
		
		Set<String> tokensLimit = new HashSet<>(); //keep the first 100 tokens as values (for efficiency)
		for (int i=0; i < elements.length; ++i) { 
			String pred = elements[i].toLowerCase(); //already prefixed with dID	
			if (pred.startsWith("#")) { //bug of splitting by ###
				pred = pred.substring(1);
			}
			String val = elements[++i].toLowerCase();
			val = val.replaceAll("[^a-z0-9 ]+"," "); //remove special characters for URIs), keep white space
			val = val.replaceAll("[ ]+", " "); //FIXME: use this for trigrams only
			
			//the following code keeps the first 100 distinct non-stopword values from each entity 
			for (String token: val.split(" ")) {
				
				if (token.length() > 1 && !stopWords.contains(token)) { //keep value
					tokensLimit.add(token);
					if (tokensLimit.size() > 100) { break;} //FIXME: change/remove parameter
				} else { //remove stopword from values
					//"\\b" gives you the word boundaries
				    //"\\s*" sops up any white space on either side of the word being removed
					String regex = "\\s*\\b"+token+"\\b\\s*";
					val = val.replaceAll(regex, " ");		
					reporter.progress();
					val = val.replaceAll("[ ]+", " ").trim();
				}				
			}			
			output.collect(new Text(pred), new Text(val));
			if (tokensLimit.size() > 100) { return;} //FIXME: change/remove parameter
		}		
	}
}
