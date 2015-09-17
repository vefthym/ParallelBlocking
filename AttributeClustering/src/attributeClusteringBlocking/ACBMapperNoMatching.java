package attributeClusteringBlocking;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class ACBMapperNoMatching extends MapReduceBase implements Mapper<Text, Text, Text, Text> {

	static enum Entities { NOT_AN_ENTITY, MALFORMED_PAIRS, NULL_VALUE };
		
	private Map<String, Integer> clustering; //key: predicate, value: clusterIndex
	private Set<String> stopWords;	
	private Path[] localFiles; //HDFS
	private Map<Integer, Integer> matchingClusters; //key: existing clusterIndex, value: new clusterIndex
		
	public void configure(JobConf job){
		
		stopWords = new HashSet<>();
		//unaryTokens = new HashSet<>();
		
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
					
		matchingClusters = new HashMap<>();
		
		BufferedReader SW2;
		try {
			localFiles = DistributedCache.getLocalCacheFiles(job); //HDFS			
			SW2 = new BufferedReader(new FileReader(localFiles[1].toString())); //HDFS
			String line;
			while ((line = SW2.readLine()) != null) {
				String[] lineParams = line.split("\t");
				matchingClusters.put(Integer.parseInt(lineParams[0]), Integer.parseInt(lineParams[1]));
			}
		    SW2.close();
		} catch (FileNotFoundException e) {
			System.err.println(e.toString());
		} catch (IOException e) {
			System.err.println(e.toString());
		}
		
		clustering = new HashMap<>();
		
		BufferedReader SW3;
		try {
			localFiles = DistributedCache.getLocalCacheFiles(job); //for the cluster version			
			SW3 = new BufferedReader(new FileReader(localFiles[2].toString())); //for the cluster version			
			String line;
			while ((line = SW3.readLine()) != null) {
				String[] pred_clusterIndex = line.split("\t");
				String predicate = pred_clusterIndex[0];
				Integer index = Integer.parseInt(pred_clusterIndex[1]);
				Integer finalIndex = getFinalClusterIndex(index);
				clustering.put(predicate, finalIndex);
			}		
		    SW3.close();
		} catch (FileNotFoundException e) {
			System.err.println(e.toString());
		} catch (IOException e) {
			System.err.println(e.toString());
		}	
		
		
	}
	
	/**
	 * map each entity to its set of tokens appearing in the values, 
	 * based on the attribute clusters
	 * @param value a line in the document describing an entity
	 * @param output (cluster:token, eid) pairs for each token in the values of an entity description 
	*/ 
	public void map(Text key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {	
	    /*
		String entityDescriptionString = value.toString();
		String[] singleEntityData = entityDescriptionString.split("\t"); //split into id \t att###val###att###val### ...
		
		if (singleEntityData.length != 2) {
			reporter.incrCounter(Entities.NOT_AN_ENTITY, 1);
			System.err.println("Malformed input:"+entityDescriptionString);
			return;
		}
		String eid = singleEntityData[0]; //entity id is the first string (prefixed with dID;;;)
		String[] attsNvalues = singleEntityData[1].split("###");
		*/		
		String eid = key.toString(); //entity id is the first string (prefixed with dID;;;)
		reporter.setStatus("mapping entity "+eid);
		String[] attsNvalues = value.toString().split("###");
		
		
		if (attsNvalues.length < 2 || attsNvalues.length % 2 != 0) {
			reporter.incrCounter(Entities.MALFORMED_PAIRS, 1);
			System.out.println("Malformed: "+attsNvalues);
			return;
		}
		
		Set<String> attributeValues;// = new HashSet<>(); //treeset to order the tokens
		Set<String> values = new HashSet<>(100); //this is just for the 100 values restriction
		
		//int valueCounter = 0;
				
		for (int i = 0; i < attsNvalues.length; ++i) {			
			String attributeField = attsNvalues[i];
			if (attributeField.startsWith("#")) { //bug of splitting by ###
				attributeField = attributeField.substring(1);
			}		
						
			String valueField = attsNvalues[++i];
			
			Integer clusterInt = clustering.get(attributeField);
			if (clusterInt == null) { //put it in the glue cluster
				clusterInt = -1;
				//continue;
			}
			
//			if (valueField == null) { //not sure if this is useful
//				reporter.incrCounter(Entities.NULL_VALUE, 1);
//				System.out.println("Null value for attribute: "+attributeField);
//				continue;
//			}
			
			//valueField = valueField.toLowerCase();
			valueField = valueField.replaceAll("[^a-z0-9 ]+"," "); //remove special characters, keep white space
			reporter.progress();
			valueField = valueField.replaceAll("[ ]+", " "); //keep only one white space if more exist
			reporter.progress();
			String [] tokens = valueField.split(" ");
			reporter.progress();
			//String[] tokens = valueField.split("[\\W_]"); //split the value field into tokens
			attributeValues = new HashSet<>(Arrays.asList(tokens));//set: no duplicate tokens here			
			
			if (values.size() < 100) { //keep 100 distinct values
				values.addAll(attributeValues);
			} else { //keep the first 100 distinct values for each entity, as in Token Blocking
				int counter = 100;
				Set<String> tmpValues = new HashSet<>(100);
				for (Iterator<String> it = values.iterator(); it.hasNext() && counter >0; counter--) {
					tmpValues.add(it.next());					
				}
				values = tmpValues;
			}
			
			//this will create duplicate entries for the same token appearing in different attribute
			//duplicates will be removed by the combiner
			for (String val : attributeValues) {
				if (!values.contains(val)) continue;
				if (val.length() > 1 && !stopWords.contains(val)) {
					output.collect(new Text(clusterInt+":"+val), new Text(eid));
				}
			}
		}
		
	}
	
	
	
	private Integer getFinalClusterIndex(Integer clusterIndex) {
	    Integer ultimateClusterId = clusterIndex;
	    Integer soughtClusterId = clusterIndex;	   
	    do {
	        Integer nextClusterId = matchingClusters.get(soughtClusterId);
	        if (nextClusterId != null) {
	            ultimateClusterId = nextClusterId;
	        }
	        soughtClusterId = nextClusterId;
	    } while (soughtClusterId != null);
	    
	    return ultimateClusterId;

	    //what follows is just a normalization of ultimateClusterId to values starting from zero
	    /* not working when #mappers > 1 
	    Integer finalClustering = usedIndices.get(ultimateClusterId);
	    if (finalClustering != null) {
	    	return finalClustering;
	    } else {
	    	usedIndices.put(ultimateClusterId, finalIndex);
	    	return finalIndex++;
	    }
	    */
	}
}