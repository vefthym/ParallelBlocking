package attributeClustering;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class ClusteringMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {	
	private Map<Integer, Integer> matchingClusters; //key: existing clusterIndex, value: new clusterIndex
	
	int finalIndex;
	//Map<Integer,Integer> usedIndices; //just to start cluster numbering from 0
	private Path[] localFiles; //HDFS
	
	public void configure(JobConf job){
		matchingClusters = new HashMap<>();
		finalIndex = 0;
		//usedIndices = new HashMap<>();
		
        BufferedReader SW;
		try {
			localFiles = DistributedCache.getLocalCacheFiles(job); //HDFS			
			SW = new BufferedReader(new FileReader(localFiles[0].toString())); //HDFS
			String line;
			while ((line = SW.readLine()) != null) {
				String[] lineParams = line.split("\t");
				matchingClusters.put(Integer.parseInt(lineParams[0]), Integer.parseInt(lineParams[1]));
			}
		    SW.close();
		} catch (FileNotFoundException e) {
			System.err.println(e.toString());
		} catch (IOException e) {
			System.err.println(e.toString());
		}
		/*//DEBUGGING
		Iterator<Map.Entry<Integer, Integer>> it = matchingClusters.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<Integer, Integer> pair = (Map.Entry<Integer, Integer>) it.next();
			System.out.println("from: "+pair.getKey()+" to "+pair.getValue());
		}
		*/
	}
	
	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {	
		String line = value.toString();
		String[] pred_clusterIndex = line.split("\t");
		String predicate = pred_clusterIndex[0];
		Integer index = Integer.parseInt(pred_clusterIndex[1]);
		
		Integer finalIndex = getFinalClusterIndex(index);
		//System.out.println("The final index of cluster "+index+" is "+finalIndex);
		output.collect(new Text(predicate), new IntWritable(finalIndex));
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
