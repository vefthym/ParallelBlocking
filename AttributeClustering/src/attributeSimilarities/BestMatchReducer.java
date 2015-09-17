package attributeSimilarities;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

public class BestMatchReducer extends MapReduceBase implements Reducer<Text, Text, Text, VIntWritable> {

	private Map<String, Integer> tmpClustering; //key: predicate, value: clusterIndex
	private int clusterIndex;
	private Map<Integer, Integer> matchingClusters; //key: existing clusterIndex, value: new clusterIndex
	MultipleOutputs mos = null;
	
	/**
	 * works with one reducer
	 * keep a list of temporary cluster allocations
	 */
	public void configure(JobConf conf) {
		tmpClustering = new HashMap<>();
		matchingClusters = new HashMap<>();
		clusterIndex = 0; //using clusterIndex -1 for the glue cluster
		mos = new MultipleOutputs(conf);		
	}
	
	/**
	 * emit the key with the value having the highest score
	 * also output 2 extra files in the HDFS:
	 * tmpclusters: the initial clustering (without transitive closure) predicate \t clusterindex
	 * merges: the clusterIndices (from tmpclusters file) that need to be merged
	 */
	public void reduce(Text _key, Iterator<Text> values,
			OutputCollector<Text, VIntWritable> output, Reporter reporter) throws IOException {	

		double bestScore = -1; //even if bestMatch is 0, this qualifies
		String bestPredicate = "";		
		while (values.hasNext()) {
			 String value = values.next().toString();
			 reporter.progress();
			 String[] valueParams = value.split(";;;");
			 reporter.progress();
			 double score = Double.parseDouble(valueParams[2]);
			 if (score > bestScore) {
				 bestScore = score;  
				 bestPredicate = new String(valueParams[0]+";;;"+valueParams[1]);
			 }
		}		
				 
		String keyPredicateName = _key.toString();
		String bestPredicateName = bestPredicate;
		
		if (bestScore > 0.2) { //FIXME: TUNE THIS THRESHOLD (INITIALLY 0)
			//no need to output anything here --> this output is not needed in the next steps
			//output.collect(_key, new Text(bestPredicate)); //output in part-00000
		} else {
			//put it in the glue cluster and exit (merges should not apply here)
			//mos.getCollector("tmpclusters", reporter).collect(new Text(keyPredicateName), new VIntWritable(-1)); //-1 for the glue cluster
			output.collect(new Text(keyPredicateName), new VIntWritable(-1)); //-1 for the glue cluster
			return;
		}
		
		
		/* check clustering */		
		Integer left_cluster = tmpClustering.get(keyPredicateName);
		Integer right_cluster = tmpClustering.get(bestPredicateName);
		//System.out.println("left_cluster:"+ left_cluster);
		//System.out.println("right_cluster:"+ right_cluster);
		
		if (left_cluster == null && right_cluster == null) {
			clusterIndex++;
			//System.out.println("Putting "+keyPredicateName+" to cluster: "+clusterIndex);
			tmpClustering.put(keyPredicateName, clusterIndex);
			//mos.getCollector("tmpclusters", reporter).collect(new Text(keyPredicateName), new VIntWritable(clusterIndex));
			output.collect(new Text(keyPredicateName), new VIntWritable(clusterIndex));
			//System.out.println("Putting "+bestPredicateName+" to cluster: "+clusterIndex);
			tmpClustering.put(bestPredicateName, clusterIndex);
			//mos.getCollector("tmpclusters", reporter).collect(new Text(bestPredicateName), new VIntWritable(clusterIndex));
			output.collect(new Text(bestPredicateName), new VIntWritable(clusterIndex));
		} else if (left_cluster != null && right_cluster == null){
			//System.out.println("Putting "+bestPredicateName+" to cluster: "+left_cluster);
			tmpClustering.put(bestPredicateName, left_cluster);
			//mos.getCollector("tmpclusters", reporter).collect(new Text(bestPredicateName), new VIntWritable(left_cluster));
			output.collect(new Text(bestPredicateName), new VIntWritable(left_cluster));
		} else if (left_cluster == null && right_cluster != null) {
			//System.out.println("Putting "+keyPredicateName+" to cluster: "+right_cluster);
			tmpClustering.put(keyPredicateName, right_cluster);
			//mos.getCollector("tmpclusters", reporter).collect(new Text(keyPredicateName), new VIntWritable(right_cluster));
			output.collect(new Text(keyPredicateName), new VIntWritable(right_cluster));
		} else if (left_cluster == right_cluster) { //they already belong to the same cluster
			//do nothing!
		} else { //write to the merges file			
			reporter.progress();
			Integer left_transitive_cluster = getTransitiveClusterIndex(left_cluster);
			reporter.progress();
			Integer right_transitive_cluster = getTransitiveClusterIndex(right_cluster);
			reporter.progress();
			
			//System.out.println("left_transitive_cluster:"+ left_transitive_cluster);
			//System.out.println("right_transitive_cluster:"+ right_transitive_cluster);
			
			if (left_transitive_cluster != right_transitive_cluster) {
				clusterIndex++;
				//System.out.println("Merging clusters "+left_transitive_cluster+", "+clusterIndex);
				//System.out.println("Merging clusters "+right_transitive_cluster+", "+clusterIndex);
				matchingClusters.put(left_transitive_cluster, clusterIndex);
				reporter.progress();
				matchingClusters.put(right_transitive_cluster, clusterIndex);
				reporter.progress();
				
				mos.getCollector("merges", reporter).collect(new IntWritable(left_transitive_cluster), new VIntWritable(clusterIndex));
				mos.getCollector("merges", reporter).collect(new IntWritable(right_transitive_cluster), new VIntWritable(clusterIndex));
			} //else, they already belong to the same cluster --> do nothing
		}
	}
	
	
	
	private Integer getTransitiveClusterIndex(Integer clusterIndex) {
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
	}
		
	@Override
    public void close() throws IOException {
		 mos.close();
    }

}
