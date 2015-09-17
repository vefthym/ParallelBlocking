package attributeClusteringBlocking;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class ACBReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	
	static enum Statistics {COMPARISONS};
	
	
	class Entity {
		private String dataSourceId,entityId,values;		
					
		public Entity (String input) {
			String [] inputParams = input.split(";;;");			
			this.dataSourceId = inputParams[0]; 
			this.entityId = inputParams[1];
			this.values = inputParams[2];
		}
		
		public Entity (Entity copy) {			
			this.dataSourceId = copy.getDataSourceId(); 
			this.entityId = copy.getId();
			this.values = copy.getValues();
		}
				
		public String getDataSourceId() { return dataSourceId; 	}
		public String getId() 			{ return entityId; 		}
		public String getValues() 		{ return values; 		}
	}
	
	private final double simThresh = 0.65;

	public void reduce(Text _key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		
		reporter.setStatus("computing "+_key);		
		
		ArrayList<Entity> prevEntities = new ArrayList<>();
		while (values.hasNext()) {			
			Entity curr = new Entity(values.next().toString());
			reporter.progress();			
			String currDatasourceId = curr.getDataSourceId(); 
			String currEntity = curr.getId();
			String currValues = curr.getValues();	
			reporter.progress();
			
			for (Entity prev : prevEntities) {				 
				String prevDatasourceId = prev.getDataSourceId();
				String prevEntity = prev.getId();
				String prevValues = prev.getValues();
				reporter.progress();
				if (!prevDatasourceId.equals(currDatasourceId)) { //clean-clean ER					
					double score = jaccard(currValues, prevValues);		
					reporter.incrCounter(Statistics.COMPARISONS, 1);
					//System.out.println("Similarity of "+currPredicate +" and "+prevPredicate+": "+score);
					if (score > simThresh) {//saves great space!
						output.collect( 
							new Text(currDatasourceId+";;;"+currEntity), //key
							new Text(prevDatasourceId+";;;"+prevEntity+";;;"+score)); //value								
					}					
				} //else do nothing (each data source is clean)
			} //end of for all prevEntities
			
			prevEntities.add(new Entity(curr));
		} //end of while(values.hasNext())
	}
	
	private double jaccard (String values1, String values2) {
		String[] v1 = values1.split(", ");
		String[] v2 = values2.split(", ");
		
		Set<String> intersection = new HashSet<>(Arrays.asList(v1));				
		intersection.retainAll(Arrays.asList(v2));	
		return (double) intersection.size() / (v1.length + v2.length - intersection.size()); 
	}

}
