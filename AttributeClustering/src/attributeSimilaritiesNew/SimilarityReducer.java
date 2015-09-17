package attributeSimilaritiesNew;

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

public class SimilarityReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	
	Text keyToEmit = new Text();
	Text valueToEmit = new Text();

	class Predicate {
		private String mapperId;
		private char dataSourceId;
		private String name;
		private String values;
					
		public Predicate (String input) {
			String [] inputParams = input.split(";;;");
			this.mapperId = inputParams[0]; 
			this.dataSourceId = inputParams[1].charAt(0); //0 (for dbpedia) or 1
			this.name = inputParams[1].substring(1);
			this.values = inputParams[2];
		}
		
		public Predicate (Predicate copy) {
			this.mapperId = copy.getMapperId(); 
			this.dataSourceId = copy.getDataSourceId(); 
			this.name = copy.getName();
			this.values = copy.getValues();
		}

		public String getMapperId() {
			return mapperId;
		}

		public char getDataSourceId() {
			return dataSourceId;
		}

		public String getName() {
			return name;
		}

		public String getValues() {
			return values;
		}
	}
	/**
	 * input:
	 * 	key: a pair determined by totalMappers and currentMapper (separated by underslash"_")
	 * 	value: list of mapperId;;;datasourceID;;;predicate;;;all the values of this predicate
	 * output:
	 * 	key: datasourceIdOfPred1;;;predicate1
	 * 	value: datasourceIdOfPred2;;;predicate2;;;similarity with predicate1
	 */
	public void reduce(Text _key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {			
		
		reporter.setStatus("computing "+_key);
		int left = Integer.parseInt(_key.toString().split("_")[0]);
		int right = Integer.parseInt(_key.toString().split("_")[1]);
				
		reporter.progress();
					
		boolean sameBlock = (left == right) ? true : false; // check or not within the same block		

		ArrayList<Predicate> prevValues = new ArrayList<>();
		while (values.hasNext()) {
			String input = values.next().toString();
			Predicate curr = new Predicate(input);
			reporter.progress();
			String currMapperId = curr.getMapperId(); 
			char currDatasourceId = curr.getDataSourceId(); 
			String currPredicate = curr.getName();
			String currPredValues = curr.getValues();	
			reporter.progress();
			
			for (Predicate prev : prevValues) {				
				String prevMapperId = prev.getMapperId(); 
				char prevDatasourceId = prev.getDataSourceId();
				String prevPredicate = prev.getName();
				String prevPredValues = prev.getValues();
				reporter.progress();
				if (prevDatasourceId != currDatasourceId) { //clean-clean ER
					if (sameBlock) { //check within the same block
						double score = jaccard(currPredValues, prevPredValues);						
						//System.out.println("Similarity of "+currPredicate +" and "+prevPredicate+": "+score);
						if (score > 0) {//saves great space!
							keyToEmit.set(currDatasourceId+currPredicate);
							valueToEmit.set(prevDatasourceId+prevPredicate+";;;"+score);
							output.collect(keyToEmit, valueToEmit); //add the inverse in the next job's mapper	
							/*
							output.collect( //the inverse
								new Text(prevDatasourceId+";;;"+prevPredicate), //key
								new Text(currDatasourceId+";;;"+currPredicate+";;;"+score)); //value
								*/
						}
					} else { //don't check within the same block
						if (!currMapperId.equals(prevMapperId)) {
							double score = jaccard(currPredValues, prevPredValues);
							if (score > 0) { //saves great space!
								keyToEmit.set(currDatasourceId+currPredicate);
								valueToEmit.set(prevDatasourceId+prevPredicate+";;;"+score);
								output.collect(keyToEmit, valueToEmit); //add the inverse in the next job's mapper
							/*output.collect( //the inverse
									new Text(prevDatasourceId+";;;"+prevPredicate), //key
									new Text(currDatasourceId+";;;"+currPredicate+";;;"+score)); //value
									*/
							}
						}
					}
				} //else do nothing (each data source is clean)				
			}
			prevValues.add(new Predicate(curr));
		}
	}
	
	private double jaccard (String values1, String values2) {
		String[] v1 = values1.split(" ");
		String[] v2 = values2.split(" ");
		//System.out.println("v1"+Arrays.toString(v1));
		//System.out.println("v2"+Arrays.toString(v2));
		
		Set<String> intersection = new HashSet<>(Arrays.asList(v1));				
		intersection.retainAll(Arrays.asList(v2));	
		return (double) intersection.size() / (v1.length + v2.length - intersection.size()); 
	}
	
//	private double linda (String values1, String values2) {
//		String[] v1 = values1.split(" ");
//		String[] v2 = values2.split(" ");
//		//System.out.println("v1"+Arrays.toString(v1));
//		//System.out.println("v2"+Arrays.toString(v2));
//		
//		Set<String> intersection = new HashSet<>(Arrays.asList(v1));				
//		intersection.retainAll(Arrays.asList(v2));
//		return (double) intersection.size() / (Math.min(v1.length, v2.length) + Math.log10(Math.abs(v1.length-v2.length)+1));
//	}
}
