package evaluation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ACBPairsReducer extends MapReduceBase implements Reducer<Text, VIntArrayWritable, Text, Text> {
	
	static enum OutputData {UNIQUE_PAIRS, REDUNDANT_PAIRS};
	
	/**
	 * input:
	 * 	key: token.hashCode()
	 * 	value: list of Arrays a of hashes, where a[0] = dID, a[1] = subject hash and the rest are the previous tokens hashes
	 * output: nothing (just the counters)
	 */
	public void reduce(Text _key, Iterator<VIntArrayWritable> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {			
		List<VIntWritable[]> buf = new ArrayList<>(); //String representations of entity ids (URIs)
		reporter.setStatus("Reducing block "+_key);
		while (values.hasNext()) {
			VIntArrayWritable e1Array = (VIntArrayWritable) values.next();			
			VIntWritable[] e1 = (VIntWritable[]) e1Array.get();			
			reporter.progress();
			//System.out.println("Reducing..."+e1);
			VIntWritable dID1 = e1[0]; //clean-clean ER
			//IntWritable eid1 = e1[1];			
			VIntWritable[] e1Values = Arrays.copyOfRange(e1, 2, e1.length);			
			
			for (VIntWritable[] e2 : buf) {			
				
				reporter.progress();
				if (!e2[0].equals(dID1)){ //clean-clean ER
					//IntWritable eid2 = e2[1];					
					
					//this is the main logic of the reducer!
					if (!doOverlapFast(e1Values,Arrays.copyOfRange(e2, 2, e2.length))) {
						reporter.incrCounter(OutputData.UNIQUE_PAIRS, 1);
						//output.collect(new Text(e1.split(";;;")[1]), new Text(e2.split(";;;")[1]));
					} /*else {
						reporter.incrCounter(OutputData.REDUNDANT_PAIRS, 1);
					}*/
				} //clean-clean ER
			}			
			buf.add(e1);	
		}
	}
	
	/**
	 * adapted from http://stackoverflow.com/questions/7574311/efficiently-compute-intersection-of-two-sets-in-java
	 * @param e1
	 * @param e2
	 * @return true if e1 and e1 overlap (their intersection is non-empty)
	 */
	public boolean doOverlapFast(VIntWritable[] e1, VIntWritable[] e2){
		VIntWritable[] a; //a does not need to be transformed to Set
		Set<VIntWritable> b;
		//assign the smaller one to a and the larger one to b. Complexity: O(|a|)
        if (e1.length <= e2.length) {
            a = e1;
            b = new HashSet<>(Arrays.asList(e2));           
        } else {
            a = e2;
            b = new HashSet<>(Arrays.asList(e1));
        }        
        for (VIntWritable e : a) {
            if (b.contains(e)) { //b needs to be transformed to Set, for faster contains() method
                return true;
            }           
        }
        return false;
	}
	
	public boolean doOverlapNew(VIntWritable[] e1, VIntWritable[] e2){		
		//System.out.println("Checking for overlap:");
		//System.out.println(new VIntArrayWritable(e1).toString());
		//System.out.println(new VIntArrayWritable(e2).toString());
		
		//SOLUTION 1: fastest O(n) but needs more space
		Set<VIntWritable> intersection = new HashSet<>(Arrays.asList(e1));				
		intersection.retainAll(Arrays.asList(e2));		
		
		//System.out.println("overlap? "+ (intersection.isEmpty() ? false : true));
		return intersection.isEmpty() ? false : true;
		
		//SOLUTION 2: slower O(nlogn) but needs less space
//		Arrays.sort(e1); //O(nlogn)
//		for (VIntWritable tmp : e2) { //O(m)
//			if (Arrays.binarySearch(e1, tmp) == -1) //O(logm)
//				return false;
//		}
//		return true;
		
		
			
	}
	
	
	public boolean doOverlap(String e1, String e2){
		String[] e1tokens = e1.split(" ");
		String[] e2tokens = e2.split(" ");
		
		Set<String> intersection = new TreeSet<String>(Arrays.asList(e1tokens));
		intersection.retainAll(Arrays.asList(e2tokens));	
		intersection.remove("");
		
		//System.out.println("overlap? "+ (intersection.isEmpty() ? false : true));
		return intersection.isEmpty() ? false : true;	
	}

}
