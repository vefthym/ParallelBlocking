package attributeClusteringBlockingNew;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ACBCombinerNoMatching extends MapReduceBase implements Reducer<Text, VIntWritable, Text, VIntWritable> {

	VIntWritable eidToEmit = new VIntWritable();
	
/**
 * remove duplicate entries
 */
public void reduce(Text _key, Iterator<VIntWritable> values,
		OutputCollector<Text, VIntWritable> output, Reporter reporter) throws IOException {		
	
	Set<Integer> buf = new HashSet<>(); //previous values having the same key
		
	while(values.hasNext()){
		reporter.progress();
		int eid = values.next().get(); //this is just an entity id		
		if (buf.add(eid)) { //remove identical entries (add returns true if the element did NOT exist previously)
			eidToEmit.set(eid);
			output.collect(_key, eidToEmit);			
		} 
	}	
}

}