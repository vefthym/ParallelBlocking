package evaluation;

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

public class ACBEvalCombiner extends MapReduceBase implements Reducer<Text, VIntArrayWritable, Text, VIntArrayWritable> {

/**
 * remove duplicate entries
 */
public void reduce(Text _key, Iterator<VIntArrayWritable> values,
		OutputCollector<Text, VIntArrayWritable> output, Reporter reporter) throws IOException {		
	
	Set<String> buf = new HashSet<>(); //previous values having the same key
		
	while(values.hasNext()){
		VIntArrayWritable e1Array = (VIntArrayWritable) values.next();			
		VIntWritable[] e1 = (VIntWritable[]) e1Array.get();			
		reporter.progress();
		VIntWritable dID1 = e1[0]; 
		VIntWritable eid1 = e1[1];		
		reporter.progress();		
		if (!buf.contains(dID1.toString()+eid1.toString())) { //remove identical entries
			output.collect(_key, e1Array);
			buf.add(dID1.toString()+eid1.toString());
		} 
	}	
}

}