package attributeClusteringBlocking;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ACBCombinerNoMatching extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

/**
 * remove duplicate entries
 */
public void reduce(Text _key, Iterator<Text> values,
		OutputCollector<Text, Text> output, Reporter reporter) throws IOException {		
	
	Set<String> buf = new HashSet<>(); //previous values having the same key
		
	while(values.hasNext()){
		reporter.progress();
		String eid = values.next().toString(); //this is just an entity id		
		if (!buf.contains(eid)) { //remove identical entries
			output.collect(_key, new Text(eid));
			buf.add(eid);
		} 
	}	
}

}