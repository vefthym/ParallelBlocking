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

public class ValuesReducer extends MapReduceBase implements Reducer<Text, VIntWritable, Text, VIntArrayWritable> {

	public void reduce(Text _key, Iterator<VIntWritable> values,
			OutputCollector<Text, VIntArrayWritable> output, Reporter reporter) throws IOException {					
		
		Set<VIntWritable> valuesSet = new HashSet<>();		
		while (values.hasNext()) {
			VIntWritable value = values.next();
			valuesSet.add(new VIntWritable(value.get()));			
		}
		
		VIntWritable [] tmp = new VIntWritable[valuesSet.size()];		
		tmp = valuesSet.toArray(tmp);		
		
		output.collect(_key, new VIntArrayWritable(tmp));
	}

}
