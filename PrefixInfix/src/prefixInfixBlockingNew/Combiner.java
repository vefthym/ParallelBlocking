package prefixInfixBlockingNew;

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

public class Combiner extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text _key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		Set<VIntWritable> buf = new HashSet<>();
		reporter.setStatus("Combining...");
		while (values.hasNext()) {
			reporter.progress();
			String val = values.next().toString();
			if (val.startsWith("#")) val = val.substring(1); //don't know why this appears
			//FIXME: Change for D1
			//VIntWritable vIntVal = new VIntWritable(Integer.parseInt(val.split(";;;")[1])); //D2 and D3
			VIntWritable vIntVal = new VIntWritable(val.hashCode()); //D1
			if (!buf.contains(vIntVal)) {
				output.collect(_key, new Text(val));
				buf.add(vIntVal);
			}
		}
	}

}
