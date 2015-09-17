package attributeSimilaritiesNew;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class SimilarityMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
		
	int totalMappers;
	private int currentMapper;
	static enum InputData { NOT_A_VALID_KEY };
	
	Text keyToEmit = new Text();
	Text toEmit = new Text();
	
	public void configure(JobConf conf) {		
		totalMappers = Integer.parseInt(conf.get("mapred.map.tasks")); //only works with the jar, not from eclipse-plugin!!!
        currentMapper = Integer.parseInt(conf.get("mapred.task.partition"));
	}
	

	/**
	 * input = the output of AttributesMapper:
	 * 	input key: datasourceIDpredicate (datasourceId is either 0 or 1)
	 * 	input value: all the values of this predicate (taking data source into account)
	 * output
	 * 	key: a pair determined by totalMappers and currentMapper (separated by underslash"_")
	 * 	value: mapperID;;;a copy of the input (both input key and input value separated by ";;;")
	 */
	public void map(Text key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {			
		String actualKey = key.toString();
		String actualValue = value.toString().trim();
						
		toEmit.set(currentMapper+";;;"+actualKey+";;;"+actualValue);		

		for (int i = 0; i < totalMappers; ++i) {			
			if (currentMapper < i) {				
				keyToEmit.set(Integer.toString(currentMapper)+"_"+i);
				output.collect(keyToEmit, toEmit);
			} else {
				keyToEmit.set(i+"_"+Integer.toString(currentMapper));
				output.collect(keyToEmit, toEmit);
			}
		}		
	}
}
