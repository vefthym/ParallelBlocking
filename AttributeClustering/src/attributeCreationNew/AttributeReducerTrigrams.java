package attributeCreationNew;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import util.Tools;

public class AttributeReducerTrigrams extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	Text trigramsToEmit = new Text();
	
	public void reduce(Text _key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {		

		
		//from wikipedia - the last ones (after "men") are mine (web-specific)
		//String[] stop3gramsArray = {"the","and","tha","ent","ing","ion","tio","for","nde","has","nce","edt","tis","oft","sth","men", "www", "com", "org", "rdf", "xml", "owl"}; //FIXME: update dynamically
		//Set<String> stop3grams = new HashSet<>(Arrays.asList(stop3gramsArray));		
		
		Set<String> trigramsSet = new HashSet<>();
		while (values.hasNext()) { //this is the object of one triple (many words)			
			String value = values.next().toString(); //split the value field into tokens
			value = value.trim().replaceAll("[ ]+", "_"); //replace white spaces with '_'		

			reporter.setStatus(_key+" starting trigramization...");
			Set<String> value3grams = Tools.getTrigrams(value);
			reporter.setStatus("Finished trigramization!");
			for (String trigram: value3grams) {
				//if (stop3grams.contains(trigram)) {	continue; }			
				trigramsSet.add(trigram);			
				reporter.progress();
			}			
		}
		reporter.setStatus("Start emitting...");
		
		if (trigramsSet.isEmpty()) {
			return;
		}
		
		StringBuilder toEmit = new StringBuilder();
		for (String value : trigramsSet) {
			toEmit.append(" ").append(value);
			reporter.progress();
		}		
		trigramsToEmit.set(toEmit.toString().trim());
		output.collect(_key, trigramsToEmit);
	}
}
