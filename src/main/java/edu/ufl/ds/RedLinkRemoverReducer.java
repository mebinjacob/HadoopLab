package edu.ufl.ds;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RedLinkRemoverReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		boolean found = false;
		for (Text val : values) {
			if (key.equals(val)) {
				found = true;
				break;
			}
		}
		boolean firstTime = true;
		if (found) {
			for (Text val : values) {
				if (!val.equals(key) && firstTime) {
					context.write(key, val);
				}
				else{
					firstTime = false;
				}
			}
		}
	}

}
