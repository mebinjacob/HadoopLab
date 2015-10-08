package deadLinks;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DeadLinkReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		boolean found = false;
		for (Text val : values) {
			if (val.toString().equals("===")) {
				found = true;
				break;
			}
		}
		if (found) {
			StringBuffer sb = new StringBuffer();
			for (Text val : values) {
				if (!val.toString().equals("===")) {
					sb.append(" " + val.toString());
				}
			}
			context.write(key, new Text(sb.toString()));
		}
	}

}
