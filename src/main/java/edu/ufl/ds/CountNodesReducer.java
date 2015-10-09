package edu.ufl.ds;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountNodesReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		int count = 0;
		for (Text v : values) {
			count++;
		}

		context.write(new Text("N= "), new Text(count + ""));
	}

}
