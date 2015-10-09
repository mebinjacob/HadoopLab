package edu.ufl.ds;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.ufl.ds.parser.XmlInputFormat;

public class RedLinkRemover extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RedLinkRemover(), args);
		System.exit(res);
	}

	public int run(String args[]) {
		try {
			Configuration conf = new Configuration();

			conf.set("xmlinput.start", "<page>");
			conf.set("xmlinput.end", "</page>");
			conf.set("io.serializations",
					"org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
			Job job = Job.getInstance(conf);
			job.setJarByClass(RedLinkRemover.class);

			job.setMapperClass(RedLinkRemoverMapper.class);

			job.setReducerClass(RedLinkRemoverReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			// specify input and output DIRECTORIES
			FileInputFormat.addInputPath(job, new Path(args[0]));

			job.setInputFormatClass(XmlInputFormat.class);

			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			job.setOutputFormatClass(TextOutputFormat.class);

			if (job.waitForCompletion(true)) {
				return 0;
			} else {
				return 1;
			}
		} catch (InterruptedException | ClassNotFoundException | IOException e) {
			System.err.println("Error during mapreduce job.");
			e.printStackTrace();
			return 2;
		}
	}

}