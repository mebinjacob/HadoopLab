package deadLinks;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import parser.XmlInputFormat;

public class DeadLink extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new DeadLink(), args);
		System.exit(res);
	}

	public int run(String args[]) {
		try {
			Configuration conf = new Configuration();

			conf.set("xmlinput.start", "<page>");
			conf.set("xmlinput.end", "</page>");
			conf.set(
					"io.serializations",
					"org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");

			Job job = Job.getInstance(conf);
			job.setJarByClass(DeadLink.class);

			// specify a mapper
			job.setMapperClass(DeadLinkMapper.class);

			// specify a reducer
			job.setReducerClass(DeadLinkReducer.class);

			// specify output types
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			// specify input and output DIRECTORIES
			FileInputFormat.addInputPath(job, new Path(args[0]));
			// job.setInputFormatClass(TextInputFormat.class);
			job.setInputFormatClass(XmlInputFormat.class);

			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.setOutputFormatClass(TextOutputFormat.class);

			if (job.waitForCompletion(true)) {
				return inlinkToOutlinkJob(args[1],
						"src/main/resources/output_task_2") ? 0 : 1;

			} else {
				return 1;
			}
		} catch (InterruptedException | ClassNotFoundException | IOException e) {
			System.err.println("Error during mapreduce job.");
			e.printStackTrace();
			return 2;
		}
	}

	public static boolean inlinkToOutlinkJob(String inputPath, String outputPath)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(DeadLink.class);

		// specify a mapper
		job.setMapperClass(InlinkToOutlinkMapper.class);

		// specify a reducer
		job.setReducerClass(InlinkToOutlinkReducer.class);

		// specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// specify input and output DIRECTORIES
		FileInputFormat.addInputPath(job, new Path(inputPath));
		// job.setInputFormatClass(TextInputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.setOutputFormatClass(TextOutputFormat.class);

		return job.waitForCompletion(true);

	}
}