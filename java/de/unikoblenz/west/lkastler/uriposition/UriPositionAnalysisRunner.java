package de.unikoblenz.west.lkastler.uriposition;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.NLineInputFormat;

/**
 * MapReduce Job that takes a tab-separated values text file of format:
 * <code>[URI,
 * string-encoding for position ("s","p","o")]</code> and creates a
 * tab-separated values text file of form
 * <code>[URI, # occurrences as subject,- ad predicate ,- as object,
 * - # total occurrences]</code>
 * 
 * @author lkastler
 */
public class UriPositionAnalysisRunner {

	/**
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf(UriPositionAnalysisRunner.class);
		// job name
		conf.setJobName("UriPositionAnalysis");

		// set in and out formats
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Position.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(PositionCounts.class);

		// set mapper and reducer
		conf.setMapperClass(UriPositionMapper.class);
		conf.setReducerClass(UriPositionReducer.class);

		// set input format
		conf.setInputFormat(NLineInputFormat.class);
		conf.setInt("mapreduce.input.lineinputformat.linespermap", 100000);
		
		// set output format
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		// set number of reducer tasks
		conf.setNumReduceTasks(Integer.parseInt(args[2]));

		JobClient.runJob(conf);
	}
}
