package edu.rosehulman;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.io.TupleFile;
import com.datasalt.pangool.tuplemr.MapOnlyJobBuilder;
import com.datasalt.pangool.tuplemr.mapred.MapOnlyMapper;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HadoopInputFormat;
import com.datasalt.pangool.tuplemr.mapred.lib.output.HadoopOutputFormat;

@SuppressWarnings("serial")
public class NaiveBayesClassifier implements Serializable, Tool {

	public final static Charset UTF8 = Charset.forName("UTF-8");
	protected Configuration conf;
	
	private static final Long ZERO = new Long(0);
	Map<String, Long> tokensPerCategory = new HashMap<String, Long>();
	Map<String, Map<String, Long>> wordCountPerCategory = new HashMap<String, Map<String, Long>>();
	long V;

	public NaiveBayesClassifier() { }

	public String classify(String text, Path generatedModel) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(conf);
		// Read tuples from generate job
		StringTokenizer itr = new StringTokenizer(text);
		Set<String> textSet = new HashSet<String>(text.length()*5);
		while(itr.hasMoreTokens()){
			String key = itr.nextToken();
			if(textSet.contains(key)){
				textSet.add(key);
			}
		}
		V = 0;

		for(FileStatus fileStatus : fileSystem.globStatus(generatedModel)) {
			TupleFile.Reader reader = new TupleFile.Reader(fileSystem, conf, fileStatus.getPath());
			Tuple tuple = new Tuple(reader.getSchema());
			while(reader.next(tuple)) {
				// Read Tuple
				long count = tuple.getLong("count");
				String category = tuple.get("category").toString();
				String word = tuple.get("word").toString();
				V += count;
				if(textSet.contains(word)){
					if(!wordCountPerCategory.containsKey(category)){
						wordCountPerCategory.put(category,new HashMap<String,Long>());
					}
					wordCountPerCategory.get(category).put(word, count);				
				}
				tokensPerCategory.put(category, MapUtils.getLong(tokensPerCategory, category,ZERO) + count);

			}
			reader.close();
		}

		itr = new StringTokenizer(text);
		Map<String, Double> scorePerCategory = new HashMap<String, Double>();
		double bestScore = Double.NEGATIVE_INFINITY;
		String bestCategory = null;
		while(itr.hasMoreTokens()) {
			String token = ModelGenerator.normalizeWord(itr.nextToken());
			for(String category : wordCountPerCategory.keySet()) {
				long count = MapUtils.getLong(wordCountPerCategory.get(category), token,ZERO) + 1;
				double wordScore  = Math.log(count / (double) (tokensPerCategory.get(category) + V));
				double totalScore = MapUtils.getDouble(scorePerCategory, category, 0.) + wordScore;
				if(totalScore > bestScore) {
					bestScore = totalScore;
					bestCategory = category;
				}
				scorePerCategory.put(category, totalScore);
			}
		}
		return bestCategory;
	}

	public int run(String[] args) throws Exception {
		if(args.length != 3) {
			System.err.println("Wrong number of arguments");
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}
		final String modelFolder = args[0];
		String input = args[1];
		String output = args[2];
		Configuration conf = new Configuration();
		FileSystem.get(conf).delete(new Path(output),true);

		MapOnlyJobBuilder job = new MapOnlyJobBuilder(conf);
		job.setOutput(new Path(output), new HadoopOutputFormat(TextOutputFormat.class), Text.class, NullWritable.class);
		job.addInput(new Path(input), new HadoopInputFormat(TextInputFormat.class), new MapOnlyMapper<LongWritable, Text, Text, NullWritable>() {
			protected void map(LongWritable key, Text value, Context context) throws IOException ,InterruptedException {
				value.set(value.toString() + "\t" + classify(value.toString(),new Path(modelFolder)));
				context.write(value, NullWritable.get());
			}
		});

		Job j = job.createJob();
		try {
			j.waitForCompletion(true);
		} finally {
			job.cleanUpInstanceFiles();
		}

		return 1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new NaiveBayesClassifier(), args);
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	public Configuration getConf() {
		return this.conf;
	}
}
