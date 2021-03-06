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
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.TupleReducer;
import com.datasalt.pangool.tuplemr.mapred.lib.output.HadoopOutputFormat;

@SuppressWarnings("serial")
public class NaiveBayesClassifier implements Serializable, Tool {

	public final static Charset UTF8 = Charset.forName("UTF-8");
//	protected Configuration conf = new Configuration();
	
	private static final Long ZERO = new Long(0);
	Map<String, Long> tokensPerCategory = new HashMap<String, Long>();
	Map<String, Map<String, Long>> wordCountPerCategory = new HashMap<String, Map<String, Long>>();
	long V;

	public NaiveBayesClassifier() { }

	/**
	 * Naive Bayes Text Classification with Add-1 Smoothing
	 * @param text Input text
	 * @return the best Category
	 * @throws TupleMRException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public String classify(String text, Path generatedModel,Path outputPath) throws IOException, ClassNotFoundException, InterruptedException, TupleMRException {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(conf);
		// Read tuples from generate job
		StringTokenizer itr = new StringTokenizer(text);
		final Set<String> textSet = new HashSet<String>(text.length()*5);
		while(itr.hasMoreTokens()){
			String key = itr.nextToken();
			if(textSet.contains(key)){
				textSet.add(key);
			}
		}
		V = 0;

		for(FileStatus fileStatus : fileSystem.globStatus(generatedModel)) {
			TupleMRBuilder job = new TupleMRBuilder(conf);
			job.addTupleInput(fileStatus.getPath(),new TupleMapper<ITuple,NullWritable>(){

				@Override
				public void map(ITuple tuple, NullWritable n, TupleMapper<ITuple, NullWritable>.TupleMRContext con,TupleMapper.Collector col) throws IOException, InterruptedException {
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
			});
			job.addIntermediateSchema(ModelGenerator.INTERMEDIATE_SCHEMA);
			job.setGroupByFields("year");
			job.setTupleReducer(new TupleReducer());
			job.setOutput(outputPath, new HadoopOutputFormat(NullOutputFormat.class), NullWritable.class, NullWritable.class);
			job.setTupleOutput(outputPath,ModelGenerator.INTERMEDIATE_SCHEMA);
			if(!job.createJob().waitForCompletion(true)){
				throw new Error("classify job failed");
			}
/*			TupleFile.Reader reader = new TupleFile.Reader(fileSystem, conf, fileStatus.getPath());
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
			reader.close();*/
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
		FileSystem fs = FileSystem.get(conf);
		for (FileStatus file : fs.listStatus(new Path(input))) {
			System.out.println("This should only run once");
			FSDataInputStream in = fs.open(file.getPath());
			String fullInputText = IOUtils.toString(in,"UTF-8");
			String category = classify(fullInputText,new Path(modelFolder),new Path(output));
			System.out.println(category);
		}
		return 1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new NaiveBayesClassifier(), args);
	}

	public void setConf(Configuration conf) {
		//this.conf = conf;
	}

	public Configuration getConf() {
		return new Configuration();
	}
}
