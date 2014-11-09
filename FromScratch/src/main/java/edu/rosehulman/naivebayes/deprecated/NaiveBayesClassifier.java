package edu.rosehulman.naivebayes.deprecated;

import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.tuplemr.IdentityTupleMapper;
import com.datasalt.pangool.tuplemr.IdentityTupleReducer;
import com.datasalt.pangool.tuplemr.MapOnlyJobBuilder;
import com.datasalt.pangool.tuplemr.OrderBy;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleReducer;
import com.datasalt.pangool.tuplemr.mapred.MapOnlyMapper;
import com.datasalt.pangool.tuplemr.mapred.lib.output.TupleTextOutputFormat;
import com.firebase.client.Firebase;
import com.google.common.collect.Sets;

public class NaiveBayesClassifier implements Tool, Serializable {

	private static final long serialVersionUID = -2375691330604388212L;

	static Schema input_schema = new Schema("input schema",
			Fields.parse("word: string, year: int, count: int, dcount: int"));	
	
	static Schema count_schema = new Schema("count schema",
			Fields.parse("year: int, count: int"));
	
	static Schema score_schema = new Schema("score schema",
			Fields.parse("year: int, score: double"));
	
	public void setConf(Configuration conf) { }
	
	public Configuration getConf() {
		return null;
	}

	public void preprocess(String model, final Set<String> words, String filterOutput) throws ClassNotFoundException, IOException, InterruptedException, TupleMRException, URISyntaxException {
		Configuration conf = new Configuration();
		MapOnlyMapper<ITuple,NullWritable,ITuple,NullWritable> mapper = new MapOnlyMapper<ITuple,NullWritable,ITuple,NullWritable>() {
			private static final long serialVersionUID = -5104195321429602029L;
			@Override
			protected void map(ITuple key, NullWritable value, Context context) throws IOException, InterruptedException {
				String word = key.get("word").toString();
				int year = (Integer) key.get("year");
				int count = (Integer) key.get("count");
				if (words.contains(word.toLowerCase())) {
					context.write(key,value);
				}
			}
		};
		MapOnlyJobBuilder job = new MapOnlyJobBuilder(conf,"Filter");
		job.addTupleInput(new Path(model), mapper);
		job.setTupleOutput(new Path(filterOutput), input_schema);
		job.createJob().waitForCompletion(true);
		job.cleanUpInstanceFiles();
	}
	
	public void countByYear(String model, String countOutput) throws ClassNotFoundException, IOException, InterruptedException, TupleMRException, URISyntaxException {
		Configuration conf = new Configuration();
		
		TupleReducer<ITuple, NullWritable> reducer = new TupleReducer<ITuple,NullWritable>() {
			private static final long serialVersionUID = 7208815953760503728L;
			public void reduce(ITuple key, Iterable<ITuple> values, TupleMRContext context, Collector collector) throws IOException, InterruptedException {
				int year = (Integer) key.getInteger("year");
				int count = 0;
				for (ITuple tuple : values) {
					count += (Integer) tuple.getInteger("count");
				}
				ITuple outTuple = new Tuple(count_schema);
				outTuple.set("year",year);
				outTuple.set("count",count);
				collector.write(outTuple, NullWritable.get() );
			}
		};
		
		TupleMRBuilder job = new TupleMRBuilder(conf,"Count");
		job.addTupleInput(new Path(model),new IdentityTupleMapper());
		job.addIntermediateSchema(input_schema);
		job.setGroupByFields("year");
		job.setTupleOutput(new Path(countOutput), count_schema);
		job.setTupleReducer(reducer);
		job.createJob().waitForCompletion(true);
		job.cleanUpInstanceFiles();
	}
	
	public void distributedScore(Set<String> words, final int vocabulary_size, String filtered, String counts, String tempOutput, String scoresOutput) throws TupleMRException, ClassNotFoundException, IOException, InterruptedException {
		Configuration conf = new Configuration();
		TupleReducer<ITuple,NullWritable> reducer = new TupleReducer<ITuple,NullWritable>() {
			private static final long serialVersionUID = -7293758282851758882L;
			public void reduce(ITuple key, Iterable<ITuple> tuples, TupleMRContext context, Collector collector) throws IOException, InterruptedException {
				int year = (Integer) key.get("year");
				int yeartotalwords = 0;
				double score = 0;
				int words = 0;
				for (ITuple tuple : tuples) {
					if (tuple.getSchema().getName().equals("input schema")) {
						int wordcount = (Integer) tuple.get("count") + 1;
						score += Math.log(wordcount);
						words += 1;
					} else if (tuple.getSchema().getName().equals("count schema")) {
						yeartotalwords = (Integer) tuple.get("count") + 1;
					}
				}
				
				// this is an inner join
				if (words == 0) return;
				
				// per the formula
				score -= words * Math.log(yeartotalwords + vocabulary_size);
				
				// construct the output
				ITuple outTuple = new Tuple(score_schema);
				outTuple.set("year", year);
				outTuple.set("score", score);
				collector.write(outTuple, NullWritable.get());
				System.out.println(outTuple);
			}
		};
		TupleMRBuilder job = new TupleMRBuilder(conf,"Distributed Scorer");
		job.addIntermediateSchema(input_schema);
		job.addIntermediateSchema(count_schema);
		job.addTupleInput(new Path(filtered),new IdentityTupleMapper());
		job.addTupleInput(new Path(counts), new IdentityTupleMapper());
		job.setTupleOutput(new Path(tempOutput), score_schema);
		job.setTupleReducer(reducer);
		job.setGroupByFields("year");
		job.createJob().waitForCompletion(true);
		job.cleanUpInstanceFiles();
		
		TupleMRBuilder sort = new TupleMRBuilder(conf,"Sort scores");
		sort.addIntermediateSchema(score_schema);
		sort.addTupleInput(new Path(tempOutput), new IdentityTupleMapper());
		sort.setGroupByFields("score");
		sort.setOrderBy(OrderBy.parse("score:desc"));
		sort.setTupleReducer(new IdentityTupleReducer());
		sort.setOutput(new Path(scoresOutput), new TupleTextOutputFormat(score_schema, true, '\t', TupleTextOutputFormat.NO_QUOTE_CHARACTER, TupleTextOutputFormat.NO_ESCAPE_CHARACTER), ITuple.class, NullWritable.class);
		sort.createJob().waitForCompletion(true);
		sort.cleanUpInstanceFiles();
	}
	
	public int run(String[] args) throws Exception {
		String model = args[0];
		String text = args[1];
		String output = args[2];
		String filterOutput = output+"/filtered";
		String countsOutput = output+"/counts";
		String tempOutput = output+"/temp";
		String scoresOutput = output+"/scores";
		Firebase ref = new Firebase("https://mahout.firebaseio.com");
		final Set<String> words = Sets.newHashSet(text.toLowerCase().split(" "));
		ref.child("status").setValue("phase 1");
		this.preprocess(model, words, filterOutput);
		this.countByYear(model,countsOutput);
		this.distributedScore(words,0,filterOutput,countsOutput,tempOutput,scoresOutput);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new NaiveBayesClassifier(), args);
	}

}
