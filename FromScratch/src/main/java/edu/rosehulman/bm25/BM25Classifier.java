package edu.rosehulman.bm25;

import static edu.rosehulman.bm25.BM25Modeler.IDF_schema;
import static edu.rosehulman.bm25.BM25Modeler.input_schema;
import static edu.rosehulman.bm25.BM25Modeler.readAverageYearCount;
import static edu.rosehulman.bm25.BM25Modeler.words_per_year_schema;

import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
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
import com.datasalt.pangool.tuplemr.MapOnlyJobBuilder;
import com.datasalt.pangool.tuplemr.OrderBy;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.TupleReducer;
import com.datasalt.pangool.tuplemr.mapred.MapOnlyMapper;
import com.datasalt.pangool.tuplemr.mapred.lib.output.TupleTextOutputFormat;
import com.firebase.client.Firebase;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class BM25Classifier implements Tool, Serializable {

	private static final long serialVersionUID = -7355240271344556170L;

	static Schema joined_schema = new Schema("joined schema",Fields.parse("word: string, year: int, count: long, idf: double"));
	static Schema score_schema = new Schema("score schema",Fields.parse("year: int, score: double"));
	static Schema firebase_schema = new Schema("firebase schema",Fields.parse("year: int, score: double, zero: int"));

	public void setConf(Configuration conf) { }

	public Configuration getConf() {
		return new Configuration();
	}

	public void filterFrequencies(final Set<String> words, String input, String output) throws ClassNotFoundException, IOException, InterruptedException, TupleMRException, URISyntaxException {
		Configuration conf = new Configuration();
		MapOnlyMapper<ITuple,NullWritable,ITuple,NullWritable> mapper = new MapOnlyMapper<ITuple,NullWritable,ITuple,NullWritable>() {
			private static final long serialVersionUID = -4330251227649700349L;
			@Override
			protected void map(ITuple key, NullWritable value, Context context) throws IOException, InterruptedException {
				String word = key.getString("word").toString();
				if (words.contains(word.toLowerCase())) {
					context.write(key, value);
				}
			}
		};
		MapOnlyJobBuilder job = new MapOnlyJobBuilder(conf,"yearly frequency filter");
		job.addTupleInput(new Path(input), mapper);
		job.setTupleOutput(new Path(output), input_schema);
		job.createJob().waitForCompletion(true);
		job.cleanUpInstanceFiles();
	}
	
	public void filterIDFs(final Set<String> words, String input, String output) throws ClassNotFoundException, IOException, InterruptedException, TupleMRException, URISyntaxException {
		Configuration conf = new Configuration();
		MapOnlyMapper<ITuple,NullWritable,ITuple,NullWritable> mapper = new MapOnlyMapper<ITuple,NullWritable,ITuple,NullWritable>() {
			private static final long serialVersionUID = -4330251227649700349L;
			@Override
			protected void map(ITuple key, NullWritable value, Context context) throws IOException, InterruptedException {
				String word = key.getString("word").toString();
				if (words.contains(word.toLowerCase())) {
					context.write(key, value);
				}
			}
		};
		MapOnlyJobBuilder job = new MapOnlyJobBuilder(conf,"idf filter");
		job.addTupleInput(new Path(input), mapper);
		job.setTupleOutput(new Path(output), IDF_schema);
		job.createJob().waitForCompletion(true);
		job.cleanUpInstanceFiles();
	}
	
	public void distributedScore(Firebase ref, Set<String> words, final double k1, final double b, String filteredFrequencies, String filteredIDFs, String temp, String yearCount, String output, final double averageYearCount) throws TupleMRException, IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		TupleReducer<ITuple,NullWritable> reducer = new TupleReducer<ITuple,NullWritable>() {
			private static final long serialVersionUID = -2135548052151923858L;
			@Override
			public void reduce(ITuple key, Iterable<ITuple> tuples, TupleMRContext context, Collector collector) throws IOException, InterruptedException {
				List<ITuple> intuples = Lists.newArrayList();
				double idf = 0;
				for (ITuple t : tuples) {
					if (t.getSchema().getName().equals("input schema")) {
						intuples.add(t);
					} else if (t.getSchema().getName().equals("IDF schema")) {
						idf = t.getDouble("idf");
					}
				}
				for (ITuple t : intuples) {
					ITuple out = new Tuple(joined_schema);
					out.set("word",t.get("word"));
					out.set("year",t.get("year"));
					out.set("count",t.get("count"));
					out.set("idf",idf);
					collector.write(out, NullWritable.get());
				}
			}
		};
		TupleMRBuilder job = new TupleMRBuilder(conf,"joiner");
		job.addIntermediateSchema(input_schema);
		job.addIntermediateSchema(IDF_schema);
		job.setGroupByFields("word");
		job.addTupleInput(new Path(filteredFrequencies), new IdentityTupleMapper());
		job.addTupleInput(new Path(filteredIDFs), new IdentityTupleMapper());
		job.setTupleReducer(reducer);
		job.setTupleOutput(new Path(temp+"/joined"), joined_schema);
		job.createJob().waitForCompletion(true);
		job.cleanUpInstanceFiles();

		//ref.child("status").setValue("phase 4");
		
		TupleReducer<ITuple,NullWritable> scorer = new TupleReducer<ITuple,NullWritable>() {
			private static final long serialVersionUID = 2046753994886472655L;
			@Override
			public void reduce(ITuple key, Iterable<ITuple> tuples, TupleMRContext context, Collector collector) throws IOException, InterruptedException {
				int year = (Integer) key.get("year");
				double score = 0;
				long totalwords = 0;
				List<ITuple> cached = Lists.newArrayList();
				for (ITuple t : tuples) {
					if (t.getSchema().getName().equals("words per year schema")) {
						totalwords = t.getLong("count");
					} else {
						cached.add(t);
					}
				}
				for (ITuple t : cached) {
					double idf = (Double) t.get("idf");
					long freq = t.getLong("count");
					score += idf * freq * (k1 + 1) / (freq + k1 * (1 - b + b * ((double)totalwords) / ((double)averageYearCount)));
				}
				ITuple out = new Tuple(score_schema);
				out.set("year", year);
				out.set("score", score);
				collector.write(out, NullWritable.get());
			}	
		};
		
		TupleMRBuilder score = new TupleMRBuilder(conf,"BM25 scorer");
		score.addTupleInput(new Path(temp+"/joined"), new IdentityTupleMapper());
		score.addTupleInput(new Path(yearCount), new IdentityTupleMapper());
		score.addIntermediateSchema(joined_schema);
		score.addIntermediateSchema(words_per_year_schema);
		score.setGroupByFields("year");
		score.setTupleReducer(scorer);
		score.setTupleOutput(new Path(temp+"/scored"), score_schema);
		score.createJob().waitForCompletion(true);
		score.cleanUpInstanceFiles();
		
		//ref.child("status").setValue("phase 5");
		
		TupleMapper<ITuple,NullWritable> firebaseMapper = new TupleMapper<ITuple,NullWritable>() {
			private static final long serialVersionUID = 3432835275171567372L;
			@Override
			public void map(ITuple key, NullWritable values, TupleMRContext context, Collector collector)
					throws IOException, InterruptedException {
				ITuple outTuple = new Tuple(firebase_schema);
				outTuple.set("year", key.get("year"));
				outTuple.set("score", key.get("score"));
				outTuple.set("zero", 0);
				collector.write(outTuple);
			}
		};
		
		TupleReducer<ITuple,NullWritable> firebaseReducer = new TupleReducer<ITuple,NullWritable>() {
			private static final long serialVersionUID = 4345644638524415125L;
			@Override
			public void reduce(ITuple key, Iterable<ITuple> tuples, TupleMRContext context, Collector collector) throws IOException, InterruptedException {
				int rank = 1;
				Map<Integer,Integer> map = Maps.newHashMap();
				int year;
				for (ITuple t : tuples) {
					year = (Integer) t.get("year");
					map.put(rank,year);
					rank++;
					ITuple out = new Tuple(score_schema);
					out.set("year", year);
					out.set("score",t.get("score"));
					collector.write(out, NullWritable.get());
				}
				//Firebase ref = new Firebase("https://mahout.firebaseio.com");
				//ref.child("results").setValue(map);					
			}
		};
		
		TupleMRBuilder sort = new TupleMRBuilder(conf,"Sorts scores");
		sort.addIntermediateSchema(firebase_schema);
		sort.addTupleInput(new Path(temp+"/scored"), firebaseMapper);
		sort.setGroupByFields("zero");
		sort.setOrderBy(OrderBy.parse("zero:desc,score:desc"));
		sort.setTupleReducer(firebaseReducer);
		sort.setOutput(new Path(output), new TupleTextOutputFormat(score_schema,true,'\t',TupleTextOutputFormat.NO_QUOTE_CHARACTER,TupleTextOutputFormat.NO_ESCAPE_CHARACTER), ITuple.class, NullWritable.class);
		sort.createJob().waitForCompletion(true);
		sort.cleanUpInstanceFiles();
	}
	
	public int run(String[] args) throws Exception {
		String model = args[0];
		String text = args[1];
		String output = args[2];
		double k1 = Double.parseDouble(args[3]);
		double b = Double.parseDouble(args[4]);
		final Set<String> words = Sets.newHashSet(text.toLowerCase().split(" "));
		Firebase ref = new Firebase("https://mahout.firebaseio.com");
		//ref.child("status").setValue("phase 1");
		filterFrequencies(words,model+"/filtered",output+"/filteredfrequencies");
		//ref.child("status").setValue("phase 2");
		filterIDFs(words,model+"/idf",output+"/filteredidf");
		//ref.child("status").setValue("phase 3");
		double averageYearCount = readAverageYearCount(model+"/averageyearcount");
		distributedScore(ref,words, k1, b, output+"/filteredfrequencies",output+"/filteredidf",output+"/unsorted",model+"/wordsperyear/total",output+"/scores", averageYearCount);
		//ref.child("status").setValue("complete");
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new BM25Classifier(),args);
	}

}
