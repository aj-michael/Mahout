package edu.rosehulman.bm25;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.tuplemr.IdentityTupleMapper;
import com.datasalt.pangool.tuplemr.MapOnlyJobBuilder;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.TupleReducer;
import com.datasalt.pangool.tuplemr.mapred.MapOnlyMapper;
import com.datasalt.pangool.tuplemr.mapred.lib.input.TupleTextInputFormat;
import com.datasalt.pangool.tuplemr.mapred.lib.output.TupleTextOutputFormat;

public class BM25Modeler implements Tool, Serializable {

	private static final long serialVersionUID = 8730911456123262901L;
	
	static Schema line_schema = new Schema("line schema",Fields.parse("line: string"));
	static Schema number_schema = new Schema("number schema",Fields.parse("value: int"));
	static Schema input_schema = new Schema("input schema",Fields.parse("word: string, year: int, count: int"));
	static Schema years_per_word_schema = new Schema("years per word schema",Fields.parse("word: string, count: int"));
	static Schema words_per_year_schema = new Schema("words per year schema",Fields.parse("year: int, count: int"));

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new BM25Modeler(), args);
	}

	public void setConf(Configuration conf) { }

	public Configuration getConf() {
		return null;
	}

	public void wordsByYear(String input, String output) {}
	
	public void filterBadRecords(String input, String output) throws ClassNotFoundException, IOException, InterruptedException, TupleMRException, URISyntaxException {
		Configuration conf = new Configuration();
		MapOnlyMapper<ITuple,NullWritable,ITuple,NullWritable> mapper = new MapOnlyMapper<ITuple,NullWritable,ITuple,NullWritable>(){
			
			private static final long serialVersionUID = 8368049550008622173L;

			@Override
			public void map(ITuple key, NullWritable value, Context context)
					throws IOException, InterruptedException {
				String line = (String)key.get("line");
				try {
					String[] tokens = line.split("\t");
					String word = tokens[0];
					int year = Integer.parseInt(tokens[1]);
					int count = Integer.parseInt(tokens[2]);
					ITuple outTuple = new Tuple(input_schema);
					outTuple.set("word",word);
					outTuple.set("year", year);
					outTuple.set("count",count);
					context.write(outTuple,NullWritable.get());
				} catch (Exception e) {
					// flawless coding practice right here
				}
			}
			
		};
		MapOnlyJobBuilder job = new MapOnlyJobBuilder(conf,"filter");
		job.addInput(new Path(input),new TupleTextInputFormat(line_schema,false,false,TupleTextInputFormat.NO_SEPARATOR_CHARACTER,TupleTextInputFormat.NO_QUOTE_CHARACTER,TupleTextInputFormat.NO_ESCAPE_CHARACTER,null,"\\NULL"),mapper);
		job.setTupleOutput(new Path(output), input_schema);
		job.createJob().waitForCompletion(true);
		job.cleanUpInstanceFiles();
		
	}
	
	public void yearsPerWord(String input, String output) throws TupleMRException, ClassNotFoundException, IOException, InterruptedException {
		Configuration conf = new Configuration();
		TupleReducer<ITuple,NullWritable> reducer = new TupleReducer<ITuple,NullWritable>() {
			private static final long serialVersionUID = 5717147519795618810L;
			public void reduce(ITuple key, Iterable<ITuple> tuples, TupleMRContext context, Collector collector) throws IOException, InterruptedException {
				int count = 0;
				for (ITuple t : tuples) {
					count++;
				}
				ITuple outTuple = new Tuple(years_per_word_schema);
				outTuple.set("word",key.get("word"));
				outTuple.set("count",count);
				collector.write(outTuple, NullWritable.get());
			}
		};
		TupleMRBuilder job = new TupleMRBuilder(conf,"years per word");
		job.addTupleInput(new Path(input), new IdentityTupleMapper());
		job.addIntermediateSchema(input_schema);
		job.setTupleOutput(new Path(output), years_per_word_schema);
		job.setTupleReducer(reducer);
		job.setGroupByFields("word");
		job.createJob().waitForCompletion(true);
		job.cleanUpInstanceFiles();
	}

	public void wordsPerYear(String input, String output) throws TupleMRException, ClassNotFoundException, IOException, InterruptedException {
		Configuration conf = new Configuration();
		TupleReducer<ITuple,NullWritable> reducer = new TupleReducer<ITuple,NullWritable>() {
			private static final long serialVersionUID = -8360227290180720239L;
			public void reduce(ITuple key, Iterable<ITuple> tuples, TupleMRContext context, Collector collector) throws IOException, InterruptedException {
				int year = (Integer) key.get("year");
				int count = 0;
				for (ITuple t : tuples) {
					count++;
				}
				ITuple outTuple = new Tuple(words_per_year_schema);
				outTuple.set("year", year); 
				outTuple.set("count", count);
				collector.write(outTuple, NullWritable.get());
			}
		};
		TupleMRBuilder job = new TupleMRBuilder(conf,"words per year");
		job.addTupleInput(new Path(input), new IdentityTupleMapper());
		job.addIntermediateSchema(input_schema);
		job.setTupleOutput(new Path(output), words_per_year_schema);
		job.setTupleReducer(reducer);
		job.setGroupByFields("year");
		job.createJob().waitForCompletion(true);
		job.cleanUpInstanceFiles();
	}

	public void countYears(String input, String output) throws ClassNotFoundException, IOException, InterruptedException, TupleMRException, URISyntaxException {
		Configuration conf = new Configuration();
		TupleMapper<ITuple,NullWritable> mapper = new TupleMapper<ITuple,NullWritable>() {
			private static final long serialVersionUID = 7457869387208667293L;
			@Override
			public void map(ITuple key, NullWritable value, TupleMRContext context, Collector collector)
					throws IOException, InterruptedException {
				ITuple outTuple = new Tuple(number_schema);
				outTuple.set("value", 1);
				collector.write(outTuple);
			}
		};
		TupleReducer<ITuple,NullWritable> reducer = new TupleReducer<ITuple,NullWritable>() {
			private static final long serialVersionUID = -5654351407813197875L;
			@Override
			public void reduce(ITuple key, Iterable<ITuple> tuples, TupleMRContext context, Collector collector) throws IOException, InterruptedException {
				int count = 0;
				for (ITuple t : tuples) {
					count++;
				}
				ITuple outTuple = new Tuple(number_schema);
				outTuple.set("value", count);
				collector.write(outTuple, NullWritable.get());
			}
		};
		TupleMRBuilder job = new TupleMRBuilder(conf,"year count");
		job.addTupleInput(new Path(input), mapper);
		job.addIntermediateSchema(number_schema);
		job.setGroupByFields("value");
		job.setTupleReducer(reducer);
		job.setOutput(new Path(output), new TupleTextOutputFormat(number_schema, false, '\t', TupleTextOutputFormat.NO_QUOTE_CHARACTER, TupleTextOutputFormat.NO_ESCAPE_CHARACTER), ITuple.class, NullWritable.class);
		job.createJob().waitForCompletion(true);
		job.cleanUpInstanceFiles();
	}
	
	public int readYearCount(String input) throws FileNotFoundException, IllegalArgumentException, IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		for (FileStatus fileStatus : fs.listStatus(new Path(input))) {
			
		}
		return 0;
	}
	
	public int run(String[] args) throws Exception {
		String input = args[0];
		String filtered = args[1];
		String yearsPerWord = args[2];
		String wordsPerYear = args[3];
		String yearCount = args[4];
		filterBadRecords(input,filtered);
		yearsPerWord(filtered,yearsPerWord);
		wordsPerYear(filtered,wordsPerYear);
		countYears(wordsPerYear,yearCount);
		int numyears = readYearCount(yearCount);
		return 0;
	}

}