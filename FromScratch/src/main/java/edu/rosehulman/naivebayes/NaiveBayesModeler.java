package edu.rosehulman.naivebayes;

import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;

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
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.TupleReducer;
import com.datasalt.pangool.tuplemr.mapred.lib.input.TupleTextInputFormat;

public class NaiveBayesModeler implements Tool, Serializable {

	private static final long serialVersionUID = -5565331847345488539L;

	static Schema line_schema = new Schema("line schema",
			Fields.parse("line: string"));
	
	static Schema input_schema = new Schema("first schema",
			Fields.parse("word: string, year: int, count: long, dcount: long"));

	static Schema count_schema = new Schema("count schema",
			Fields.parse("year: int, count: long"));
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new NaiveBayesModeler(), args);
	}

	public void setConf(Configuration conf) {
	}

	public Configuration getConf() {
		return null;
	}
	
	public void countByYear(String model, String countOutput) throws ClassNotFoundException, IOException, InterruptedException, TupleMRException, URISyntaxException {
		Configuration conf = new Configuration();
		
		TupleReducer<ITuple, NullWritable> reducer = new TupleReducer<ITuple,NullWritable>() {
			private static final long serialVersionUID = 7208815953760503728L;
			public void reduce(ITuple key, Iterable<ITuple> values, TupleMRContext context, Collector collector) throws IOException, InterruptedException {
				int year = (Integer) key.getInteger("year");
				long count = 0;
				for (ITuple tuple : values) {
					count += tuple.getLong("count");
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
	
	public void filter(String input, String output) throws TupleMRException, IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		TupleMRBuilder job = new TupleMRBuilder(conf, "Model Generator");
		job.addIntermediateSchema(input_schema);
		TupleMapper<ITuple,NullWritable> mapper = new TupleMapper<ITuple,NullWritable>() {
			private static final long serialVersionUID = -5833328716962384147L;
			@Override
			public void map(ITuple key, NullWritable value,
					TupleMapper<ITuple, NullWritable>.TupleMRContext context,
					com.datasalt.pangool.tuplemr.TupleMapper.Collector collector)
					throws IOException, InterruptedException {
				String line = (String)key.get("line");
				try {
					String[] tokens = line.split("\t");
					String word = tokens[0];
					int year = Integer.parseInt(tokens[1]);
					long count = Long.parseLong(tokens[2]);
					long dcount = Long.parseLong(tokens[3]);
					ITuple outTuple = new Tuple(input_schema);
					outTuple.set("word", word);
					outTuple.set("year", year);
					outTuple.set("count", count);
					outTuple.set("dcount",dcount);
					collector.write(outTuple);
				} catch (Exception e) {
					// great coding practice right here
				}
				
			}
			
		};
		TupleReducer<ITuple, NullWritable> reducer = new TupleReducer<ITuple, NullWritable>() {
			private static final long serialVersionUID = -3998304148648759758L;

			public void reduce(ITuple group, Iterable<ITuple> tuples,
					TupleMRContext context, Collector collector)
					throws IOException, InterruptedException {
				int count = 0;
				ITuple outputTuple = null;
				for (ITuple tuple : tuples) {
					count += (Integer) tuple.get("count");
					outputTuple = tuple;
				}
				outputTuple.set("count", count);
				collector.write(outputTuple, NullWritable.get());
			}
		};
		job.addInput(new Path(input), new TupleTextInputFormat(line_schema,
				false, false, TupleTextInputFormat.NO_SEPARATOR_CHARACTER, TupleTextInputFormat.NO_QUOTE_CHARACTER, TupleTextInputFormat.NO_ESCAPE_CHARACTER, null, "\\NULL"),
				mapper);
		reducer = new IdentityTupleReducer();
		job.setTupleCombiner(reducer);
		job.setTupleReducer(reducer);
		job.setGroupByFields("word", "year");
		job.setTupleOutput(new Path(output), input_schema);
		job.createJob().waitForCompletion(true);
		job.cleanUpInstanceFiles();
	}

	public int run(String[] args) throws Exception {
		String input = args[0];
		String output = args[1];
		filter(input,output+"/filtered");
		countByYear(output+"/filtered",output+"/counts");
		return 0;
	}
}
