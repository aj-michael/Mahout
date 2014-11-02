package edu.rosehulman;

import java.io.IOException;
import java.io.Serializable;

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
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.TupleReducer;
import com.datasalt.pangool.tuplemr.mapred.lib.input.TupleTextInputFormat;

public class ModelGenerator implements Tool, Serializable {

	private static final long serialVersionUID = -5565331847345488539L;

	static Schema line_schema = new Schema("line schema",
			Fields.parse("line: string"));
	
	static Schema input_schema = new Schema("first schema",
			Fields.parse("word: string, year: int, count: int, dcount: int"));

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new ModelGenerator(), args);
	}

	public void setConf(Configuration conf) {
	}

	public Configuration getConf() {
		return null;
	}

	public int run(String[] args) throws Exception {
		String input = args[0];
		String output = args[1];
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
					int count = Integer.parseInt(tokens[2]);
					int dcount = Integer.parseInt(tokens[3]);
					ITuple outTuple = new Tuple(input_schema);
					outTuple.set("word", word);
					outTuple.set("year", year);
					outTuple.set("count", count);
					outTuple.set("dcount",dcount);
					collector.write(outTuple);
				} catch (Exception e) {
					// do nothing
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
		/*
		job.addInput(new Path(input), new TupleTextInputFormat(input_schema,
				false, false, '\t', TupleTextInputFormat.NO_QUOTE_CHARACTER, TupleTextInputFormat.NO_ESCAPE_CHARACTER, null, "\\NULL"),
				new IdentityTupleMapper());
		*/
		reducer = new IdentityTupleReducer();
		job.setTupleCombiner(reducer);
		job.setTupleReducer(reducer);
		job.setGroupByFields("word", "year");
		job.setTupleOutput(new Path(output), input_schema);
		job.createJob().waitForCompletion(true);
		job.cleanUpInstanceFiles();
		return 0;
	}
}
