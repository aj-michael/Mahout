package edu.rosehulman.dan;

import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.Tool;
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

public abstract class AbstractClassifier implements Tool, Serializable {

	private static final long serialVersionUID = -2375691330604388212L;

	protected Configuration conf = new Configuration();

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

	public static void filter(String model, final Set<String> words, String output) throws ClassNotFoundException, IOException, InterruptedException, TupleMRException, URISyntaxException {
		Configuration conf = new Configuration();
		MapOnlyMapper<ITuple,NullWritable,ITuple,NullWritable> mapper = new MapOnlyMapper<ITuple,NullWritable,ITuple,NullWritable>() {
			private static final long serialVersionUID = -5104195321429602029L;
			@Override
			protected void map(ITuple key, NullWritable value, Context context) throws IOException, InterruptedException {
				String word = key.get("word").toString();
				if (words.contains(word.toLowerCase())) {
					context.write(key,value);
				}
			}
		};
		MapOnlyJobBuilder job = new MapOnlyJobBuilder(conf,"Filter");
		job.addTupleInput(new Path(model), mapper);
		job.setTupleOutput(new Path(output), input_schema);
		job.createJob().waitForCompletion(true);
		job.cleanUpInstanceFiles();
	}

	public static void countByYear(Path model, Path countOutput) throws ClassNotFoundException, IOException, InterruptedException, TupleMRException, URISyntaxException {
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
		job.addTupleInput(model,new IdentityTupleMapper());
		job.addIntermediateSchema(input_schema);
		job.setGroupByFields("year");
		job.setTupleOutput(countOutput, count_schema);
		job.setTupleReducer(reducer);
		job.createJob().waitForCompletion(true);
		job.cleanUpInstanceFiles();
	}

	public abstract int run(String[] args) throws Exception;
}
