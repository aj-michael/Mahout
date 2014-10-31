package edu.rosehulman;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.TupleReducer;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HadoopInputFormat;

/**
 * This class is a simple distributed M/R model generator for performing Naive Bayes text classification tasks. We see
 * how easy it is to perform an efficient M/R job that uses compund registers (3 fields), grouping by two fields and
 * using counters natively. We also see there is no need for a lot of boilerplate code as we can
 * use instances for everything: Reducers, Mappers, ...
 * <p>
 * The output model can later be read by {@link NaiveBayesClassifier}.
 */
@SuppressWarnings({ "rawtypes", "serial" })
public class NaiveBayesGenerate implements Tool, Serializable {

	protected Configuration conf = new Configuration();

	private static Schema INTERMEDIATE_SCHEMA = new Schema("categoryCounter", Fields.parse(
		"category:string, word:string, count:long"
	));

	public static String normalizeWord(String word) {
		return word.replaceAll("\\p{Punct}", "").toLowerCase();
	}

	public int run(String[] args) throws Exception {
		if(args.length != 2) {
			System.out.println("Wrong number of arguments");
			return -1;
		}
		String inputExamples = args[0];
		String output = args[1];

		Configuration conf = new Configuration();
		TupleMRBuilder job = new TupleMRBuilder(conf, "Naive Bayes Model Generator");
		job.addIntermediateSchema(INTERMEDIATE_SCHEMA);
		// perform per-category word count mapping
		
		job.addInput(new Path(inputExamples), new HadoopInputFormat(TextInputFormat.class),
		    new TupleMapper<LongWritable, Text>() {

			    ITuple tuple = new Tuple(INTERMEDIATE_SCHEMA);

			    @Override
			    public void map(LongWritable toIgnore, Text value, TupleMRContext context, Collector collector)
			        throws IOException, InterruptedException {

			    	String[] tokens = value.toString().split("\t");
				    String category = tokens[1];
				    String word = tokens[0];
				    long frequency = Long.parseLong(tokens[2]);
				    tuple.set("category",category);
				    tuple.set("word",word);
				    tuple.set("count",frequency);
				    collector.write(tuple);
			    }
		    });

		TupleReducer countReducer = new TupleReducer<ITuple, NullWritable>() {

			public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context,
			    Collector collector) throws IOException, InterruptedException, TupleMRException {
				long count = 0;
				ITuple outputTuple = null;
				for(ITuple tuple : tuples) {
					count += tuple.getLong("count");
					outputTuple = tuple;
				}
				outputTuple.set("count", count);
				collector.write(outputTuple, NullWritable.get());
			}
		};
		job.setTupleCombiner(countReducer);
		job.setTupleReducer(countReducer);
		job.setGroupByFields("word", "category");
		job.setTupleOutput(new Path(output), INTERMEDIATE_SCHEMA);
		try {
			if(job.createJob().waitForCompletion(true)) {
				return 1;
			}
		} finally {
			job.cleanUpInstanceFiles();
		}
		return -1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new NaiveBayesGenerate(), args);
	}

	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub
		
	}

	public Configuration getConf() {
		// TODO Auto-generated method stub
		return null;
	}
}