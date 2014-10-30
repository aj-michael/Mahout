package edu.rosehulman;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.TupleReducer;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HadoopInputFormat;

public class ModelGenerator implements Tool, Serializable {

	private static final long serialVersionUID = 1L;
	
	private static Schema INTERMEDIATE_SCHEMA = new Schema("categoryCounter", Fields.parse("year:string, word:string, count:long"));
	
	private Configuration conf;
	
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	public Configuration getConf() {
		return this.conf;
	}
	
	public static String normalizeWord(String word) {
		return word.replaceAll("\\p{Punct}","").toLowerCase();
	}

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("This function requires two arguments");
			return -1;
		}
		
		String input = args[0];
		String output = args[1];
		FileSystem.get(conf).delete(new Path(output), true);
		
		TupleMRBuilder job = new TupleMRBuilder(conf,"Naive Bayes Model Generator");
		job.addIntermediateSchema(INTERMEDIATE_SCHEMA);
		
		TupleMapper<LongWritable, Text> mapper = new TupleMapper<LongWritable, Text>() {

			private static final long serialVersionUID = 1L;
			ITuple tuple = new Tuple(INTERMEDIATE_SCHEMA);

			@Override
			public void map(LongWritable key, Text value, TupleMRContext context, Collector collector)
					throws IOException, InterruptedException {
				String[] tokens = value.toString().split("\t");
				String word = tokens[0];
				String year = tokens[1];
				String docs = tokens[2];
				long count = Long.parseLong(tokens[3]);
				tuple.set("year",year);
				tuple.set("word",word);
				tuple.set("count",count);
				collector.write(tuple);
			}
		};
		
		TupleReducer<ITuple, NullWritable> reducer = new TupleReducer<ITuple,NullWritable>() {
		

			private static final long serialVersionUID = 1L;

			public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector) {
				
			}
		};
		
		job.addInput(new Path(input), new HadoopInputFormat(TextInputFormat.class),mapper);
		job.setTupleCombiner(reducer);
		job.setTupleReducer(reducer);
		job.setTupleOutput(new Path(output), INTERMEDIATE_SCHEMA);
		try {
			return job.createJob().waitForCompletion(true) ? 1 : 0;
		} finally {
			job.cleanUpInstanceFiles();
		}
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new ModelGenerator(), args);
	}

}
