/**
 * Copyright [2012] [Datasalt Systems S.L.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasalt.pangool.examples.simplesecondarysort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import com.datasalt.pangool.examples.BaseExampleJob;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Schema.Field;
import com.datasalt.pangool.io.Schema.Field.Type;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.tuplemr.Criteria.Order;
import com.datasalt.pangool.tuplemr.OrderBy;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.TupleReducer;
import com.datasalt.pangool.tuplemr.mapred.lib.input.HadoopInputFormat;
import com.datasalt.pangool.tuplemr.mapred.lib.output.HadoopOutputFormat;

/**
 * Like original Hadoop's SecondarySort example. Reads a space-separated text file with two numbers, groups by the first
 * and sorts by both.
 */
public class SimpleSecondarySort extends BaseExampleJob {

	static Schema getSchema() {
		// Configure schema, sort and group by
		List<Field> fields = new ArrayList<Field>();
		fields.add(Field.create("first", Type.INT));
		fields.add(Field.create("second", Type.INT));

		return new Schema("my_schema", fields);
	}

	@SuppressWarnings("serial")
	private static class IProcessor extends TupleMapper<LongWritable, Text> {

		private Tuple tuple = new Tuple(getSchema());

		@Override
		public void map(LongWritable key, Text value, TupleMRContext context, Collector collector)
		    throws IOException, InterruptedException {
			String[] fields = value.toString().trim().split(" ");
			tuple.set("first", Integer.parseInt(fields[0]));
			tuple.set("second", Integer.parseInt(fields[1]));
			collector.write(tuple);
		}
	}

	@SuppressWarnings("serial")
	public static class Handler extends TupleReducer<Text, NullWritable> {

		@Override
		public void reduce(ITuple group, Iterable<ITuple> tuples, TupleMRContext context, Collector collector)
		    throws IOException, InterruptedException, TupleMRException {

			for(ITuple tuple : tuples) {
				collector.write(new Text(tuple.get("first") + "\t" + tuple.get("second")), NullWritable.get());
			}
		}
	}

	public SimpleSecondarySort() {
		super("Usage: [input] [output]");
	}

	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 2) {
			failArguments("Invalid number of arguments");
			return -1;
		}
		String input = args[0];
		String output = args[1];

		delete(output);

		TupleMRBuilder builder = new TupleMRBuilder(conf);
		builder.addIntermediateSchema(getSchema());
		builder.setGroupByFields("first");
		builder.setOrderBy(new OrderBy().add("first", Order.ASC).add("second", Order.ASC));
		// Input / output and such
		builder.setTupleReducer(new Handler());
		builder.setOutput(new Path(output), new HadoopOutputFormat(TextOutputFormat.class), Text.class,
		    NullWritable.class);
		builder.addInput(new Path(input), new HadoopInputFormat(TextInputFormat.class), new IProcessor());

		try {
			builder.createJob().waitForCompletion(true);
		} finally {
			builder.cleanUpInstanceFiles();
		}

		return 1;
	}

	public static void main(String args[]) throws Exception {
		ToolRunner.run(new SimpleSecondarySort(), args);
	}
}
