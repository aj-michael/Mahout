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
package com.datasalt.pangool.tuplemr;

import static com.datasalt.pangool.tuplemr.TupleMRException.failIfEmpty;
import static com.datasalt.pangool.tuplemr.TupleMRException.failIfNull;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.datasalt.pangool.io.DatumWrapper;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.tuplemr.MultipleInputsInterface.Input;
import com.datasalt.pangool.tuplemr.NamedOutputsInterface.Output;
import com.datasalt.pangool.tuplemr.mapred.GroupComparator;
import com.datasalt.pangool.tuplemr.mapred.RollupReducer;
import com.datasalt.pangool.tuplemr.mapred.SimpleCombiner;
import com.datasalt.pangool.tuplemr.mapred.SimpleReducer;
import com.datasalt.pangool.tuplemr.mapred.SortComparator;
import com.datasalt.pangool.tuplemr.mapred.TupleHashPartitioner;
import com.datasalt.pangool.tuplemr.mapred.lib.input.PangoolMultipleInputs;
import com.datasalt.pangool.tuplemr.mapred.lib.input.TupleInputFormat;
import com.datasalt.pangool.tuplemr.mapred.lib.output.ProxyOutputFormat;
import com.datasalt.pangool.tuplemr.mapred.lib.output.TupleOutputFormat;
import com.datasalt.pangool.tuplemr.serialization.TupleSerialization;
import com.datasalt.pangool.utils.InstancesDistributor;

/**
 * 
 * TupleMRBuilder creates Tuple-based Map-Reduce jobs.
 * <p>
 * 
 * One of the key concepts of Tuple-based Map-Reduce is that Hadoop Key-Value pairs are no longer used.Instead,they are
 * replaced by tuples.<br>
 * 
 * Tuples(see {@link ITuple}) are just an ordered list of elements whose types are defined in a {@link Schema}
 * .TupleMRBuilder contains several methods to define how grouping and sorting among tuples will be performed, avoiding
 * the complex task of defining custom binary {@link SortComparator} , {@link GroupComparator} and
 * {@link TupleHashPartitioner} implementations.
 * <p>
 * 
 * A Tuple-based Map-Red job, in its simplest form, requires to define :<br>
 * <ul>
 * 
 * <li><b>Intermediate schemas:</b><br>
 * An schema specifies the name and types of a Tuple's fields. Several schemas can be defined in order to perform joins
 * among different input data. It's mandatory to specify ,at least,one schema using
 * {@link #addIntermediateSchema(Schema)}<br>
 * 
 * <li><b>Group-by fields:</b><br>
 * Needed to specify how the tuples will be grouped. Several tuples with the same group-by fields will be groupped and
 * reduced together in the Reduce phase.<br>
 * 
 * <li><b>Tuple-based Mapper:</b><br>
 * The job needs to specify a {@link TupleMapper} instance,the Tuple-based implementation of Hadoop's {@link Mapper}.
 * Unlike Hadoop's Mappers, Tuple-based mappers are configured using stateful serializable instances and not static
 * class definitions.<br>
 * 
 * <li><b>Tuple-based Reducer:</b> Similar to mapper instances,the job needs to specify a {@link TupleReducer}
 * instance,the Tuple-based implementation of Hadoop's {@link Reducer}. <br>
 * 
 * </ul>
 * 
 * @see ITuple
 * @see Schema
 * @see TupleMapper
 * @see TupleReducer
 * 
 */
@SuppressWarnings("rawtypes")
public class TupleMRBuilder extends TupleMRConfigBuilder {

	private Configuration conf;

	private TupleReducer tupleReducer;
	private TupleReducer tupleCombiner;
	private OutputFormat outputFormat;
	private Class<?> jarByClass;
	private Class<?> outputKeyClass;
	private Class<?> outputValueClass;
	private String jobName;

	private Path outputPath;

	private MultipleInputsInterface multipleInputs;
	private NamedOutputsInterface namedOutputs;
	
	private Set<String> instanceFilesCreated = new HashSet<String>();

	public TupleMRBuilder(Configuration conf) {
		this.conf = conf;
		multipleInputs = new MultipleInputsInterface(this.conf);
		namedOutputs = new NamedOutputsInterface(this.conf);
	}

	/**
	 * @param conf
	 *          Configuration instance
	 * @param name
	 *          Job's name as in {@link Job}
	 */
	public TupleMRBuilder(Configuration conf, String name) {
		this(conf);
		this.jobName = name;
	}

	public Configuration getConf() {
		return conf;
	}

	/**
	 * Sets the jar by class , as in {@link Job#setJarByClass(Class)}
	 */
	public void setJarByClass(Class<?> jarByClass) {
		this.jarByClass = jarByClass;
	}

	/**
	 * Adds an input file associated with a TupleFile.
	 */
	public void addTupleInput(Path path, TupleMapper<ITuple, NullWritable> tupleMapper) {
		addInput(path, new TupleInputFormat(), tupleMapper);
	}

	/**
	 * Adds an input file associated with a TupleFile.
	 * <p>
	 * A specific "Target Schema" is specified, which should be backwards-compatible with the Schema in the
	 * Tuple File (new nullable fields are allowed, not used old fields too).
	 */
	public void addTupleInput(Path path, Schema targetSchema, TupleMapper<ITuple, NullWritable> tupleMapper) {
		addInput(path, new TupleInputFormat(targetSchema), tupleMapper);
	}
	
	public void addNamedOutput(String namedOutput, OutputFormat outputFormat, Class keyClass,
	    Class valueClass) throws TupleMRException {
		addNamedOutput(namedOutput, outputFormat, keyClass, valueClass, null);
	}

  /**
   * Sets the default named output specs. By using this method one can use an arbitrary number of named outputs
   * without pre-defining them beforehand.
   */
  public void setDefaultNamedOutput(OutputFormat outputFormat, Class keyClass, Class valueClass) throws TupleMRException {
  	setDefaultNamedOutput(outputFormat, keyClass, valueClass, null);
  }
  
  /**
   * Sets the default named output specs. By using this method one can use an arbitrary number of named outputs
   * without pre-defining them beforehand.
   * <p>
   * The specific (key, value) default context defined here will be applied to ALL named outputs.
   */
  public void setDefaultNamedOutput(OutputFormat outputFormat, Class keyClass, Class valueClass, Map<String, String> specificContext)  throws TupleMRException {
  	namedOutputs.add(new Output(true, "DEFAULT", outputFormat, keyClass, valueClass, specificContext));
  }
  
  /**
   * Sets the default named output (Tuple format) specs. By using this method one can use an arbitrary number of named outputs
   * without pre-defining them beforehand.
   */
  public void setDefaultNamedOutput(Schema outputSchema) throws TupleMRException {
  	Output output = new Output(true, "DEFAULT", new TupleOutputFormat(outputSchema),
        ITuple.class, NullWritable.class, null);
    namedOutputs.add(output);
  }
  
	public void addNamedOutput(String namedOutput, OutputFormat outputFormat, Class keyClass,
	    Class valueClass, Map<String, String> specificContext) throws TupleMRException {
		namedOutputs.add(new Output(false, namedOutput, outputFormat, keyClass, valueClass, specificContext));
	}

	public void addNamedTupleOutput(String namedOutput, Schema outputSchema) throws TupleMRException {
		Output output = new Output(false, namedOutput, new TupleOutputFormat(outputSchema), ITuple.class,
		    NullWritable.class, null);
		namedOutputs.add(output);
	}

	/**
	 * Defines an input as in {@link PangoolMultipleInputs}
	 * 
	 * @see PangoolMultipleInputs
	 */
	public void addInput(Path path, InputFormat inputFormat, TupleMapper inputProcessor) {
		multipleInputs.getMultiInputs().add(new Input(path, inputFormat, inputProcessor, new HashMap<String, String>()));
	}

	public void addInput(Path path, InputFormat inputFormat, TupleMapper inputProcessor, Map<String, String> specificContext) {
		multipleInputs.getMultiInputs().add(new Input(path, inputFormat, inputProcessor, specificContext));
	}
	
	/**
	 * 
	 */
	public void setTupleCombiner(TupleReducer tupleCombiner) {
		this.tupleCombiner = tupleCombiner;
	}

	public void setOutput(Path outputPath, OutputFormat outputFormat, Class<?> outputKeyClass,
	    Class<?> outputValueClass) {
		this.outputFormat = outputFormat;
		this.outputKeyClass = outputKeyClass;
		this.outputValueClass = outputValueClass;
		this.outputPath = outputPath;
	}

	public void setTupleOutput(Path outputPath, Schema schema) {
		this.outputPath = outputPath;
		this.outputFormat = new TupleOutputFormat(schema);
		this.outputKeyClass = ITuple.class;
		this.outputValueClass = NullWritable.class;
	}

	public void setTupleReducer(TupleReducer tupleReducer) {
		this.tupleReducer = tupleReducer;
	}

	/**
	 * Run this method after running your Job for instance files to be properly cleaned. 
	 * @throws IOException 
	 */
	public void cleanUpInstanceFiles() throws IOException {
		for(String instanceFile: instanceFilesCreated) {
			InstancesDistributor.removeFromCache(conf, instanceFile);
		}
	}
	
	public Job createJob() throws IOException, TupleMRException {

		failIfNull(tupleReducer, "Need to set a group handler");
		failIfEmpty(multipleInputs.getMultiInputs(), "Need to add at least one input");
		failIfNull(outputFormat, "Need to set output format");
		failIfNull(outputKeyClass, "Need to set outputKeyClass");
		failIfNull(outputValueClass, "Need to set outputValueClass");
		failIfNull(outputPath, "Need to set outputPath");

		// perform a deep copy of the Configuration
		this.conf = new Configuration(this.conf);
		
		TupleMRConfig tupleMRConf = buildConf();
		// Serialize PangoolConf in Hadoop Configuration
		instanceFilesCreated.addAll(TupleMRConfig.set(tupleMRConf, conf));
		Job job = (jobName == null) ? new Job(conf) : new Job(conf, jobName);
		if(tupleMRConf.getRollupFrom() != null) {
			job.setReducerClass(RollupReducer.class);
		} else {
			job.setReducerClass(SimpleReducer.class);
		}

		if(tupleCombiner != null) {
			job.setCombinerClass(SimpleCombiner.class); // not rollup by now
			// Set Combiner Handler
			String uniqueName = UUID.randomUUID().toString() + '.' + "combiner-handler.dat";
			try {
				InstancesDistributor.distribute(tupleCombiner, uniqueName, job.getConfiguration());
				instanceFilesCreated.add(uniqueName);
				job.getConfiguration().set(SimpleCombiner.CONF_COMBINER_HANDLER, uniqueName);
			} catch(URISyntaxException e1) {
				throw new TupleMRException(e1);
			}
		}

		// Set Tuple Reducer
		try {
			String uniqueName = UUID.randomUUID().toString() + '.' + "group-handler.dat";
			InstancesDistributor.distribute(tupleReducer, uniqueName, job.getConfiguration());
			instanceFilesCreated.add(uniqueName);
			job.getConfiguration().set(SimpleReducer.CONF_REDUCER_HANDLER, uniqueName);
		} catch(URISyntaxException e1) {
			throw new TupleMRException(e1);
		}

		// Enabling serialization
		TupleSerialization.enableSerialization(job.getConfiguration());

		job.setJarByClass((jarByClass != null) ? jarByClass : tupleReducer.getClass());
		job.setMapOutputKeyClass(DatumWrapper.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setPartitionerClass(TupleHashPartitioner.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setSortComparatorClass(SortComparator.class);
		job.setOutputKeyClass(outputKeyClass);
		job.setOutputValueClass(outputValueClass);
		FileOutputFormat.setOutputPath(job, outputPath);
		instanceFilesCreated.addAll(multipleInputs.configureJob(job));
		instanceFilesCreated.addAll(namedOutputs.configureJob(job));
		// Configure a {@link ProxyOutputFormat} for Pangool's Multiple Outputs to
		// work: {@link PangoolMultipleOutput}
		String uniqueName = UUID.randomUUID().toString() + '.' + "out-format.dat";
		try {
			InstancesDistributor.distribute(outputFormat, uniqueName, conf);
			instanceFilesCreated.add(uniqueName);
		} catch(URISyntaxException e1) {
			throw new TupleMRException(e1);
		}
		job.getConfiguration().set(ProxyOutputFormat.PROXIED_OUTPUT_FORMAT_CONF, uniqueName);
		job.setOutputFormatClass(ProxyOutputFormat.class);

		return job;
	}
}