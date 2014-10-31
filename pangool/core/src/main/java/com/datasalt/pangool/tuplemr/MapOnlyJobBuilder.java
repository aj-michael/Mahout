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

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.tuplemr.MultipleInputsInterface.Input;
import com.datasalt.pangool.tuplemr.NamedOutputsInterface.Output;
import com.datasalt.pangool.tuplemr.mapred.MapOnlyMapper;
import com.datasalt.pangool.tuplemr.mapred.lib.input.TupleInputFormat;
import com.datasalt.pangool.tuplemr.mapred.lib.output.ProxyOutputFormat;
import com.datasalt.pangool.tuplemr.mapred.lib.output.TupleOutputFormat;
import com.datasalt.pangool.utils.InstancesDistributor;

/**
 * The MapOnlyJobBuilder is a simple Pangool primitive that executes map-only Jobs. You must implement
 * {@link MapOnlyMapper} for using it.
 */
@SuppressWarnings("rawtypes")
public class MapOnlyJobBuilder {

  private Configuration conf;

  private Class<?> jarByClass;
  private Class<?> outputKeyClass;
  private Class<?> outputValueClass;
  private OutputFormat outputFormat;

  private Path outputPath;

  private MultipleInputsInterface multipleInputs;
  private NamedOutputsInterface namedOutputs;

  private MapOnlyMapper mapOnlyMapper;
  private String jobName = null;

	private List<String> instanceFilesCreated = new ArrayList<String>();
	
  public MapOnlyJobBuilder setJarByClass(Class<?> jarByClass) {
    this.jarByClass = jarByClass;
    return this;
  }

  /**
   * Adds an input file associated with a TupleFile.
   */
	public void addTupleInput(Path path, MapOnlyMapper tupleMapper) {
		addInput(path, new TupleInputFormat(), tupleMapper);
	}

	/**
	 * Adds an input file associated with a TupleFile.
	 * <p>
	 * A specific "Target Schema" is specified, which should be backwards-compatible with the Schema in the
	 * Tuple File (new nullable fields are allowed, not used old fields too).
	 */
	public void addTupleInput(Path path, Schema targetSchema, MapOnlyMapper tupleMapper) {
		addInput(path, new TupleInputFormat(targetSchema), tupleMapper);
	}

  public MapOnlyJobBuilder addInput(Path path, InputFormat inputFormat, MapOnlyMapper processor) {
    return addInput(path, inputFormat, processor, new  HashMap<String, String>());
  }
  
  public MapOnlyJobBuilder addInput(Path path, InputFormat inputFormat, MapOnlyMapper processor, Map<String, String> specificContext) {
    multipleInputs.getMultiInputs().add(new Input(path, inputFormat, processor, specificContext));
    return this;
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
                             Class valueClass) throws TupleMRException {
    addNamedOutput(namedOutput, outputFormat, keyClass, valueClass, null);
  }

  public void addNamedOutput(String namedOutput, OutputFormat outputFormat, Class keyClass,
                             Class valueClass, Map<String, String> specificContext) throws TupleMRException {
    namedOutputs.add(new Output(false, namedOutput, outputFormat, keyClass, valueClass, specificContext));
  }

  public void addNamedTupleOutput(String namedOutput, Schema outputSchema) throws TupleMRException {
    Output output = new Output(false, namedOutput, new TupleOutputFormat(outputSchema),
        ITuple.class, NullWritable.class, null);
    namedOutputs.add(output);
  }

  public MapOnlyJobBuilder setTupleOutput(Path outputPath, Schema schema) {
    this.outputPath = outputPath;
    this.outputFormat = new TupleOutputFormat(schema);
    this.outputKeyClass = ITuple.class;
    this.outputValueClass = NullWritable.class;
    return this;
  }

  public MapOnlyJobBuilder setOutput(Path outputPath, OutputFormat outputFormat,
                                     Class<?> outputKeyClass, Class<?> outputValueClass) {
    this.outputFormat = outputFormat;
    this.outputKeyClass = outputKeyClass;
    this.outputValueClass = outputValueClass;
    this.outputPath = outputPath;
    return this;
  }

  /**
   * Deprecated. Use {@link #addInput(org.apache.hadoop.fs.Path, org.apache.hadoop.mapreduce.InputFormat, com.datasalt.pangool.tuplemr.mapred.MapOnlyMapper)}
   * instead.
   */
  @Deprecated
  public MapOnlyJobBuilder setMapper(MapOnlyMapper mapOnlyMapper) {
    this.mapOnlyMapper = mapOnlyMapper;
    return this;
  }

  public MapOnlyJobBuilder(Configuration conf) {
    this(conf, null);
  }

  public MapOnlyJobBuilder(Configuration conf, String jobName) {
    this.conf = conf;
    this.jobName = jobName;
    this.multipleInputs = new MultipleInputsInterface(conf);
    this.namedOutputs = new NamedOutputsInterface(conf);
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
	
  public Job createJob() throws IOException, TupleMRException, URISyntaxException {
  	// perform a deep copy of the configuration
  	this.conf = new Configuration(this.conf);

    String uniqueName = UUID.randomUUID().toString() + '.' + "out-format.dat";
    try {
      InstancesDistributor.distribute(outputFormat, uniqueName, conf);
      instanceFilesCreated.add(uniqueName);
    } catch (URISyntaxException e1) {
      throw new TupleMRException(e1);
    }
  	
    Job job;
    if (jobName == null) {
      job = new Job(conf);
    } else {
      job = new Job(conf, jobName);
    }
    job.setNumReduceTasks(0);
    
    job.getConfiguration().set(ProxyOutputFormat.PROXIED_OUTPUT_FORMAT_CONF, uniqueName);
    job.setOutputFormatClass(ProxyOutputFormat.class);

    if (outputKeyClass == null) {
      throw new TupleMRException("Output spec must be defined, use setOutput()");
    }
    job.setOutputKeyClass(outputKeyClass);
    job.setOutputValueClass(outputValueClass);
    FileOutputFormat.setOutputPath(job, outputPath);

    Input lastInput = null;

    for (Input input : multipleInputs.getMultiInputs()) {
      if (input.inputProcessor == null) {
        input.inputProcessor = mapOnlyMapper;
        if (input.inputProcessor == null) {
          throw new TupleMRException("Either mapOnlyMapper property or full Input spec must be set.");
        }
      }
      lastInput = input;
    }

    if (lastInput == null) {
      throw new TupleMRException("At least one input must be specified");
    }
    job.setJarByClass((jarByClass != null) ? jarByClass : lastInput.inputProcessor.getClass());

    instanceFilesCreated.addAll(multipleInputs.configureJob(job));
    instanceFilesCreated.addAll(namedOutputs.configureJob(job));

    return job;
  }
}
