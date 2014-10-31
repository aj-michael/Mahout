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
package com.datasalt.pangool.tuplemr.mapred.lib.output;

import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.datasalt.pangool.utils.InstancesDistributor;
import com.datasalt.pangool.utils.JobContextFactory;
import com.datasalt.pangool.utils.TaskAttemptContextFactory;

/**
 * This special implementation of {@link FileOutputFormat} is used as a proxy for being able to support any type of
 * OutputFormat at the same time that we support Multiple Output Formats (also with any type of OutputFormat).
 * <p>
 * The idea is to use the "_temporary" folder for storing everything (including multiple sub/folders) so that we have
 * the full control over the commit / fail process.
 * <p>
 * The wrapped (proxied) output format can be of any type. It is configured through {@link #PROXIED_OUTPUT_FORMAT_CONF}.
 * 
 */
@SuppressWarnings("rawtypes")
public class ProxyOutputFormat extends FileOutputFormat implements Configurable {

	public final static String PROXIED_OUTPUT_FORMAT_CONF = ProxyOutputFormat.class.getName()
	    + ".proxied.output.format";

	protected Configuration conf;
	protected OutputFormat outputFormat;

	// The original mapred.output.dir
	protected String originalDir = null;
	// The _temporary folder over the mapred.output.dir that will be seen by the
	// proxied output format as the original
	protected String baseDir = null;

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public RecordWriter getRecordWriter(TaskAttemptContext context) throws IOException,
	    InterruptedException {
		createOutputFormatIfNeeded(context);
		return outputFormat.getRecordWriter(context);
	}

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException {
		createOutputFormatIfNeeded(context);
		try {
			outputFormat.checkOutputSpecs(context);
		} catch(InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	private void createOutputFormatIfNeeded(JobContext context) throws IOException {
		if(outputFormat == null) {
			outputFormat = InstancesDistributor.loadInstance(context.getConfiguration(), OutputFormat.class,
			    context.getConfiguration().get(PROXIED_OUTPUT_FORMAT_CONF, null), true);
		}
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
		createOutputFormatIfNeeded(context);

		String outDir = context.getConfiguration().get("mapred.output.dir");
		originalDir = outDir;
		FileOutputCommitter committer = (FileOutputCommitter) super.getOutputCommitter(context);
		baseDir = committer.getWorkPath() + "";

		Configuration conf = new Configuration(context.getConfiguration());
		TaskAttemptContext reContext;
		try {
			reContext = TaskAttemptContextFactory.get(conf, context.getTaskAttemptID());
		} catch(Exception e) {
			throw new IOException(e);
		}

		reContext.getConfiguration().set("mapred.output.dir", baseDir);
		// This is for Hadoop 2.0 :
		reContext.getConfiguration().set("mapreduce.output.fileoutputformat.outputdir", baseDir);

		try {
			return new ProxyOutputCommitter(new Path(originalDir), context,
			    outputFormat.getOutputCommitter(reContext));
		} catch(InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	public class ProxyOutputCommitter extends FileOutputCommitter {

		OutputCommitter committer;

		public ProxyOutputCommitter(Path outputPath, TaskAttemptContext context, OutputCommitter committer)
		    throws IOException {
			this(outputPath, context);
			this.committer = committer;
		}

		public ProxyOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
			super(outputPath, context);
		}

		public String getBaseDir() {
			return baseDir;
		}

		@Override
		public void setupJob(JobContext jobContext) throws IOException {
			committer.setupJob(jobContext);
			super.setupJob(jobContext);
		}

		@Override
		public void setupTask(TaskAttemptContext taskContext) throws IOException {
			committer.setupTask(taskContext);
			super.setupTask(taskContext);
		}

		@Override
		public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
			return true;
		}

		final int maxRetries = 5;

		@Override
		public void commitTask(TaskAttemptContext taskContext) throws IOException {
			committer.commitTask(taskContext);
			try {
				// This is a trick for Hadoop 2.0 where there is extra business logic in commitJob()
				JobContext jContext = JobContextFactory.get(taskContext.getConfiguration(), new JobID());
				Class cl = Class.forName(OutputCommitter.class.getName());
				@SuppressWarnings("unchecked")
				Method method = cl.getMethod("commitJob", Class.forName(JobContext.class.getName()));
				if(method != null) {
					method.invoke(committer, jContext);
				}
			} catch(Exception e) {
				// Hadoop 2.0 : do nothing
				// we need to call commitJob as a trick, but the trick itself may throw an IOException.
				// it doesn't mean that something went wrong.
				// If there was something really wrong it would have failed before.
			}

			/**
			 * This code is made to overcome race conditions in CDH4 where two tasks try to commit at the same time
			 * and IOExceptions happen. This is due to some particular implementation of FileOutputFormat and this issue
			 * is not a general issue in Hadoop 1 or Hadoop 2. Therefore we implemented this small trick which just retries
			 * until the task can be committed.
			 */
			int retries = 0;
			boolean succeed = false;
			IOException lastException = null;

			do {
				try {
					super.commitTask(taskContext);
					succeed = true;
				} catch(IOException e) {
					e.printStackTrace();
					lastException = e;
					retries++;
					try {
						// Two tasks collide: wait a bit before retrying...
	          Thread.sleep((long)(Math.random()*2000));
          } catch(InterruptedException e1) {
          	//
          }
				}
			} while(!succeed && retries < maxRetries);
			
			if(!succeed) {
				throw lastException;
			}
		}

		@Override
		public void abortTask(TaskAttemptContext taskContext) {
			try {
				committer.abortTask(taskContext);
			} catch(IOException e) {
				throw new RuntimeException(e);
			}
			try {
				super.abortTask(taskContext);
			} catch(Throwable e) {
				// catching Throwable here to maintain compatibility with Hadoop 1.0 where there is no Exception thrown
				throw new RuntimeException(e);
			}
		}
	}
}
