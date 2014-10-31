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
package com.datasalt.pangool.tuplemr.mapred;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.mapreduce.Mapper;

import com.datasalt.pangool.tuplemr.MapOnlyJobBuilder;
import com.datasalt.pangool.tuplemr.MultipleOutputsCollector;

/**
 * Mapper to be implemented by Map-only jobs.
 * 
 * @see MapOnlyJobBuilder
 */
@SuppressWarnings("serial")
public abstract class MapOnlyMapper<I1, I2, O1, O2> extends Mapper<I1, I2, O1, O2> implements
    Serializable {

	private MultipleOutputsCollector collector;

	protected void map(I1 key, I2 value, Mapper<I1, I2, O1, O2>.Context context,
	    MultipleOutputsCollector collector) throws IOException, InterruptedException {

	}

	protected void map(I1 key, I2 value, Mapper<I1, I2, O1, O2>.Context context) throws IOException,
	    InterruptedException {
		map(key, value, context, collector);
	}

	/**
	 * Override this method, not the other one. This one will be automatically called by the default setup() after
	 * initializing MultipleOutputs.
	 */
	protected void setup(Mapper<I1, I2, O1, O2>.Context context, MultipleOutputsCollector mOuts)
	    throws IOException, InterruptedException {

	}

	/**
	 * @deprecated This method shouldn't be implemented, otherwise MultipleOutputs may not work. When implemented,
	 *             super.setup() should be called if MultipleOutputs are to be used. Therefore we have created another
	 *             signature with the MultipleOutputsCollector in order to avoid this, which will be the official setup()
	 *             method in the future (this one will be declared final in further versions).
	 */
	protected void setup(Mapper<I1, I2, O1, O2>.Context context) throws IOException, InterruptedException {
		collector = new MultipleOutputsCollector(context);
		this.setup(context, collector);
	}

	/**
	 * Override this method, not the other declared as final.
	 */
	protected void cleanup(Mapper<I1, I2, O1, O2>.Context context, MultipleOutputsCollector mOuts)
	    throws IOException, InterruptedException {

	}

	protected final void cleanup(Mapper<I1, I2, O1, O2>.Context context) throws java.io.IOException,
	    InterruptedException {
		this.cleanup(context, collector);
		collector.close();
	}
}
