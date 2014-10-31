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
import java.util.Iterator;
import java.util.List;

import com.datasalt.pangool.utils.InstancesDistributor;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.Reducer;

import com.datasalt.pangool.io.DatumWrapper;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.ViewTuple;
import com.datasalt.pangool.tuplemr.Criteria.SortElement;
import com.datasalt.pangool.tuplemr.SerializationInfo;
import com.datasalt.pangool.tuplemr.TupleMRConfig;
import com.datasalt.pangool.tuplemr.TupleMRConfigBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleReducer;
import com.datasalt.pangool.tuplemr.TupleReducer.TupleMRContext;
import com.datasalt.pangool.tuplemr.TupleRollupReducer;

/**
 * 
 * This {@link Reducer} implements a similar functionality than
 * {@link SimpleReducer} but adding a Rollup feature.
 */
public class RollupReducer<OUTPUT_KEY, OUTPUT_VALUE> extends
    Reducer<DatumWrapper<ITuple>, NullWritable, OUTPUT_KEY, OUTPUT_VALUE> {

	private boolean firstRun = true;
	private TupleMRConfig tupleMRConfig;
	private SerializationInfo serInfo;
	private TupleMRContext context;
	private TupleReducer<OUTPUT_KEY, OUTPUT_VALUE>.Collector collector;
	private int minDepth, maxDepth;
	private ViewTuple groupTuple;
	private TupleIterator<OUTPUT_KEY, OUTPUT_VALUE> tupleIterator;
	private TupleRollupReducer<OUTPUT_KEY, OUTPUT_VALUE> handler;
	private boolean isMultipleSources;
	private Schema groupSchema;
	private RawComparator<?>[] customComparators;

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		try {
			this.tupleMRConfig = TupleMRConfig.get(context.getConfiguration());
			this.isMultipleSources = this.tupleMRConfig.getNumIntermediateSchemas() >= 2;
			this.serInfo = tupleMRConfig.getSerializationInfo();
			this.groupSchema = this.serInfo.getGroupSchema();
			if(!isMultipleSources) {
				/* schema id 0 is the only intermediate schema defined */
				this.groupTuple = new ViewTuple(groupSchema,
				    this.serInfo.getGroupSchemaIndexTranslation(0)); 
			} else {
				this.groupTuple = new ViewTuple(groupSchema);
			}
			List<String> groupFields = tupleMRConfig.getGroupByFields();
			this.maxDepth = groupFields.size() - 1;
			this.minDepth = tupleMRConfig.calculateRollupBaseFields().size() - 1;
			this.tupleIterator = new TupleIterator<OUTPUT_KEY, OUTPUT_VALUE>(context);
			initHandlerContextAndCollector(context);
			initComparators();
		} catch(TupleMRException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Initialize the custom comparators. Creates a quick access array for the
	 * custom comparators.
	 */
	private void initComparators() {
		TupleMRConfigBuilder.initializeComparators(context.getHadoopContext()
		    .getConfiguration(), tupleMRConfig);
		customComparators = new RawComparator<?>[maxDepth + 1];
		for(int i = minDepth; i <= maxDepth; i++) {
			SortElement element = tupleMRConfig.getCommonCriteria().getElements().get(i);
			if(element.getCustomComparator() != null) {
				customComparators[i] = element.getCustomComparator();
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void initHandlerContextAndCollector(Context context) throws IOException,
	    InterruptedException, TupleMRException {
		String fileName = context.getConfiguration().get(SimpleReducer.CONF_REDUCER_HANDLER);
		handler = InstancesDistributor.loadInstance(context.getConfiguration(),
        TupleRollupReducer.class, fileName, true);
		collector = handler.new Collector(
		    (ReduceContext<DatumWrapper<ITuple>, NullWritable, Object, Object>) context);
		this.context = new TupleMRContext(
		    (ReduceContext<DatumWrapper<ITuple>, NullWritable, Object, Object>) context,
		    tupleMRConfig);
		handler.setup(this.context, collector);
	}

	public void cleanup(Context context) throws IOException, InterruptedException {
		try {
			handler.cleanup(this.context, collector);
			collector.close();
			super.cleanup(context);
		} catch(TupleMRException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public final void run(Context context) throws IOException, InterruptedException {
		try {
			setup(context);
			firstRun = true;
			boolean anyValue = false;
			while(context.nextKey()) {
				anyValue = true;
				reduce(context.getCurrentKey(), context.getValues(), context);
			}

			// close last group. Only if there was any value.
			if (anyValue) {
				for(int i = maxDepth; i >= minDepth; i--) {
					handler.onCloseGroup(i, groupSchema.getField(i).getName(), context
					    .getCurrentKey().datum(), this.context, collector);
				}
			}
			cleanup(context);
		} catch(TupleMRException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public final void reduce(DatumWrapper<ITuple> key, Iterable<NullWritable> values,
	    Context context) throws IOException, InterruptedException {

		try {
			Iterator<NullWritable> iterator = values.iterator();
			tupleIterator.setIterator(iterator);
			ITuple currentTuple = key.datum();
			ITuple previousKey = key.previousDatum();
			int indexMismatch;
			if(firstRun) {
				indexMismatch = minDepth;
				firstRun = false;
			} else {
				indexMismatch = indexMismatch(previousKey, currentTuple, 0, maxDepth);
				if(indexMismatch < minDepth) {
					indexMismatch = minDepth;
				}
				for(int i = maxDepth; i >= indexMismatch; i--) {
					handler.onCloseGroup(i, groupSchema.getField(i).getName(), previousKey,
					    this.context, collector);
				}
			}

			for(int i = indexMismatch; i <= maxDepth; i++) {
				handler.onOpenGroup(i, groupSchema.getField(i).getName(), currentTuple,
				    this.context, collector);
			}

			// We set a view over the group fields to the method.
			if(isMultipleSources) {
				int schemaId = tupleMRConfig
				    .getSchemaIdByName(currentTuple.getSchema().getName());
				int[] indexTranslation = serInfo.getGroupSchemaIndexTranslation(schemaId);
				groupTuple.setContained(currentTuple, indexTranslation);
			} else {
				groupTuple.setContained(currentTuple);
			}

			handler.reduce(groupTuple, tupleIterator, this.context, collector);

			// This loop consumes the remaining elements that reduce didn't consume
			// The goal of this is to correctly set the last element in the next
			// onCloseGroup() call
			while(iterator.hasNext()) {
				iterator.next();
			}
		} catch(TupleMRException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Compares sequentially the fields from two tuples and returns which field
	 * they differ from. Use custom comparators when provided. The provided
	 * RawComparators must implement "compare" so we should use them.<p>
	 * 
	 * Important. The contract of this method is that the tuples will differ
	 * always between minField and maxField. If they are equal then an Exception
	 * is thrown.</p>
	 * 
	 * 
	 */
	private int indexMismatch(ITuple tuple1, ITuple tuple2, int minFieldIndex,
	    int maxFieldIndex) {
		int schemaId1 = tupleMRConfig.getSchemaIdByName(tuple1.getSchema().getName());
		int schemaId2 = tupleMRConfig.getSchemaIdByName(tuple2.getSchema().getName());
		int[] translationTuple1 = serInfo.getGroupSchemaIndexTranslation(schemaId1);
		int[] translationTuple2 = serInfo.getGroupSchemaIndexTranslation(schemaId2);

		for(int i = minFieldIndex; i <= maxFieldIndex; i++) {
			Object obj1 = tuple1.get(translationTuple1[i]);
			Object obj2 = tuple2.get(translationTuple2[i]);
			@SuppressWarnings("unchecked")
			RawComparator<Object> customComparator = (RawComparator<Object>) customComparators[i];

			if(customComparator != null) {
				//TODO we assume here that customComparator must implement compare by objects.
				//Even if it's annoying, we should serialize and compare in binary.
				if(customComparator.compare(obj1, obj2) != 0) {
					return i;
				}
			} else {
				if (SortComparator.compareObjects(obj1,obj2) != 0){
					return i;
				}
			}
		}
		throw new RuntimeException("Illegal state.The tuples " + tuple1 + " and " + tuple2
		    + " compare the same between indexes " + minFieldIndex + " and " + maxFieldIndex);
	}
}
