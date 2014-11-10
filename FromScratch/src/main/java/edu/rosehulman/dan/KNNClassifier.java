package edu.rosehulman.dan;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;

import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.io.TupleFile;
import com.datasalt.pangool.tuplemr.IdentityTupleReducer;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.TupleReducer;
import com.datasalt.pangool.tuplemr.mapred.lib.output.TupleTextOutputFormat;

public class KNNClassifier extends AbstractClassifier{

	private static final long serialVersionUID = 1L;

	public static final Schema TERM_MAGNITUDE_SCHEMA = new Schema("term magnitude schema",
			Fields.parse("year:int,word:string,magnitude:double"));

	public static final Schema MAGNITUDE_SQUARED_SCHEMA = new Schema("magnitude squared schema",
			Fields.parse("year:int,magSquared:long"));

	public static final Schema DISTANCE_COMPONENT_SCHEMA = new Schema("distance component schema",
			Fields.parse("year:int,magnitude:double"));

	public static final Schema FOUND_WORD_SCHEMA = new Schema("found word schema",
			Fields.parse("year:int,word:string"));

	Map<String,Integer> textVector = new HashMap<String,Integer>();

	protected static final Comparator<? super ITuple> DISTANCE_COMPONENT_COMPARATOR = new Comparator<ITuple>(){
		@Override
		public int compare(ITuple o1, ITuple o2) {
			return o1.getDouble("magnitude").compareTo(o2.getDouble("magnitude"));
		}		
	};

	public static void calculateMagnitudesSquared(Path model, Path magnitudes, Configuration conf) throws IOException, TupleMRException, ClassNotFoundException, InterruptedException{
		FileSystem f = FileSystem.get(conf);
		if(f.exists(magnitudes)){
			return;
		}
		TupleMRBuilder calcMags = new TupleMRBuilder(conf);
		calcMags.addTupleInput(model,input_schema,new TupleMapper<ITuple,NullWritable>(){

			private static final long serialVersionUID = 1L;

			Tuple val = new Tuple(MAGNITUDE_SQUARED_SCHEMA);

			@Override
			public void map(ITuple tuple, NullWritable arg1, TupleMRContext con, Collector col) throws IOException, InterruptedException {
				long count = tuple.getInteger("count");	// TODO use longs instead
				val.set("year",tuple.get("year"));
				val.set("magSquared",throwIfNegative(count*count,"long oveflow"));
				//System.out.println("Calculate Magnitude Mapper: "+val);
				col.write(val);
			}
		});
		calcMags.addIntermediateSchema(MAGNITUDE_SQUARED_SCHEMA);
		calcMags.setTupleOutput(magnitudes,MAGNITUDE_SQUARED_SCHEMA);
		calcMags.setGroupByFields("year");
		TupleReducer<ITuple,NullWritable> addMags = new TupleReducer<ITuple,NullWritable>(){

			private static final long serialVersionUID = 1L;

			private Tuple val = new Tuple(MAGNITUDE_SQUARED_SCHEMA);

			@Override
			public void reduce(ITuple key,Iterable<ITuple> tuples, TupleMRContext con, Collector col) throws IOException, InterruptedException{
				long totalMagSquared = 0;
				for(ITuple tuple : tuples){
					throwIfNegative(totalMagSquared += tuple.getLong("magSquared"),"long overflow");
				}
				val.set("magSquared",totalMagSquared);
				val.set("year",key.getInteger("year"));
				//System.out.println("Calculate Magnitude Squared Reducer: "+val.toString());
				col.write(val,NullWritable.get());
			}
		};
		calcMags.setTupleReducer(addMags);
		calcMags.setTupleCombiner(addMags);

		boolean worked = calcMags.createJob().waitForCompletion(true);
		calcMags.cleanUpInstanceFiles();
		if(!worked){
			throw new TupleMRException("Job Failed");
		}
	}

	public static void normalizeModel(Path model, Path magsSquared, Path normalizedModel, Configuration conf) throws IOException, TupleMRException, ClassNotFoundException, InterruptedException{
		FileSystem f = FileSystem.get(conf);
		if(f.exists(normalizedModel)){
			return;
		}
		final Map<Integer,Double> mags = new HashMap<Integer,Double>();
		for(FileStatus stat : f.listStatus(magsSquared)){
			if(stat.isDirectory() || stat.getLen() == 0){
				continue;
			}
			Path file = stat.getPath();
			TupleFile.Reader magReader = new TupleFile.Reader(f, conf, file);
			Tuple count = new Tuple(MAGNITUDE_SQUARED_SCHEMA);
			while(magReader.next(count)){
				mags.put(count.getInteger("year"),Math.sqrt(count.getLong("magSquared")));
			}
			magReader.close();
		}
		TupleMRBuilder normalizeJob = new TupleMRBuilder(conf);
		normalizeJob.addTupleInput(model, new TupleMapper<ITuple, NullWritable>(){

			private static final long serialVersionUID = 1L;

			private Tuple val = new Tuple(TERM_MAGNITUDE_SCHEMA);

			@Override
			public void map(ITuple tup,NullWritable arg,TupleMRContext con,Collector col) throws IOException, InterruptedException {
				int year = tup.getInteger("year");
				int count = tup.getInteger("count");
				String word = tup.getString("word");
				val.set("year",year);
				val.set("word", word);
				val.set("magnitude", ((double)count) / mags.get(year));
				//System.out.println("normalize model mapper: "+val.toString());
				col.write(val);
			}
		});

		normalizeJob.addIntermediateSchema(TERM_MAGNITUDE_SCHEMA);
		normalizeJob.setGroupByFields("year");
		normalizeJob.setTupleOutput(normalizedModel,TERM_MAGNITUDE_SCHEMA);
		normalizeJob.setTupleReducer(new IdentityTupleReducer());
		boolean worked = normalizeJob.createJob().waitForCompletion(true);
		normalizeJob.cleanUpInstanceFiles();
		if(!worked){
			throw new TupleMRException("Job Failure");
		}
	}

	public static Map<String,Double> normalizeTextVector(Map<String,Integer> textVector){
		Map<String,Double> normalizedMap = new HashMap<String,Double>(textVector.size());
		long lengthSquared = 0;
		for(Integer count : textVector.values()){
			throwIfNegative(lengthSquared += count * count,"long overflow");
		}
		double length = Math.sqrt(lengthSquared);
		for(Map.Entry<String,Integer> ent : textVector.entrySet()){
			normalizedMap.put(ent.getKey(),ent.getValue()/length);
		}
		return normalizedMap;
	}

	public static void calculateDistances(Path normalizedModel,Path result,Map<String,Integer> textVector,Configuration conf) throws TupleMRException, ClassNotFoundException, IOException, InterruptedException, URISyntaxException{
		final Map<String,Double> normalizedTextVector = normalizeTextVector(textVector);
		/*for(Map.Entry<String, Double> ent : normalizedTextVector.entrySet()){
			System.out.println(ent.getKey() + ": "+ent.getValue());
		}*/
		TupleMRBuilder builder = new TupleMRBuilder(conf);
		builder.addTupleInput(normalizedModel,TERM_MAGNITUDE_SCHEMA,new TupleMapper<ITuple,NullWritable>(){

			private static final long serialVersionUID = 1L;

			Tuple val = new Tuple(DISTANCE_COMPONENT_SCHEMA);
			Tuple foundWord = new Tuple(FOUND_WORD_SCHEMA);

			@Override
			public void map(ITuple tuple, NullWritable n, TupleMRContext con, Collector col) throws IOException, InterruptedException {
				String word = tuple.getString("word");
				int year = tuple.getInteger("year");
				double distCompSquared;
				if(normalizedTextVector.containsKey(word)){
					distCompSquared = Math.pow(tuple.getDouble("magnitude") - normalizedTextVector.get(word),2);
					foundWord.set("year",year);
					foundWord.set("word",word);
					//System.out.println("Calculate Distance Mapper: "+foundWord);
					col.write(foundWord);
				} else {
					distCompSquared = Math.pow(tuple.getDouble("magnitude"),2);
				}
				val.set("year",year);
				val.set("magnitude",distCompSquared);
				//System.out.println("Calculate Distance Mapper: "+val);
				col.write(val);
			}
		});

		TupleReducer<ITuple,NullWritable> distanceCombiner = new TupleReducer<ITuple,NullWritable>(){
	
			private static final long serialVersionUID = 1L;

			Tuple val = new Tuple(DISTANCE_COMPONENT_SCHEMA);

			@Override
			public void reduce(ITuple key, Iterable<ITuple> tuples, TupleMRContext con, Collector col) throws IOException, InterruptedException{
				double distSquared = 0;
				int year = key.getInteger("year");
				for(ITuple tup : tuples){
					if(tup.getSchema().equals(DISTANCE_COMPONENT_SCHEMA)){
						distSquared += tup.getDouble("magnitude");
					} else {
						col.write(tup,NullWritable.get());
					}
				}
				val.set("magnitude",distSquared);
				val.set("year",year);
				//System.out.println("Calculate Distance Combiner: "+val);
				col.write(val,NullWritable.get());
			}
		
		};

		TupleReducer<ITuple,NullWritable> distanceReducer = new TupleReducer<ITuple,NullWritable>(){

			private static final long serialVersionUID = 1L;


			ArrayList<ITuple> results = new ArrayList<ITuple>();

			@Override
			public void reduce(ITuple key, Iterable<ITuple> tuples, TupleMRContext con, Collector col){
				Map<String,Double> localNormalizedTextVector = new HashMap<String,Double>(normalizedTextVector);
				double distSquared = 0;
				int year = key.getInteger("year");
				for(ITuple tup : tuples){
					if(tup.getSchema().equals(DISTANCE_COMPONENT_SCHEMA)){
						distSquared += tup.getDouble("magnitude");
					} else {
						localNormalizedTextVector.remove(tup.getString("word")); // normalizedTextVector is copied in mapreduce jobs
					}
				}
				for(Map.Entry<String, Double> ent : localNormalizedTextVector.entrySet()){
					distSquared += Math.pow(ent.getValue(),2);
				}
				Tuple val = new Tuple(DISTANCE_COMPONENT_SCHEMA);
				val.set("magnitude",distSquared);
				val.set("year",year);
				//System.out.println(val);
				results.add(val);
			}

			@Override
			public void cleanup(TupleMRContext con, Collector col) throws IOException, InterruptedException{
				ITuple[] arr = results.toArray(new ITuple[results.size()]); // list.sort does not work!! Neither does priority queues
				Arrays.sort(arr,DISTANCE_COMPONENT_COMPARATOR);
				for(ITuple tup : arr){
					//System.out.println("Calculate Distance Reducer: "+tup);
					col.write(tup,NullWritable.get());
				}
			}
		};

		builder.setTupleReducer(distanceReducer);
		builder.setTupleCombiner(distanceCombiner);
		builder.setOutput(result,new TupleTextOutputFormat(DISTANCE_COMPONENT_SCHEMA,false,'\t','"','\\'),ITuple.class,NullWritable.class);
		builder.addIntermediateSchema(DISTANCE_COMPONENT_SCHEMA);
		builder.addIntermediateSchema(FOUND_WORD_SCHEMA);
		builder.setGroupByFields("year");
		boolean worked = builder.createJob().waitForCompletion(true);
		builder.cleanUpInstanceFiles();
		if(!worked){
			throw new TupleMRException("Job Failed");
		}
		
	}

	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 4){
			System.out.println("usage: [text] [model] [workspace] [results]");
			return 0;
		}
		String text = args[0];
		Path model = new Path(args[1]);
		Path work = new Path(args[2]);
		Path results = new Path(args[3]);
		Path magnitudesSquared = new Path(work,"magnitudes-squared");
		Path normalizedModel = new Path(work,"normalized-model");
		calculateMagnitudesSquared(model,magnitudesSquared,conf);
		normalizeModel(model,magnitudesSquared,normalizedModel,conf);
		Map<String,Integer> textVector = new HashMap<String,Integer>();
		for(String token : text.split("\\s+")){
			String word = token.replaceAll("\\p{P}","");
			if(textVector.containsKey(word)){
				textVector.put(word, 1+textVector.get(word));
			} else {
				textVector.put(word, 1);
			}
		}
		calculateDistances(normalizedModel,results,textVector,conf);
		return 0;
	}

	public static void main(String[] args) throws Exception{
		new KNNClassifier().run(args);
	}
}
