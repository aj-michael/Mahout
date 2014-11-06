package edu.rosehulman.dan;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;

import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.io.Tuple;
import com.datasalt.pangool.io.TupleFile;
import com.datasalt.pangool.io.TupleFile.Reader;
import com.datasalt.pangool.tuplemr.Criteria.Order;
import com.datasalt.pangool.tuplemr.OrderBy;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleMapper;
import com.datasalt.pangool.tuplemr.TupleReducer;
import com.datasalt.pangool.tuplemr.mapred.lib.input.TupleFileRecordReader;
import com.datasalt.pangool.tuplemr.mapred.lib.input.TupleInputFormat;

public class KNNClassifier extends AbstractClassifier{

	private static final long serialVersionUID = 1L;

	public static final Schema TERM_MAGNITUDE_SCHEMA = new Schema("normalized count schema",
			Fields.parse("year:int,word:string,magnitude:double"));

	public static final Schema MAGNITUDE_SQUARED_SCHEMA = new Schema("magnitude squared schema",
			Fields.parse("year:int,magSquared:long"));

	Map<String,Integer> textVector = new HashMap<String,Integer>();

	public static void calculateMagnitudesSquared(Path model, Path magnitudes, Configuration conf) throws IOException, TupleMRException{
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
				String word = tuple.getString("word");
				long count = tuple.getInteger("count");	// TODO use longs instead
				int year = tuple.getInteger("count");
				val.set("word",word);
				val.set("year",year);
				val.set("magSquared",count << 1);
			}
			
		});
		
		calcMags.setTupleReducer(new TupleReducer<ITuple,NullWritable>(){

			private static final long serialVersionUID = 1L;

			@Override
			public void reduce(ITuple key,Iterable<ITuple> tuples, TupleMRContext con, Collector col) throws IOException, InterruptedException{
				long totalMagSquared = 0;
				for(ITuple tuple : tuples){
					totalMagSquared += tuple.getLong("magSquared");
				}
				key.set("magSquared",totalMagSquared);
				col.write(key,NullWritable.get());
			}
			
		});
		calcMags.setGroupByFields("year");
	}

	public static void normalizeModel(Path model, Path magsSquared, Path normalizedModel, Configuration conf) throws IOException, TupleMRException, ClassNotFoundException, InterruptedException{
		FileSystem f = FileSystem.get(conf);
		if(f.exists(normalizedModel)){
			return;
		}
		TupleFile.Reader magReader = new TupleFile.Reader(f, conf, magsSquared);
		final Map<Integer,Double> mags = new HashMap<Integer,Double>();
		Tuple count = new Tuple(MAGNITUDE_SQUARED_SCHEMA);
		while(magReader.next(count)){
			mags.put(count.getInteger("year"),Math.sqrt(count.getLong("magSquared")));
		}
		magReader.close();
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
				col.write(val);
			}
		});

		normalizeJob.addIntermediateSchema(TERM_MAGNITUDE_SCHEMA);
		normalizeJob.setTupleOutput(normalizedModel,TERM_MAGNITUDE_SCHEMA);
		normalizeJob.createJob().waitForCompletion(true);
		normalizeJob.cleanUpInstanceFiles();
	}

	public static Map<String,Double> normalizeTextVector(Map<String,Integer> textVector){
		Map<String,Double> normalizedMap = new HashMap<String,Double>(textVector.size());
		int lengthSquared = 0;
		for(Integer count : textVector.values()){
			lengthSquared = count << 1;
		}
		double length = Math.sqrt(lengthSquared);
		for(Map.Entry<String,Integer> ent : textVector.entrySet()){
			normalizedMap.put(ent.getKey(),ent.getValue()/length);
		}
		return normalizedMap;
	}

	public static void calculateDistances(Path normalizedModel,Path result,Map<String,Integer> textVector,Configuration conf) throws TupleMRException, ClassNotFoundException, IOException, InterruptedException, URISyntaxException{
		FileSystem f = FileSystem.get(conf);
		final Map<String,Double> normalizedTextVector = normalizeTextVector(textVector);
		TupleMRBuilder builder = new TupleMRBuilder(conf);
		builder.addTupleInput(normalizedModel,TERM_MAGNITUDE_SCHEMA,new TupleMapper<ITuple,NullWritable>(){

			private static final long serialVersionUID = 1L;

			Tuple val = new Tuple(TERM_MAGNITUDE_SCHEMA);

			@Override
			public void map(ITuple tuple, NullWritable n, TupleMRContext con, Collector col) throws IOException, InterruptedException {
				String word = tuple.getString("word");
				int year = tuple.getInteger("year");
				double distCompSquared = Math.pow(tuple.getDouble("magnitude") - normalizedTextVector.get(word),2);
				val.set("word",word);
				val.set("year",year);
				val.set("magnitude",distCompSquared);
				col.write(val);
			}
		});
		builder.setTupleReducer(new TupleReducer<ITuple,NullWritable>(){

			private static final long serialVersionUID = 1L;

			Tuple val = new Tuple(MAGNITUDE_SQUARED_SCHEMA);

			@Override
			public void reduce(ITuple key, Iterable<ITuple> tuples, TupleMRContext con, Collector col) throws IOException, InterruptedException{
				double distSquared = 0;
				for(ITuple tup : tuples){
					distSquared += tup.getDouble("magnitude");
				}
				val.set("magnitude",distSquared);
				val.set("year",key.get("year"));
				col.write(key,NullWritable.get());
			}
		});
		builder.setGroupByFields("year");
		builder.addIntermediateSchema(MAGNITUDE_SQUARED_SCHEMA);
		builder.setOrderBy(new OrderBy().add("magnitude",Order.ASC));
	}

	@Override
	public int run(String[] args) throws Exception {
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
		FileSystem f = FileSystem.get(conf);
		TupleFile.Reader resultReader = new TupleFile.Reader(f, conf,results);
		Tuple tuple = new Tuple(MAGNITUDE_SQUARED_SCHEMA);
		List<Integer> answers = new ArrayList<Integer>(3); 
		for(int i = 0; i < 3 && resultReader.next(tuple); i++){
			answers.add(tuple.getInteger("year"));
		}
		resultReader.close();
		System.out.println(answers.toArray());
		return 0;
	}

	
}
