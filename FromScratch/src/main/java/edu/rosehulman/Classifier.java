package edu.rosehulman;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.datasalt.pangool.io.Fields;
import com.datasalt.pangool.io.ITuple;
import com.datasalt.pangool.io.Schema;
import com.datasalt.pangool.tuplemr.MapOnlyJobBuilder;
import com.datasalt.pangool.tuplemr.TupleMRBuilder;
import com.datasalt.pangool.tuplemr.TupleMRException;
import com.datasalt.pangool.tuplemr.TupleMapper.Collector;
import com.datasalt.pangool.tuplemr.mapred.MapOnlyMapper;
import com.google.common.base.Functions;
import com.google.common.collect.Ordering;

public class Classifier implements Tool, Serializable {

	private static final long serialVersionUID = -2375691330604388212L;

	static Schema input_schema = new Schema("first schema",
			Fields.parse("word: string, year: int, count: int, dcount: int"));	
	
	public void setConf(Configuration conf) { }
	
	public Configuration getConf() {
		return null;
	}

	// year -> (word -> count)
	static Map<Integer,Map<String, Integer>> yearlyFrequency = new ConcurrentHashMap<Integer,Map<String,Integer>>();
	
	// year -> count
	static Map<Integer,Long> yearlyCount = new ConcurrentHashMap<Integer,Long>(); 
	
	// apparently they forgot to implement ConcurrentHashMap
	static Set<String> vocabulary = Collections.newSetFromMap(new ConcurrentHashMap<String,Boolean>());
	
	public void filter(String model, final Set<String> words, String filterOutput) throws ClassNotFoundException, IOException, InterruptedException, TupleMRException, URISyntaxException {
		Configuration conf = new Configuration();
		MapOnlyMapper<ITuple,NullWritable,ITuple,NullWritable> mapper = new MapOnlyMapper<ITuple,NullWritable,ITuple,NullWritable>() {
			private static final long serialVersionUID = -5104195321429602029L;
			@Override
			protected void map(ITuple key, NullWritable value, Context context) throws IOException, InterruptedException {
				String word = key.get("word").toString();
				int year = (Integer) key.get("year");
				int count = (Integer) key.get("count");
				if (words.contains(word.toLowerCase())) {
					yearlyFrequency.putIfAbsent(year, new HashMap<String,Integer>());
					yearlyFrequency.get(year).put(word, count);
					context.write(key,value);
				}
				yearlyCount.putIfAbsent(year, 0L);
				yearlyCount.put(year, yearlyCount.get(year)+count);
				vocabulary.add(word);
			}
		};
		MapOnlyJobBuilder job = new MapOnlyJobBuilder(conf,"Filter");
		job.addTupleInput(new Path(model), mapper);
		job.setTupleOutput(new Path(filterOutput), input_schema);
		job.createJob().waitForCompletion(true);
		job.cleanUpInstanceFiles();
	}

	public int run(String[] args) throws Exception {
		String model = args[0];
		String text = args[1];
		String filterOutput = args[2];
		final Set<String> words = new HashSet<String>(Arrays.asList(text.toLowerCase().split(" ")));
		this.filter(model, words, filterOutput);

		System.out.println(yearlyFrequency.size());
		System.out.println(yearlyCount.size());
		System.out.println(vocabulary.size());
		
		// year -> score
		Map<Integer,Double> yearScore = new HashMap<Integer,Double>();
		for (String word : text.toLowerCase().split(" ")) {
			for (int year : yearlyFrequency.keySet()) {
				int count = yearlyFrequency.get(year).getOrDefault(word,0)+1;
				double score = Math.log(count) - Math.log(yearlyCount.get(year) + vocabulary.size());
				yearScore.putIfAbsent(year,0.);
				yearScore.put(year, yearScore.get(year)+score);
			}
		}
	
		Map<Double,Integer> invertedScores = new TreeMap<Double,Integer>();
		yearScore.forEach((year,score) -> invertedScores.put(score,year));
		invertedScores.forEach((score,year) -> System.out.println("Year: "+year+", Score: "+score));
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Classifier(), args);
	}

}
