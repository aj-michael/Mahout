package edu.rosehulman.bm25;

import java.io.Serializable;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.collect.Sets;

public class BM25Classifier implements Tool, Serializable {

	private static final long serialVersionUID = -7355240271344556170L;

	public void setConf(Configuration conf) { }

	public Configuration getConf() {
		return new Configuration();
	}
	
	public void distributedScore(Set<String> words, String output) {
		
	}
	
	public int run(String[] args) throws Exception {
		String model = args[0];
		String text = args[1];
		String output = args[2];
		double k1 = Double.parseDouble(args[3]);
		double b = Double.parseDouble(args[4]);
		final Set<String> words = Sets.newHashSet(text.toLowerCase().split(" "));
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new BM25Classifier(),args);
	}

}
