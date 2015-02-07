package edu.rosehulman.dan;

import java.io.DataInputStream;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.rosehulman.bm25.BM25Classifier;
import edu.rosehulman.naivebayes.NaiveBayesClassifier;

public class Analyzer {
	
	public static final Path KNN_MODEL = new Path("ngrams/BM25model/filtered");
	public static final Path KNN_WORK = new Path("knnworkspace");
	public static final Path BAYES_MODEL = new Path("ngrams/NBModel");
	public static final Path BM25_MODEL = new Path("ngrams/BM25model");
	public static final Path RESULTS = new Path("analysis/results");

	public static void bayesClassify(Path model, Path output, String text) throws Exception{
		String[] args = {model.toString(),text,output.toString()};
		NaiveBayesClassifier.main(args);
	}

	public static void bm25Classify(Path model, Path output, String text) throws Exception {
		String[] args = {model.toString(),text,output.toString(),"1.5","0.75"};
		BM25Classifier.main(args);
	}

	// usage: [dir of text files]
	public static void main(String[] argv) throws Exception{
		Configuration conf = new Configuration();
		FileSystem f = FileSystem.get(conf);
		Path dir = new Path(argv[0]);
		for(FileStatus s : f.listStatus(dir)){
			DataInputStream in = f.open(s.getPath());
			byte[] text = new byte[in.available()];
			in.readFully(text);
			String str = new String(text);
			String name = s.getPath().getName().replace(".txt","");
			Path currentResults = new Path(RESULTS,name);
			Path knnResults = new Path(currentResults,"knn");
			Path bm25Results = new Path(currentResults,"bm25");
			Path bayesResults = new Path(currentResults,"bayes");
			try {
				bm25Classify(BM25_MODEL, bm25Results, str);
			} catch(Exception e){
				e.printStackTrace();
			}
			try {
				bayesClassify(BAYES_MODEL,bayesResults,str);
			} catch(Exception e){
				e.printStackTrace();
			}
			try {
				KNNClassifier.classify(KNN_MODEL, KNN_WORK, knnResults, str, conf);
			} catch(Exception e){
				e.printStackTrace();
			}
		}
	}
}
