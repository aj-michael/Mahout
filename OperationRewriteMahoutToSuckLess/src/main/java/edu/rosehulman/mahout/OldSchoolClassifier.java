package edu.rosehulman.mahout;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.apache.mahout.classifier.ClassifierResult;
import org.apache.mahout.classifier.bayes.Algorithm;
import org.apache.mahout.classifier.bayes.BayesAlgorithm;
import org.apache.mahout.classifier.bayes.BayesParameters;
import org.apache.mahout.classifier.bayes.ClassifierContext;
import org.apache.mahout.classifier.bayes.Datastore;
import org.apache.mahout.classifier.bayes.InMemoryBayesDatastore;
import org.apache.mahout.classifier.bayes.InvalidDatastoreException;

public class OldSchoolClassifier {

	public static void main(String[] args) throws IOException, InvalidDatastoreException {
		File modelfile = new File(args[0]);
		File inputdir = new File(args[1]);
		ClassifierContext classifier = newClassifier(modelfile);
		System.out.println(classifier.getLabels());
		for (File f : inputdir.listFiles()) {
			String[] x = preprocess(f);
			ClassifierResult[] results = classifier.classifyDocument(x, "unknown", 1);
			for (ClassifierResult res : results) {
				System.out.println("Category: " + res.getLabel() + " Score: " + res.getScore());
			}
		}
	}
	
	public static String[] preprocess(File f) throws IOException {
		StandardAnalyzer sa = new StandardAnalyzer(Version.LUCENE_34);
		FileInputStream stream = new FileInputStream(f);
		InputStreamReader reader = new InputStreamReader(stream,"UTF-8");
		TokenStream tokenstream = sa.tokenStream(null,reader);
		List<String> tokens = new ArrayList<String>();
		while (tokenstream.incrementToken()) {
			String value = tokenstream.getAttribute(CharTermAttribute.class).toString();
			tokens.add(value);
		}
		String[] processed = tokens.toArray(new String[tokens.size()]);
		sa.close();
		return processed;
	}

	private static ClassifierContext newClassifier(File modelfile) throws IOException, InvalidDatastoreException {
		BayesParameters params = getParams();
		params.setBasePath(modelfile.getCanonicalPath());
		Algorithm alg = new BayesAlgorithm();
		Datastore ds = new InMemoryBayesDatastore(params);
		ClassifierContext classifier = new ClassifierContext(alg,ds);
		classifier.initialize();
		return classifier;
	}
	
	private static BayesParameters getParams() {
		BayesParameters bayesparams = new BayesParameters();
		bayesparams.setGramSize(1);
		bayesparams.set("encoding", "UTF-8");
		bayesparams.set("dataSource", "hdfs");
		bayesparams.set("alpha_i", "1.0");
		return bayesparams;
	}

}
