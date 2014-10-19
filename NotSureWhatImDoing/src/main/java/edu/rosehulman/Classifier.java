package edu.rosehulman;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.classifier.naivebayes.StandardNaiveBayesClassifier;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.StringTuple;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.vectorizer.DictionaryVectorizer;
import org.apache.mahout.vectorizer.DocumentProcessor;
import org.apache.mahout.vectorizer.collocations.llr.CollocDriver;
import org.apache.mahout.vectorizer.common.PartialVectorMerger;
import org.apache.mahout.vectorizer.term.TFPartialVectorReducer;

import com.google.common.collect.Lists;

@SuppressWarnings("deprecation")
public class Classifier {

	private final static int NGRAM_SIZE = 1;
	private final static int NORM_POWER = 1;
	private final static int MIN_SUPPORT = 1;
	private final static int MIN_LLR_VALUE = 1;
	private final static boolean LOG_NORMALIZE = false;

	// classifier.jar document-dir work-dir model-dir dictionary-dir output-dir
	public static void main(String[] args) throws Exception {
		Path doc = new Path(args[0]);
		Configuration baseConf = new Configuration();
		Path work = new Path(args[1]);
		Path tokenizedDoc = work.suffix("tokenized-document");
		Path modelPath = new Path(args[2]);
		Path dictionary = new Path(args[3]);
		Path output = new Path(args[4]);
		Path grams = work.suffix("grams");
		Path partialVectors = work.suffix("partial-vectors");
		Path tfvectors = work.suffix("tf-Vectors");
		Path tfidfvectors = work.suffix("tfidf-vectors");
		DocumentProcessor.tokenizeDocuments(doc, StandardAnalyzer.class,
				tokenizedDoc, baseConf);
		CollocDriver.generateAllGrams(tokenizedDoc, grams, baseConf, NGRAM_SIZE, MIN_SUPPORT,
				MIN_LLR_VALUE, 1);
		FileSystem fs = FileSystem.get(baseConf);
		FileStatus[] statuses = fs.listStatus(dictionary);
		List<Path> dictionaryChunks = Arrays.asList(FileUtil
				.stat2Paths(statuses));
		int partialVectorIndex = 0;
		Collection<Path> partialVectorPaths = Lists.newArrayList();
		int[] maxTermDimension = new int[1];
		for (Path dictionaryChunk : dictionaryChunks) {
			Path partialVectorOutputPath = partialVectors.suffix("pv-"+partialVectorIndex++);
			partialVectorPaths.add(partialVectorOutputPath);
			makePartialVectors(tokenizedDoc, baseConf, NGRAM_SIZE, dictionaryChunk,
					partialVectorOutputPath, maxTermDimension[0], true, true, 1);
		}
		Configuration conf = new Configuration(baseConf);
		PartialVectorMerger.mergePartialVectors(partialVectorPaths, tfvectors,
				conf, NORM_POWER, LOG_NORMALIZE, maxTermDimension[0],
				true, true, 1);
		String[] convArgs = new String[3];

		convArgs[0] = tfvectors.toString();
		convArgs[1] = work.suffix("conversion").toString();
		convArgs[2] = tfidfvectors.toString();
		Converter.main(convArgs);

		
		NaiveBayesModel model = NaiveBayesModel.materialize(modelPath, conf);
		StandardNaiveBayesClassifier classifier = new StandardNaiveBayesClassifier(model);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs,tfidfvectors,baseConf);
		SequenceFile.Writer outputFile = SequenceFile.createWriter(fs, conf, output, Text.class, VectorWritable.class);
		VectorWritable docVect = new VectorWritable();
		VectorWritable classification = new VectorWritable();
		Text docName = new Text();
		while(reader.next(docName, docVect)){
			classification.set(classifier.classify(docVect.get()));
			outputFile.append(docName,classification);
		}
		reader.close();
	}

	private static void makePartialVectors(Path input, Configuration baseConf,
			int maxNGramSize, Path dictionaryFilePath, Path output,
			int dimension, boolean sequentialAccess, boolean namedVectors,
			int numReducers) throws IOException, InterruptedException,
			ClassNotFoundException {
		Configuration conf = new Configuration(baseConf);
		conf.set(
				"io.serializations",
				"org.apache.hadoop.io.serializer.JavaSerialization,"
						+ "org.apache.hadoop.io.serializer.WritableSerialization");
		conf.setInt(PartialVectorMerger.DIMENSION, dimension);
		conf.setBoolean(PartialVectorMerger.SEQUENTIAL_ACCESS, sequentialAccess);
		conf.setBoolean(PartialVectorMerger.NAMED_VECTOR, namedVectors);
		conf.setInt("max.ngrams", maxNGramSize);
		DistributedCache.setCacheFiles(
				new URI[] { dictionaryFilePath.toUri() }, conf);
		Job job = new Job(conf);
		job.setJobName("DictionaryVectorizer::MakePartialVectors: input-folder: "
				+ input + ", dictionary-file: " + dictionaryFilePath);
		job.setJarByClass(DictionaryVectorizer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StringTuple.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(VectorWritable.class);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);
		job.setMapperClass(Mapper.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setReducerClass(TFPartialVectorReducer.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setNumReduceTasks(numReducers);
		HadoopUtil.delete(conf, output);
		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded) {
			throw new IllegalStateException("Job failed!");
		}

	}
}