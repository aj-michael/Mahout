package edu.rose_hulman.mahout.dan;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.vectorizer.collocations.llr.Gram;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;

/**
 * Hello world!
 *
 */

@SuppressWarnings("deprecation")
public class Dan {

	public static class Reader {

		private Iterator<FileStatus> statuses;
		private BufferedReader reader = null;
		private FileSystem fs;

		public Reader(Path p, FileSystem fs) throws FileNotFoundException,
				IOException {
			this.fs = fs;
			statuses = Arrays.asList(fs.listStatus(p)).iterator();
			FileStatus s;
			do {
				if (!statuses.hasNext()) {
					return;
				}
				s = statuses.next();
			} while (s.isDirectory());
			reader = new BufferedReader(new InputStreamReader(
					new GZIPInputStream(fs.open(s.getPath()))));
		}

		public String readLine() throws IOException {
			if (reader == null) {
				return null;
			}
			String line;
			FileStatus s;
			while (null == (line = reader.readLine())) {
				reader.close();
				do {
					if (!statuses.hasNext()) {
						reader = null;
						return null;
					}
					s = statuses.next();
				} while (s.isDirectory());
				reader = new BufferedReader(new InputStreamReader(
						new GZIPInputStream(fs.open(s.getPath()))));
			}
			return line;
		}
	}

	private static final int DICTIONARY_BYTE_OVERHEAD = 4;
	private static final String DICTIONARY_FILE = "dictionary.file-";
	private static String OUTPUT_FILES_PATTERN = "part-*";
	private static final String VECTOR_OUTPUT_FOLDER = "partial-vectors-";

	private static List<Path> createDictionaryChunks(
			Path wordCountPath,
			Path dictionaryPathBase, Configuration baseConf,
			int chunkSizeInMegabytes, int[] maxTermDimension)
			throws IOException {
		List<Path> chunkPaths = Lists.newArrayList();
		Configuration conf = new Configuration(baseConf);
		FileSystem fs = FileSystem.get(wordCountPath.toUri(), conf);
		long chunkSizeLimit = chunkSizeInMegabytes * 1024L * 1024L;
		int chunkIndex = 0;
		Path chunkPath = new Path(dictionaryPathBase, DICTIONARY_FILE
				+ chunkIndex);
		chunkPaths.add(chunkPath);
		SequenceFile.Writer dictWriter = new SequenceFile.Writer(fs, conf,
				chunkPath, Text.class, IntWritable.class);
		try {
			long currentChunkSize = 0;
			Path filesPattern = new Path(wordCountPath, OUTPUT_FILES_PATTERN);
			int i = 0;
			for (Pair<Writable, Writable> record : new SequenceFileDirIterable<Writable, Writable>(
					filesPattern, PathType.GLOB, null, null, true, conf)) {
				if (currentChunkSize > chunkSizeLimit) {
					Closeables.close(dictWriter, false);
					chunkIndex++;
					chunkPath = new Path(dictionaryPathBase, DICTIONARY_FILE
							+ chunkIndex);
					chunkPaths.add(chunkPath);
					dictWriter = new SequenceFile.Writer(fs, conf, chunkPath,
							Text.class, IntWritable.class);
					currentChunkSize = 0;
				}
				Writable key = record.getFirst();
				int fieldSize = DICTIONARY_BYTE_OVERHEAD
						+ key.toString().length() * 2 + Integer.SIZE / 8;
				currentChunkSize += fieldSize;
				dictWriter.append(key, new IntWritable(i++));
			}
			maxTermDimension[0] = i;
		} finally {
			Closeables.close(dictWriter, false);
		}
		return chunkPaths;
	}

	public static void main(String[] args) throws IllegalArgumentException,
			IOException {
		if (args.length != 2) {
			System.out.println("wrong arguments");
			return;
		}
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Reader reader = new Reader(new Path(args[0]), fs);
		String currentYear = null;
		String line;
		Path dictionaryJobPath = new Path(args[1]);
		Path tfVectors = new Path(args[2]);
		Option opt1 = SequenceFile.Writer.file(new Path(args[1]));
		Option opt2 = SequenceFile.Writer.keyClass(Text.class);
		Option opt3 = SequenceFile.Writer.valueClass(LongWritable.class);
		SequenceFile.Writer writer = SequenceFile.createWriter(conf, opt1, opt2, opt3);
		String currentNgram = null;
		long totalFreq = 0;
		while(null != (line = reader.readLine())) {
			String[] tokens = line.split("\t");
			String ngram = tokens[1];
			long freq = Long.parseLong(tokens[2]);
			if(ngram.equals(currentNgram)){
				totalFreq += freq;
			} else {
				if(currentNgram != null){
					writer.append(new Text(ngram), new LongWritable(freq));
				}
				currentNgram = ngram;
				totalFreq = Long.parseLong(tokens[2]);
			}
		}
		List<Path> dictionaryChunks = createDictionaryChunks(dictionaryJobPath, tfVectors, conf, 100, new int[1]);
		int partialVectorIndex = 0;
		Collection<Path> partialVectorPaths = Lists.newArrayList();
		for (Path dictionaryChunk : dictionaryChunks) {
			Path partialVectorOutputPath = new Path(tfVectors, VECTOR_OUTPUT_FOLDER + partialVectorIndex++);
			partialVectorPaths.add(partialVectorOutputPath);
			makePartialVectors(input, baseConf, maxNGramSize, dictionaryChunk, partialVectorOutputPath,
					maxTermDimension[0], sequentialAccess, namedVectors, numReducers);
		}
		Configuration conf = new Configuration(baseConf);
		Path outputDir = new Path(output, tfVectorsFolderName);
		PartialVectorMerger.mergePartialVectors(partialVectorPaths, outputDir, conf, normPower, logNormalize,
				maxTermDimension[0], sequentialAccess, namedVectors, numReducers);
		HadoopUtil.delete(conf, partialVectorPaths);
	}
}
