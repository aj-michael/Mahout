package edu.rosehulman;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class DFVectorFactory {
	
	@SuppressWarnings("deprecation")
	public static void main(String args[]) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(args[0]),conf);
		SequenceFile.Writer sfwriter = SequenceFile.createWriter(fs,conf,new Path(args[1]),Text.class,VectorWritable.class);
		Vector v = new RandomAccessSparseVector(Integer.MAX_VALUE);
		int currentYear = 0;
		for (File file : new File(args[0]).listFiles()) {
			InputStream fileStream = new FileInputStream(file);
			InputStream gzipStream = new GZIPInputStream(fileStream);
			Reader decoder = new InputStreamReader(gzipStream, "UTF-8");
			BufferedReader buf = new BufferedReader(decoder);
			for (String line = buf.readLine(); line != null; line = buf.readLine()) {
				String[] vals = line.split("\t");
				String id = vals[0];
				//String word = vals[1];
				int year = Integer.parseInt(vals[2]);
				String docs = vals[3];
				//String freq = vals[4];
				if (year > currentYear) {
					if (currentYear > 0) {
						sfwriter.append(new Text(String.valueOf(currentYear)), new VectorWritable(new NamedVector(v,String.valueOf(currentYear))));
					}
					currentYear = year;
					v = new RandomAccessSparseVector(Integer.MAX_VALUE);
				}
				v.set(Integer.parseInt(id), Double.parseDouble(docs));
			}
			buf.close();
		}
		sfwriter.close();
	}
	
}
