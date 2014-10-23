package edu.rosehulman;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class TFVectorFactory {
	
	@SuppressWarnings("deprecation")
	public static void main(String args[]) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(args[0]),conf);
		SequenceFile.Writer sfwriter = SequenceFile.createWriter(fs,conf,new Path(args[1]),Text.class,VectorWritable.class);
		Vector v = new RandomAccessSparseVector(Integer.MAX_VALUE);
		int currentYear = 0;
	
		for (FileStatus fileStatus : fs.listStatus(new Path(args[0]))) {
			Path p = fileStatus.getPath();
			System.out.println("Started a new iteration of the loop");
			System.out.println(p.getName());
			FSDataInputStream dis = fs.open(p);
			InputStream gzipStream = new GZIPInputStream(dis);
			Reader decoder = new InputStreamReader(gzipStream, "UTF-8");
			BufferedReader buf = new BufferedReader(decoder);
			for (String line = buf.readLine(); line != null; line = buf.readLine()) {
				String[] vals = line.split("\t");
				String id = vals[0];
				//String word = vals[1];
				int year = Integer.parseInt(vals[2]);
				//String docs = vals[3];
				String freq = vals[4];
				if (year > currentYear) {
					if (currentYear > 0) {
						sfwriter.append(new Text(String.valueOf(currentYear)), new VectorWritable(new NamedVector(v,String.valueOf(currentYear))));
					}
					currentYear = year;
					v = new RandomAccessSparseVector(Integer.MAX_VALUE);
				}
				v.set(Integer.parseInt(id), Double.parseDouble(freq));
			}
			buf.close();
		}
		sfwriter.close();
	}
	
}
