package edu.rose_hulman.mahout.dan;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VectorWritable;

/**
 * Hello world!
 *
 */



public class Dan {

	public static class Reader {

		Iterator<FileStatus> statuses;
		BufferedReader reader = null;
		FileSystem fs;

		public Reader(Path p,FileSystem fs) throws FileNotFoundException, IOException{
			this.fs = fs;
			statuses = Arrays.asList(fs.listStatus(p)).iterator();
			if(!statuses.hasNext()){
				return;
			}
			FileStatus s = statuses.next();
			while(s.isDirectory()){
				s = statuses.next();
				if(s == null){
					return;
				}
			}
			reader = new BufferedReader(new InputStreamReader(fs.open(s.getPath())));
		}

		public String readLine() throws IOException{
			if(reader == null){
				return null;
			}
			String line = reader.readLine();
			if(line != null){
				return line;
			}
			FileStatus s;
			do {
				do {
					if(!statuses.hasNext()){
						reader = null;
						return null;
					}
					s = statuses.next();
				} while(s.isDirectory());
				reader = new BufferedReader(new InputStreamReader(fs.open(s.getPath())));
				line = reader.readLine();
			} while(line == null);
			return line;
		}
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IllegalArgumentException, IOException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Reader reader = new Reader(new Path(args[0]),fs);
		SequenceFile.Writer frequencies = new SequenceFile.Writer(fs, conf, new Path("file"), Text.class,VectorWritable.class);
		VectorWritable vector = new VectorWritable();
		String year = null;
		for(String line = reader.readLine(); line != null; line = reader.readLine()){
			RandomAccessSparseVector yearVector = new RandomAccessSparseVector(Integer.MAX_VALUE);
			while(line != null){
				String[] tokens = line.split("\t");
				if(tokens[2] != year){
					if(year == null){
						year = tokens[2];
					} else {
						year = tokens[2];
						break;
					}
				}
				yearVector.set(Integer.parseInt(tokens[0]),Long.parseLong(tokens[3]));
				line = reader.readLine();
			}
			vector.set(new NamedVector(yearVector,year));
			frequencies.append(new Text(year),vector);
		}
		frequencies.close();
	}
}
