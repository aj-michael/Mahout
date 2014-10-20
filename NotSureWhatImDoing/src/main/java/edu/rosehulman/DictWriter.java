package edu.rosehulman;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * Created by adam on 10/19/14.
 */
public class DictWriter {

    public static void main(String args[]) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(args[0]),conf);
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, new Path(args[1]), Text.class, IntWritable.class);
        for (File file : new File(args[0]).listFiles()) {
            InputStream fileStream = new FileInputStream(file);
            InputStrea
        }

    }

}
