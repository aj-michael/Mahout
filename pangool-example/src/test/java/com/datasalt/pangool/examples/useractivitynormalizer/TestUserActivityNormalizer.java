/**
 * Copyright [2012] [Datasalt Systems S.L.]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasalt.pangool.examples.useractivitynormalizer;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.nio.charset.Charset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import com.datasalt.pangool.utils.HadoopUtils;
import com.datasalt.pangool.utils.test.AbstractHadoopTestLibrary;
import com.google.common.io.Files;

public class TestUserActivityNormalizer extends AbstractHadoopTestLibrary{

	private final static String INPUT = "test-input-" + TestUserActivityNormalizer.class.getName();
	private final static String OUTPUT = "test-output-" + TestUserActivityNormalizer.class.getName();
	
	@Test
	public void test() throws Exception {

		Configuration conf = new Configuration();
		FileSystem fS = FileSystem.get(conf);
		HadoopUtils.deleteIfExists(fS, new Path(OUTPUT));
		Files.write(
				"user1" + "\t" + "feat1" + "\t" + "10" + "\n" +
				"user1" + "\t" + "feat1" + "\t" + "20" + "\n" +
				"user1" + "\t" + "feat2" + "\t" + "30" + "\n" +
				"user2" + "\t" + "feat1" + "\t" + "10" + "\n" +
				"user2" + "\t" + "feat2" + "\t" + "10" + "\n" +
				"user2" + "\t" + "feat3" + "\t" + "10" + "\n"
		, new File(INPUT), Charset.forName("UTF-8"));

		ToolRunner.run(getConf(),new UserActivityNormalizer(), new String[] { INPUT, OUTPUT });
		
		int validatedOutputLines = 0;
		
		for(String line : Files.readLines(new File(OUTPUT + "/part-r-00000"), Charset.forName("UTF-8"))) {
			String[] fields = line.split("\t");
			if(fields[0].equals("user1")) {
				if(fields[1].equals("feat1")) {
					assertEquals(0.5, Double.parseDouble(fields[2]), 0.00001);
					validatedOutputLines++;
				} else if(fields[1].equals("feat2")) {
					assertEquals(0.5, Double.parseDouble(fields[2]), 0.00001);
					validatedOutputLines++;
				}
			} else if(fields[0].equals("user2")) {
				if(fields[1].equals("feat1")) {
					assertEquals(0.33, Double.parseDouble(fields[2]), 0.01);
					validatedOutputLines++;
				} else if(fields[1].equals("feat2")) {
					assertEquals(0.33, Double.parseDouble(fields[2]), 0.01);
					validatedOutputLines++;
				} else if(fields[1].equals("feat3")) {
					assertEquals(0.33, Double.parseDouble(fields[2]), 0.01);
					validatedOutputLines++;
				}
			}
		}
		
		assertEquals(5, validatedOutputLines);

		HadoopUtils.deleteIfExists(fS, new Path(INPUT));
		HadoopUtils.deleteIfExists(fS, new Path(OUTPUT));
	}
}
