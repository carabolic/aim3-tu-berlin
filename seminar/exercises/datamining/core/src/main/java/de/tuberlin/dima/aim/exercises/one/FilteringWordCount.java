/**
 * Copyright (C) 2011 AIM III course DIMA TU Berlin
 *
 * This programm is free software; you can redistribute it and/or modify
 * it under the terms of the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.tuberlin.dima.aim.exercises.one;

import de.tuberlin.dima.aim.exercises.hadoop.HadoopJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;

public class FilteringWordCount extends HadoopJob {
	
  @Override
  public int run(String[] args) throws Exception {
    Map<String,String> parsedArgs = parseArgs(args);

    Path inputPath = new Path(parsedArgs.get("--input"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    Job wordCount = prepareJob(inputPath, outputPath, TextInputFormat.class, FilteringWordCountMapper.class,
        Text.class, IntWritable.class, WordCountReducer.class, Text.class, IntWritable.class, TextOutputFormat.class);
    wordCount.waitForCompletion(true);

    return 0;
  }

  static class FilteringWordCountMapper extends Mapper<Object,Text,Text,IntWritable> {
		private final String[] stopWords = {"to", "and", "in", "the"};
	  
    @Override
    protected void map(Object key, Text line, Context ctx) throws IOException, InterruptedException {
    	StringTokenizer strTok = new StringTokenizer(line.toString().replace(",", " "));
    	
    	// Use Lucene StandardAnalyzer
    	/* StandardAnalyzer stdAnal = new StandardAnalyzer(Version.LUCENE_30);
    	TokenStream stream = stdAnal.tokenStream("word", new StringReader(line.toString()));
    	stream.reset();
    	while //do do something to get all tokens
    		// confusing TokenStream API
    	*/
    	
    	while (strTok.hasMoreTokens()) {
    		String word = strTok.nextToken().toLowerCase();
    		
    		if (!isStopWord(word)) {
    			ctx.write(new Text(word), new IntWritable(1));
    		}
    	}
    }
    
    private boolean isStopWord(String word) {
    	for (String stopWord : stopWords) {
    		if (word.equals(stopWord)) {
    			return true;
    		}
    	}
    	return false;
    }
  }

  static class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context ctx)
        throws IOException, InterruptedException {
    	int sum = 0;
    	for (IntWritable val : values) {
    		sum += val.get();
    	}
    	ctx.write(key, new IntWritable(sum));
    }
  }

}
