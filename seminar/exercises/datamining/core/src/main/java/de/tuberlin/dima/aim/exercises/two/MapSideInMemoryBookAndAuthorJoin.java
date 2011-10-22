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

package de.tuberlin.dima.aim.exercises.two;

import de.tuberlin.dima.aim.exercises.hadoop.HadoopJob;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.google.common.collect.ComparisonChain;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class MapSideInMemoryBookAndAuthorJoin extends HadoopJob {
	
  @Override
  public int run(String[] args) throws Exception {

    Map<String,String> parsedArgs = parseArgs(args);

    Path authors = new Path(parsedArgs.get("--authors"));
    Path books = new Path(parsedArgs.get("--books"));
    Path outputPath = new Path(parsedArgs.get("--output"));

    Job mapSideInMemoryJoin = prepareJob(books, outputPath, TextInputFormat.class, MapSideInMemoryJoinMapper.class,
    		Text.class, TitleYearWritable.class, MapSideInMemoryJoinReducer.class, Text.class, TitleYearWritable.class, TextOutputFormat.class);
    
    mapSideInMemoryJoin.getConfiguration().set(MapSideInMemoryJoinMapper.AUTHORS_PATH, authors.toUri().toString());
    mapSideInMemoryJoin.waitForCompletion(true);
    
    return 0;
  }
  
  static class MapSideInMemoryJoinMapper extends Mapper<Object, Text, Text, TitleYearWritable> {
	  
	  private HashMap<Integer, String> buildTable;
	  
	  static final String AUTHORS_PATH = MapSideInMemoryJoinMapper.class.getName() + ".authorsPath";
	  
	  @Override
	  protected void setup(Context ctx) throws java.io.IOException ,InterruptedException {
		  buildTable = new HashMap<Integer, String>();
		  String as = ctx.getConfiguration().get(AUTHORS_PATH);
		  Path authors = new Path(as);
		  FileSystem fs = authors.getFileSystem(ctx.getConfiguration());
		  
		  BufferedReader bufReader = new BufferedReader(new InputStreamReader(fs.open(authors)));
		  
		  String line = bufReader.readLine();		  
		  while (line != null) {
			  String[] fields = line.split("\t");
			  Integer id = Integer.parseInt(fields[0]);
			  String author = fields[1];
			  buildTable.put(id, author);
			  line = bufReader.readLine();
		  }
	  }
	  
	  @Override
	  protected void map(Object key, Text line, Context ctx) throws java.io.IOException ,InterruptedException {
		  String[] fields = line.toString().split("\t");
		  
		  Integer id = Integer.parseInt(fields[0]);
		  Integer year = Integer.parseInt(fields[1]);
		  String title = fields[2];
		  
		  String author = buildTable.get(id);
		  if (author != null) {
			  ctx.write(new Text(author), new TitleYearWritable(title, year));
		  }
	  }
  }
  
  static class MapSideInMemoryJoinReducer extends Reducer<Text, TitleYearWritable, Text, TitleYearWritable> {
	  
	  @Override
	  protected void reduce(Text key, Iterable<TitleYearWritable> values, Context ctx) throws java.io.IOException ,InterruptedException {
		  for (TitleYearWritable value: values) {
			  ctx.write(key, value);
		  }
	  }
  }
  
  static class TitleYearWritable implements WritableComparable<TitleYearWritable> {
	  
	  String title;
	  int year;
	  
	  public TitleYearWritable() {
		  super();
		  title = "";
		  year = -1;
	  }
	  
	  public TitleYearWritable(String title, int year) {
		  super();
		  this.title = title;
		  this.year = year;
	  }
	  
	@Override
	public void readFields(DataInput in) throws IOException {
		int titleLen = in.readInt();
		
		StringBuilder strBld = new StringBuilder();
		for (int i = 0; i < titleLen; i++) {
			strBld.append(in.readChar());
		}
		title = strBld.toString();
		year = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(title.length());
		out.writeChars(title);
		out.writeInt(year);
	}

	@Override
	public int compareTo(TitleYearWritable other) {
		return ComparisonChain.start().compare(title, other.title).compare(year, other.year).result();
	}
	
	@Override
	public String toString() {
		return title + "\t" + year;
	}
  }

}
