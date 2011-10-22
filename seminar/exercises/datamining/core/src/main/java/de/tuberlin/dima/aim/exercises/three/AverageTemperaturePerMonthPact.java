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

package de.tuberlin.dima.aim.exercises.three;

import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.common.type.base.PactString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import com.google.common.collect.ComparisonChain;

public class AverageTemperaturePerMonthPact implements PlanAssembler, PlanAssemblerDescription {

  public static final double MINIMUM_QUALITY = 0.25;

  @Override
  public String getDescription() {
    return null;
  }

  @Override
  public Plan getPlan(String... args) throws IllegalArgumentException {
    return null;
  }

  public static class TemperaturePerYearAndMonthMapper extends MapStub<PactNull, PactString, YearMonthKey, PactInteger> {

    @Override
    public void map(PactNull pactNull, PactString line, Collector<YearMonthKey, PactInteger> collector) {
      	String[] fields = line.toString().split("\t");
      	
      	short year = Short.parseShort(fields[0]);
      	short month = Short.parseShort(fields[1]);
      	int temperature = Integer.parseInt(fields[2]);
      	double quality = Double.parseDouble(fields[3]);
      	
      	if (quality >= MINIMUM_QUALITY) {
      		collector.collect(new YearMonthKey(year, month), new PactInteger(temperature));
      	}
    }
  }

  public static class TemperatePerYearAndMonthReducer
      extends ReduceStub<YearMonthKey, PactInteger, YearMonthKey, PactDouble> {

    @Override
    public void reduce(YearMonthKey yearMonthKey, Iterator<PactInteger> temperatures,
        Collector<YearMonthKey, PactDouble> collector) {
    	int sum = 0;
    	int count = 0;
    	double avgTemp = 0;
    	
    	while (temperatures.hasNext()) {
    		sum += temperatures.next().getValue();
    		count++;
    	}
    	
    	avgTemp = (double) sum / (double) count;
    	collector.collect(yearMonthKey, new PactDouble(avgTemp));
    }
  }

  public static class YearMonthKey implements Key {

    private short year;
    private short month;
	  
	public YearMonthKey() {}

    public YearMonthKey(short year, short month) {
    	this.year = year;
    	this.month = month;
    }

    @Override
    public int compareTo(Key other) {
    	if (other != null && other instanceof YearMonthKey) {
    		YearMonthKey o = (YearMonthKey) other;
    		return ComparisonChain.start().compare(year, o.year).compare(month, o.month).result();
    	}
    	else {
    		return -1;
    	}    	
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeShort(year);
      out.writeShort(month);
    }

    @Override
    public void read(DataInput in) throws IOException {
      year = in.readShort();
      month = in.readShort();
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + month;
		result = prime * result + year;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		YearMonthKey other = (YearMonthKey) obj;
		if (month != other.month)
			return false;
		if (year != other.year)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "YearMonthKey [year=" + year + ", month=" + month + "]";
	}
  }
}