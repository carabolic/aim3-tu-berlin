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
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.type.base.PactString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BookAndAuthorJoinPact implements PlanAssembler, PlanAssemblerDescription {

  @Override
  public String getDescription() {
    // IMPLEMENT ME
    return null;
  }

  @Override
  public Plan getPlan(String... args) throws IllegalArgumentException {
    // IMPLEMENT ME
    return null;
  }

  public static class BookAndAuthorMatch extends MatchStub<PactLong,PactString,BookAndYear,PactString,BookAndYear> {

    @Override
    public void match(PactLong authorID, PactString authorName, BookAndYear bookAndYear,
        Collector<PactString, BookAndYear> collector) {
    	System.out.println(authorName.getValue() + "\t" + bookAndYear.toString());
    	collector.collect(authorName, bookAndYear);
    }
  }

  public static class BookAndYear implements Value {

	private String title;
	private short year;
	  
    public BookAndYear() {}

    public BookAndYear(String title, short year) {
      this.title = title;
      this.year = year;
    }

    @Override
    public void write(DataOutput out) throws IOException {
    	out.writeInt(title.length());
        out.writeChars(title);
    	out.writeShort(year);
    }

    @Override
    public void read(DataInput in) throws IOException {
    	int len = in.readInt();
    	
    	StringBuilder strBld = new StringBuilder();
    	for (int i = 0; i < len; i++) {
    		strBld.append(in.readChar());
    	}
    	
    	title = strBld.toString();
    	year = in.readShort();
    } 

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((title == null) ? 0 : title.hashCode());
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
		BookAndYear other = (BookAndYear) obj;
		if (title == null) {
			if (other.title != null)
				return false;
		} else if (!title.equals(other.title))
			return false;
		if (year != other.year)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "BookAndYear [title=" + title + ", year=" + year + "]";
	}
  }
}
