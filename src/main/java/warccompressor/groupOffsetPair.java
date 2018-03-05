/*
This file is part of Hadoop WARC Compressor.

Hadoop WARC Compressor is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

Hadoop WARC Compressor is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with Hadoop WARC Compressor.  If not, see <http://www.gnu.org/licenses/>.
*/

package warccompressor;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.WritableComparable;

import java.io.*;
 
public class groupOffsetPair implements Writable, WritableComparable<groupOffsetPair> {
    private Text group = new Text();  // natural key
    private Text offset = new Text(); // secondary key

	public Text getOffset() {
		return offset;
	}
	
	public Text getGroup() {
		return group;
	}
	
	public void setOffset(Text offset) {
		this.offset = offset;
	}
	
	public void setGroup(Text group) {
		this.group = group;
	}
	
	@Override
	public String toString() {
		return (new StringBuilder()).append(group.toString()).append(',').append(offset.toString()).toString();
	}
	 
	@Override
	public void readFields(DataInput in) throws IOException {
		group = new Text();
		group.readFields(in);
		offset = new Text();
		offset.readFields(in);
	}
	 
	@Override
	public void write(DataOutput out) throws IOException {
		group.write(out);
		offset.write(out);
	}
	
	// this comparator controls the sort order of the keys.
    @Override
    public int compareTo(groupOffsetPair pair) {
        int compareValue = this.group.compareTo(pair.getGroup());
        if (compareValue == 0) { // if in the same group, compare offsets
            compareValue = this.offset.compareTo(pair.getOffset());
        }
        return compareValue; // sort ascending
    }
 }