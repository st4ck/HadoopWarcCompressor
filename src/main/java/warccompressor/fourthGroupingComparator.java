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
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class fourthGroupingComparator extends WritableComparator {
	public fourthGroupingComparator() {
		super(groupOffsetPair.class, true);
	}

     @Override
     // This comparator controls which keys are grouped
     // together into a single call to the reduce() method
     public int compare(WritableComparable wc1, WritableComparable wc2) {
         groupOffsetPair pair = (groupOffsetPair) wc1;
         groupOffsetPair pair2 = (groupOffsetPair) wc2;
         return pair.getGroup().compareTo(pair2.getGroup());
     }
 }