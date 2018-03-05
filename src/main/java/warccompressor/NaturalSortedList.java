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

import java.util.Comparator;

/**
 * Provides a {@code SortedList} which sorts the elements by their
 * natural order. 
 *
 * @author Mark Rhodes
 * @version 1.1
 * @see SortedList
 * @param <T> any {@code Comparable}
 */
public class NaturalSortedList<T extends Comparable<? super T>>
		extends SortedList<T> {

	private static final long serialVersionUID = -8834713008973648930L;

	/**
	 * Constructs a new {@code NaturalSortedList} which sorts elements
	 * according to their <i>natural order</i>.
	 */
	public NaturalSortedList(){
		super(new Comparator<T>(){
			public int compare(T one, T two){
				return one.compareTo(two);
			}
		}); 
	}
}