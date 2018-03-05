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

import java.util.*;
import java.math.*;

public class RandomPermutations
{
	BigInteger a = BigInteger.ZERO;
	BigInteger b = BigInteger.ZERO;
	BigInteger p = new BigInteger("30016801");
	BigInteger c = new BigInteger("10000000000");
	Random rnd;

	public RandomPermutations(Random rnd)
	{
		this.rnd = rnd;
		while (a.compareTo(c) == -1) a = BigInteger.valueOf((((long)rnd.nextInt(32)) << 32) + (long)rnd.nextInt(32));
		while (b.compareTo(c) == -1) b = BigInteger.valueOf((((long)rnd.nextInt(32)) << 32) + (long)rnd.nextInt(32));
		while (p.compareTo(c) == -1) p = BigInteger.valueOf((((long)rnd.nextInt(32)) << 32) + (long)rnd.nextInt(32));
	}

	public BigInteger getMin(Fingerprint Fingerprints)
	{
		ArrayList<BigInteger> f = Fingerprints.getFingerprints();
		if (f.size() == 0) {
			return BigInteger.ZERO;
		}
		BigInteger min = f.get(0).multiply(a).add(b).remainder(p);
		for (BigInteger x:f)
		{
			BigInteger t = x.multiply(a).add(b).remainder(p);
			if ((t.compareTo(BigInteger.ZERO) != 0) && (t.compareTo(min) == -1)) min = t;
		}

		return min;
	}
}