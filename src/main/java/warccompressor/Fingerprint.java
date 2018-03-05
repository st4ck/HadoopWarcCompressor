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
import java.io.*;

import org.apache.hadoop.mapreduce.Mapper.Context;

class Fingerprint
{
	ArrayList<BigInteger> Fingerprints = null;
	String page = null;

	public ArrayList<BigInteger> getFingerprints()
	{
		return Fingerprints;
	}

	public Fingerprint(ArrayList<Integer> shingles, String page, int permNumber, Context context)
	{
		Fingerprints = new ArrayList<BigInteger>();
		this.page = page;
		
		if (shingles.size() < permNumber) return;

		for (int i=0; i<shingles.size() - 1; i++) {
			// avoid eventual timeout with huge bigger pages
			// reporting progress to hadoop
			context.progress();
			
			try {
				BigInteger x = calculateFingerprint(shingles.get(i), shingles.get(i + 1));

				if (x.compareTo(BigInteger.ZERO) == 1) {
					Fingerprints.add(x);
				}
			} catch (UnsupportedEncodingException ex) {
				System.out.println(ex.toString());
			} catch (ArrayIndexOutOfBoundsException ex) {
				System.out.println(ex.toString());
			}
		}

		if (Fingerprints.size() < permNumber) Fingerprints.clear();
	}

	public int getSize()
	{
		return Fingerprints.size();
	}

	private BigInteger calculateFingerprint(int p1, int p2) throws UnsupportedEncodingException
	{
		byte[] pageBytes = page.getBytes("ISO-8859-1");
		BigInteger siga = BigInteger.ZERO;
		BigInteger Q = new BigInteger("994534132561");
		//ulong D = 256;

		boolean openedtag = false;
		byte lastbyte = 0;
		int sizefingered = 0;
		int wordsfingered = 0;
		for (int i = p1; i < p2; i++)
		{
			if (pageBytes[i] == (byte)'<') { openedtag = true; continue; }
			else if (pageBytes[i] == (byte)'>') { if (!openedtag) { siga = BigInteger.ZERO; } openedtag = false; continue; }
			else if ((pageBytes[i] == (byte)'\t') || (pageBytes[i] == (byte)'\r') || (pageBytes[i] == (byte)'\n')) {
				continue;
			}
			else if ((pageBytes[i] == (byte)' ') && (lastbyte == (byte)' ')) { continue; }

			if (!openedtag) {
				sizefingered++;
				if (pageBytes[i] == ((byte)' ')) { wordsfingered++; }

				// Rolling hashing
				// (siga << 8) + (int)pageBytes[i]
				siga = (siga.shiftLeft(8).add(BigInteger.valueOf((int)pageBytes[i])));
				if (siga.compareTo(Q) == 1) { siga = siga.remainder(Q); /* siga %= Q; */ }
			}

			lastbyte = pageBytes[i];
		}

		//if ((sizefingered < 75) || (wordsfingered < 14)) {
		if ((sizefingered < 40) || (wordsfingered < 7))
		{
			siga = BigInteger.ZERO;
		}
		
		return siga;

	}
}