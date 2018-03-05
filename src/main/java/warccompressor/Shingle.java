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

public class Shingle
{

	int httpRequestStart;
	int headerStart;
	int bodyStart;
	String url;
	ArrayList<Integer> bodyShingles = null;
	String page;

	public Shingle(String page)
	{
		this.page = page;
		
		int urlStart = page.indexOf("response ") + 9;
		int urlEnd = page.indexOf(" ", urlStart + 1);
		
		url = "";
		if (urlEnd > urlStart) url = page.substring(urlStart, urlEnd);

		httpRequestStart = page.indexOf("\r\n\r\n");
		headerStart = page.indexOf("\r\n\r\n", httpRequestStart + 4);
				
		bodyStart = page.indexOf("<body");
		if (bodyStart == -1) bodyStart = page.indexOf("<BODY");

		if (httpRequestStart == -1) httpRequestStart = 0;
		if (headerStart == -1) headerStart = 0;
		if (bodyStart == -1) bodyStart = headerStart;

		bodyShingles = shingleBySeparator(bodyStart, '.');
	}

	public ArrayList<Integer> getBodyShingles()
	{
		return bodyShingles;
	}
	
	public int getShinglesCount()
	{
		return bodyShingles.size();
	}

	public String getUrl()
	{
		return url;
	}


	public ArrayList<Integer> shingleBySeparator(int offset, char separator)
	{
		int pos = offset;
		ArrayList<Integer> shingles = new ArrayList<Integer>();
		
		while ((pos = page.indexOf(separator,pos+1)) != -1) {
			shingles.add(pos);
		}

		return shingles;
	}
}