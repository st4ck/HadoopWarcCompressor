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

This library is derivated from the code https://github.com/yongtang/hadoop-xz
under Apache License Version 2.0, January 2004. Author yongtang
*/

package warccompressor;

import java.io.IOException;
import org.apache.hadoop.io.compress.Decompressor;

public class XZDecompressor implements Decompressor {

    @Override
    public void setInput(byte[] b, int off, int len) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean needsInput() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setDictionary(byte[] b, int off, int len) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean needsDictionary() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean finished() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int decompress(byte[] b, int off, int len) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getRemaining() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void reset() {
        // do nothing
    }

    @Override
    public void end() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
