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
import java.io.InputStream;
import org.apache.hadoop.fs.Seekable;
import org.tukaani.xz.SeekableInputStream;

public class XZSeekableInputStream extends SeekableInputStream {

    private final InputStream seekableIn;
    private final long length;

    XZSeekableInputStream(InputStream in, long len) {
        seekableIn = in;
        length = len;
    }

    @Override
    public long length() throws IOException {
        return length;
    }

    @Override
    public long position() throws IOException {
        return ((Seekable) seekableIn).getPos();
    }

    @Override
    public void seek(long pos) throws IOException {
        ((Seekable) seekableIn).seek(pos);
    }

    @Override
    public int read() throws IOException {
        if (((Seekable) seekableIn).getPos() <length) {
            return seekableIn.read();
        }
        return -1;
    }

}
