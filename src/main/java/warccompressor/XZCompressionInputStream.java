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

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.tukaani.xz.XZInputStream;

/**
 *
 * @author yongtang
 */
public class XZCompressionInputStream extends CompressionInputStream {

    private BufferedInputStream bufferedIn;

    private XZInputStream xzIn;

    private boolean resetStateNeeded;

    public XZCompressionInputStream(InputStream in) throws IOException {
        super(in);
        resetStateNeeded = false;
        bufferedIn = new BufferedInputStream(super.in);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (resetStateNeeded) {
            resetStateNeeded = false;
            bufferedIn = new BufferedInputStream(super.in);
            xzIn = null;
        }
        return getInputStream().read(b, off, len);
    }

    @Override
    public void resetState() throws IOException {
        resetStateNeeded = true;
    }

    @Override
    public int read() throws IOException {
        byte b[] = new byte[1];
        int result = this.read(b, 0, 1);
        return (result < 0) ? result : (b[0] & 0xff);
    }

    @Override
    public void close() throws IOException {
        if (!resetStateNeeded) {
            if (xzIn != null) {
                xzIn.close();
                xzIn = null;
            }
            resetStateNeeded = true;
        }
    }

    /**
     * This compression stream ({@link #xzIn}) is initialized lazily, in case
     * the data is not available at the time of initialization. This is
     * necessary for the codec to be used in a {@link SequenceFile.Reader}, as
     * it constructs the {@link XZCompressionInputStream} before putting data
     * into its buffer. Eager initialization of {@link #xzIn} there results in
     * an {@link EOFException}.
     */
    private XZInputStream getInputStream() throws IOException {
        if (xzIn == null) {
            xzIn = new XZInputStream(bufferedIn);
        }
        return xzIn;
    }
}
