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
import java.io.OutputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZOutputStream;

public class XZCompressionOutputStream extends CompressionOutputStream {

    private final int presetLevel;

    private final long blockSize;

    private XZOutputStream xzOut;

    private boolean resetStateNeeded;

    private long blockOffset;

    public XZCompressionOutputStream(OutputStream out, int presetLevel, long blockSize) throws IOException {
        super(out);
        this.presetLevel = presetLevel;
        this.blockSize = blockSize;
        resetStateNeeded = true;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (resetStateNeeded) {
            resetStateNeeded = false;
            xzOut = new XZOutputStream(out, new LZMA2Options(presetLevel));
            blockOffset = 0;
        }
        while (len > 0) {
            int chunk = (int) (blockOffset + len < blockSize ? len : blockSize - blockOffset);
            xzOut.write(b, off, chunk);
            off += chunk;
            len -= chunk;
            blockOffset += chunk;
            if (blockOffset == blockSize) {
                xzOut.endBlock();
                blockOffset = 0;
            }
        }
    }

    @Override
    public void finish() throws IOException {
        if (resetStateNeeded) {
            resetStateNeeded = false;
            xzOut = new XZOutputStream(out, new LZMA2Options(presetLevel));
            blockOffset = 0;
        }
        xzOut.finish();
        resetStateNeeded = true;
    }

    @Override
    public void resetState() throws IOException {
        resetStateNeeded = true;
    }

    @Override
    public void write(int b) throws IOException {
        if (resetStateNeeded) {
            resetStateNeeded = false;
            xzOut = new XZOutputStream(out, new LZMA2Options(presetLevel));
            blockOffset = 0;
        }
        xzOut.write(b);
        blockOffset++;
        if (blockOffset == blockSize) {
            xzOut.endBlock();
            blockOffset = 0;
        }
    }

    @Override
    public void flush() throws IOException {
        if (resetStateNeeded) {
            resetStateNeeded = false;
            xzOut = new XZOutputStream(out, new LZMA2Options(presetLevel));
            blockOffset = 0;
        }
        xzOut.flush();
    }

    @Override
    public void close() throws IOException {
        if (resetStateNeeded) {
            resetStateNeeded = false;
            xzOut = new XZOutputStream(out, new LZMA2Options(presetLevel));
            blockOffset = 0;
        }
        xzOut.flush();
        xzOut.close();
        resetStateNeeded = false;
    }
}
