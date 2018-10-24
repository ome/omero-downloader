/*
 * Copyright (C) 2016 University of Dundee & Open Microscopy Environment.
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

package org.openmicroscopy.client.downloader.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Abstracts basic file I/O operations behind a simple API. Each instance may be thought of as a file handle.
 * Not thread-safe.
 * @author m.t.b.carroll@dundee.ac.uk
 */
public class FileIO implements Closeable {

    /* Neither java.io nor java.nio make read-write file I/O simple and obvious so this class hides the hoop-jumping
     * behind a comprehensible API while making it easy to change the strategy. This would be much easier with POSIX. */

    public static final int INT_BYTES = Integer.SIZE >> 3;

    private static final StandardOpenOption[] READ_ONLY_OPTIONS =
            new StandardOpenOption[] {StandardOpenOption.READ};
    private static final StandardOpenOption[] READ_WRITE_OPTIONS =
            new StandardOpenOption[] {StandardOpenOption.READ, StandardOpenOption.CREATE, StandardOpenOption.WRITE};

    private final FileChannel channel;
    private final ByteBuffer intBuffer = ByteBuffer.allocate(INT_BYTES);

    /**
     * Open a new file handle for the given file.
     * @param file a file
     * @param isWritable if write operations may be required
     * @throws IOException if the handle could not be opened
     */
    public FileIO(File file, boolean isWritable) throws IOException {
        channel = (FileChannel) Files.newByteChannel(file.toPath(), isWritable ? READ_WRITE_OPTIONS : READ_ONLY_OPTIONS);
    }

    /**
     * Reads {@link #INT_BYTES} bytes from the file and returns them as an integer.
     * @return an integer read from the file
     * @throws IOException if the read failed
     */
    public int readInt() throws IOException {
        intBuffer.position(0);
        channel.read(intBuffer);
        intBuffer.position(0);
        return intBuffer.getInt();
    }

    /**
     * Writes {@link #INT_BYTES} bytes to the file.
     * @see #readInt()
     * @param n the integer to write to the file
     * @throws IOException if the write failed
     */
    public void writeInt(int n) throws IOException {
        intBuffer.position(0);
        intBuffer.putInt(n);
        intBuffer.position(0);
        channel.write(intBuffer);
    }

    /**
     * Read a byte array from the file.
     * Before the byte array is returned it is decompressed.
     * @param size how many bytes to read
     * @return the bytes read then decompressed from the file
     * @throws IOException if the read failed
     */
    public byte[] readBytes(int size) throws IOException {
        final ByteBuffer compressedStream = ByteBuffer.allocate(size);
        do {
            if (channel.read(compressedStream) < 1) {
                throw new IOException("could read only " + compressedStream.position() + " bytes, not " + size);
            }
        } while (compressedStream.hasRemaining());
        final InputStream uncompressedView = new GZIPInputStream(new ByteArrayInputStream(compressedStream.array()));
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        while (true) {
            final int nextByte = uncompressedView.read();
            if (nextByte == -1) {
                break;
            } else {
                bytes.write(nextByte);
            }
        }
        return bytes.toByteArray();
    }

    /**
     * Write a byte array to the file.
     * @see #readBytes(int)
     * @param bytes the bytes to compress then write to the file
     * @throws IOException if the write failed
     */
    public void writeBytes(byte[] bytes) throws IOException {
        final ByteArrayOutputStream compressedStream = new ByteArrayOutputStream();
        try (final OutputStream uncompressedView = new GZIPOutputStream(compressedStream)) {
            uncompressedView.write(bytes);
        }
        channel.write(ByteBuffer.wrap(compressedStream.toByteArray()));
    }

    /**
     * Set the current read/write position in the file.
     * @param position the position, as the byte count from the start of the file
     * @throws IOException if the position could not be set
     */
    public void seek(long position) throws IOException {
        channel.position(position);
    }

    /**
     * Return the current read/write position in the file.
     * @return the position, as the byte count from the start of the file
     * @throws IOException if the position could not be determined
     */
    public long tell() throws IOException {
        return channel.position();
    }

    /**
     * Flush written data to the underlying file.
     * @throws IOException if the data could not be flushed
     */
    public void flush() throws IOException {
        channel.force(false);
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
