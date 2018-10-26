/*
 * Copyright (C) 2018 University of Dundee & Open Microscopy Environment.
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

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * An output stream that truncates the start and end of the data, passing only the remainder to the underlying output stream.
 * @author m.t.b.carroll@dundee.ac.uk
 */
public class TruncatingOutputStream extends FilterOutputStream {
  
    private final byte[] singletonByte = new byte[1];
    private final byte[] backlog;
    private final int finalSkip;
    private int stillToSkip;
    private int backlogSize = 0;

    /**
     * Construct a new truncating output stream.
     * @param initialSkip how many bytes to skip from the start
     * @param finalSkip how many bytes to skip from the end
     * @param out the output stream to which to write the remaining bytes
     */
    public TruncatingOutputStream(int initialSkip, int finalSkip, OutputStream out) {
        super(out);
        this.stillToSkip = initialSkip;
        this.finalSkip = finalSkip;
        this.backlog = new byte[finalSkip];
    }

    @Override
    public void write(int buffer) throws IOException {
        singletonByte[0] = (byte) buffer;
        write(singletonByte, 0, 1);
    }

    @Override
    public void write(byte[] buffer) throws IOException {
        write(buffer, 0, buffer.length);
    }

    @Override
    public void write(byte[] buffer, int offset, int length) throws IOException {
        if (stillToSkip > 0) {
            if (length > stillToSkip) {
                final int newOff = offset + stillToSkip;
                final int newLen = length - stillToSkip;
                stillToSkip = 0;
                write(buffer, newOff, newLen);
            } else {
                stillToSkip -= length;
            }
        } else {
            if (backlogSize < finalSkip) {
                if (length > finalSkip - backlogSize) {
                    final int toCopy = finalSkip - backlogSize;
                    System.arraycopy(buffer, offset, backlog, backlogSize, toCopy);
                    backlogSize = finalSkip;
                    write(buffer, offset + toCopy, length - toCopy);
                } else {
                    System.arraycopy(buffer, offset, backlog, backlogSize, length);
                    backlogSize += length;
                }
            } else if (length < finalSkip) {
                out.write(backlog, 0, backlogSize);
                System.arraycopy(backlog, length, backlog, 0, finalSkip - length);
                System.arraycopy(buffer, offset, backlog, finalSkip - length, length);
            } else {
                out.write(backlog);
                out.write(buffer, offset, length - finalSkip);
                System.arraycopy(buffer, offset + length - finalSkip, backlog, 0, finalSkip);
            }
        }
    }
}
