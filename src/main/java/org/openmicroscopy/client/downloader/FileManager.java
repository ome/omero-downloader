/*
 * Copyright (C) 2016-2018 University of Dundee & Open Microscopy Environment.
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

package org.openmicroscopy.client.downloader;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import omero.ServerError;
import omero.api.RawFileStorePrx;
import omero.log.Logger;
import omero.log.SimpleLogger;

import com.google.common.math.IntMath;

/**
 * Manage the remote and local locations of files.
 * @author m.t.b.carroll@dundee.ac.uk
 */
public class FileManager {

    private static final Logger LOGGER = new SimpleLogger();
    private static final int BATCH_SIZE = IntMath.checkedMultiply(16, 1048576);  // 16MiB

    /**
     * Download a file from the server.
     * @param fileStore the file store to use if none of the repositories offers the file
     * @param fileId the ID of the file to download
     * @param destination the destination of the download
     */
    public static void download(RawFileStorePrx fileStore, long fileId, File destination) {
        /* download the file */
        try {
            destination.getParentFile().mkdirs();
            long bytesDownloaded = destination.exists() ? destination.length() : 0;
            long bytesRemaining = fileStore.size() - bytesDownloaded;
            if (bytesRemaining == 0) {
                LOGGER.info(null, "already downloaded file " + fileId);
            } else {
                if (bytesDownloaded > 0) {
                    System.out.print("resuming");
                } else {
                    System.out.print("commencing");
                }
                System.out.print(" download of file " + fileId + "...");
                System.out.flush();
                final OutputStream out = new FileOutputStream(destination, true);
                do {
                    final int bytesToRead;
                    if (bytesRemaining > BATCH_SIZE) {
                        bytesToRead = BATCH_SIZE;
                    } else {
                        bytesToRead = (int) bytesRemaining;
                    }
                    out.write(fileStore.read(bytesDownloaded, bytesToRead));
                    bytesDownloaded += bytesToRead;
                    bytesRemaining -= bytesToRead;
                    System.out.print('.');
                    System.out.flush();
                } while (bytesRemaining > 0);
                System.out.println(" done");
                out.close();
            }
        } catch (ServerError se) {
            // TODO: LOGGER.warn(se, "failed to read file " + fileId);
            /* download restriction */
            System.out.println(" failed");
        } catch (IOException ioe) {
            LOGGER.fatal(ioe, "failed to write file " + destination);
            Download.abortOnFatalError(3);
        }
    }
}
