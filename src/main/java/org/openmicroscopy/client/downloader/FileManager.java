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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import omero.ServerError;
import omero.api.IQueryPrx;
import omero.api.RawFileStorePrx;
import omero.grid.RepositoryMap;
import omero.grid.RepositoryPrx;
import omero.log.Logger;
import omero.log.SimpleLogger;
import omero.model.OriginalFile;

import com.google.common.math.IntMath;

/**
 * Manage the remote and local locations of files.
 * @author m.t.b.carroll@dundee.ac.uk
 */
public class FileManager {

    private static final Logger LOGGER = new SimpleLogger();
    private static final int BATCH_SIZE = IntMath.checkedMultiply(16, 1048576);  // 16MiB

    private final Map<String, RepositoryPrx> repos = new HashMap<>();
    private final Map<RepositoryPrx, Long> repoIds = new HashMap<>();
    private final IQueryPrx iQuery;

    /**
     * Construct a new file manager.
     * @param repositories the repositories on the server
     * @param iQuery the query service
     */
    public FileManager(RepositoryMap repositories, IQueryPrx iQuery) {
        final Iterator<OriginalFile> descriptions = repositories.descriptions.iterator();
        final Iterator<RepositoryPrx> proxies = repositories.proxies.iterator();
        while (descriptions.hasNext() && proxies.hasNext()) {
            final OriginalFile description = descriptions.next();
            final RepositoryPrx proxy = proxies.next();
            if (proxy != null) {
                this.repos.put(description.getHash().getValue(), proxy);
                this.repoIds.put(proxy, description.getId().getValue());
            }
        }
        this.iQuery = iQuery;
    }

    /**
     * Download a file from the server.
     * @param fallbackRFS the file store to use if none of the repositories offers the file
     * @param fileId the ID of the file to download
     * @param destination the destination of the download
     */
    public void download(RawFileStorePrx fallbackRFS, long fileId, File destination) {
        /* obtain a handle to the remote file */
        OriginalFile file = null;
        try {
            file = (OriginalFile) iQuery.get("OriginalFile", fileId);
        } catch (ServerError se) {
            LOGGER.fatal(se, "cannot use query service");
            Download.abortOnFatalError(3);
        }
        /* repository of file is not mapped in OMERO model */
        RawFileStorePrx repoRFS = null;
        Long repoId = null;
        for (final RepositoryPrx proxy : repos.values()) {
            try {
                repoRFS = proxy.fileById(fileId);
                repoId = repoIds.get(proxy);
                break;
            } catch (ServerError se) {
                /* try the next repository */
            }
        }
        if (repoRFS == null) {
            /* possibly a repository field value of null */
            try {
                fallbackRFS.setFileId(fileId);
                repoRFS = fallbackRFS;
                repoId = 0L;
            } catch (ServerError se) {
                LOGGER.fatal(se, "failed to obtain handle to file " + fileId);
                Download.abortOnFatalError(3);
            }
        }
        /* download the file */
        try {
            destination.getParentFile().mkdirs();
            long bytesDownloaded = destination.exists() ? destination.length() : 0;
            long bytesRemaining = repoRFS.size() - bytesDownloaded;
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
                    out.write(repoRFS.read(bytesDownloaded, bytesToRead));
                    bytesDownloaded += bytesToRead;
                    bytesRemaining -= bytesToRead;
                    System.out.print('.');
                    System.out.flush();
                } while (bytesRemaining > 0);
                System.out.println(" done");
                out.close();
            }
        } catch (ServerError se) {
            /* download restriction */
            System.out.println(" failed");
        } catch (IOException ioe) {
            LOGGER.fatal(ioe, "failed to write file " + destination);
            Download.abortOnFatalError(3);
        } finally {
            if (repoRFS != fallbackRFS) {
                try {
                    repoRFS.close();
                } catch (ServerError se) {
                    LOGGER.fatal(se, "failed to close remote file store");
                    Download.abortOnFatalError(3);
                }
            }
        }
    }
}
