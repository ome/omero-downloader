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

package org.openmicroscopy.client.downloader;

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

/**
 * Manage the remote and local locations of files.
 * @author m.t.b.carroll@dundee.ac.uk
 */
public class FileManager {

    private static final Logger LOGGER = new SimpleLogger();

    private final Map<String, RepositoryPrx> repos = new HashMap<>();
    private final Map<RepositoryPrx, Long> repoIds = new HashMap<>();
    private final IQueryPrx iQuery;

    public FileManager(RepositoryMap repositories, IQueryPrx iQuery) {
        final Iterator<OriginalFile> descriptions = repositories.descriptions.iterator();
        final Iterator<RepositoryPrx> proxies = repositories.proxies.iterator();
        while (descriptions.hasNext() && proxies.hasNext()) {
            final OriginalFile description = descriptions.next();
            final RepositoryPrx proxy = proxies.next();
            this.repos.put(description.getHash().getValue(), proxy);
            this.repoIds.put(proxy, description.getId().getValue());
        }
        this.iQuery = iQuery;
    }

    public void checkFile(long fileId) {
        OriginalFile file = null;
        try {
            file = (OriginalFile) iQuery.get("OriginalFile", fileId);
        } catch (ServerError se) {
            LOGGER.fatal(se, "cannot use query service");
            System.exit(3);
        }
        /* repository of file is not mapped in OMERO model */
        RawFileStorePrx rfs = null;
        Long repoId = null;
        for (final RepositoryPrx proxy : repos.values()) {
            try {
                rfs = proxy.fileById(fileId);
                repoId = repoIds.get(proxy);
                break;
            } catch (ServerError se) {
                /* try the next repository */
            }
        }
        if (rfs == null) {
            LOGGER.fatal(file, "failed to obtain handle to file " + fileId);
            System.exit(3);
        }
        try {
            System.out.println("found file " + file.getPath().getValue() + file.getName().getValue() + " size " + rfs.size() +
                    " in repository " + repoId);
            rfs.close();
        } catch (ServerError se) {
            LOGGER.fatal(se, "failed to access file " + fileId);
            System.exit(3);
        }
    }
}
