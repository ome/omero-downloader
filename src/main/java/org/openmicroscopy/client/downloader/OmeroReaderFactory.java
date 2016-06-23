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

import loci.ome.io.OmeroReader;

/**
 * Provide {@link OmeroReader} instances for a specific OMERO session.
 * @author m.t.b.carroll@dundee.ac.uk
 */
public class OmeroReaderFactory {

    private final String host;
    private final int port;
    private final String sessionId;

    /**
     * Construct a new factory whose readers use the given OMERO session.
     * @param host the name of the OMERO server hosting the session
     * @param port the port on which to connect to the OMERO server
     * @param sessionId the UUID of the active OMERO session
     */
    public OmeroReaderFactory(String host, int port, String sessionId) {
        this.host = host;
        this.port = port;
        this.sessionId = sessionId;
    }

    /**
     * @return a new reader that is set for the active OMERO session
     */
    public OmeroReader getReader() {
        final OmeroReader reader = new OmeroReader();
        reader.setServer(host);
        reader.setPort(port);
        reader.setSessionID(sessionId);
        return reader;
    }
}
