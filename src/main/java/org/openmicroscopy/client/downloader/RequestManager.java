/*
 * Copyright (C) 2016-2020 University of Dundee & Open Microscopy Environment.
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

import omero.cmd.CmdCallbackI;
import omero.cmd.ERR;
import omero.cmd.GraphException;
import omero.cmd.Request;
import omero.cmd.Response;
import omero.gateway.Gateway;
import omero.gateway.SecurityContext;

import org.apache.commons.lang3.StringUtils;

import org.openmicroscopy.client.downloader.util.TimeUtil;

/**
 * Manage submission of requests to the OMERO server.
 * @author m.t.b.carroll@dundee.ac.uk
 */
public class RequestManager {
    private final Gateway gateway;
    private final SecurityContext context;
    private final int timeoutSeconds;

    /**
     * Construct a new request manager.
     * @param gateway the OMERO gateway to use
     * @param context the security context to use with the gateway
     * @param timeoutSeconds how long to wait for requests to complete
     */
    public RequestManager(Gateway gateway, SecurityContext context, int timeoutSeconds) {
        this.gateway = gateway;
        this.context = context;
        this.timeoutSeconds = timeoutSeconds;
    }

    /**
     * Perform a request and return the response.
     * If the response is not of the given type then abort with an error message.
     * @param <X> the required response class
     * @param action a description of the requested action
     * @param request the request to be performed
     * @param responseClass the required response class
     * @return the response
     */
    public <X extends Response> X submit(String action, Request request, Class<X> responseClass) {
        System.out.print(action + "...");
        System.out.flush();
        final long startTime = System.nanoTime();
        int thisTimeout = timeoutSeconds;
        CmdCallbackI callback = null;
        try {
            callback = gateway.submit(context, request);
            boolean done = false;
            while (!done && thisTimeout-- > 0) {
                done = callback.block(1000);
                if (!done) {
                    System.out.print('.');
                    System.out.flush();
                }
            }
        } catch (Throwable t) {
            System.out.println(" failed!  " + t);
            Download.abortOnFatalError(3);
        }
        final Response response = callback.getResponse();
        if (!responseClass.isAssignableFrom(ERR.class) && response instanceof ERR) {
            final String message = ((ERR) response).parameters.get("message");
            if (StringUtils.isNotBlank(message)) {
                final BufferedReader reader = new BufferedReader(new StringReader(message));
                try {
                    System.out.println(" failed!  " + reader.readLine());
                } catch (IOException io) {
                    // impossible
                }
                Download.abortOnFatalError(3);
            }
        }
        try {
            final X desiredResponse = responseClass.cast(response);
            TimeUtil.printDone(startTime);
            return desiredResponse;
        } catch (ClassCastException cce) {
            if (response instanceof GraphException) {
                System.out.println(" failed!  " + ((GraphException) response).message);
            } else {
                System.out.println(" failed!  " + response);
            }
            Download.abortOnFatalError(3);
            return null;
        }
    }
}
