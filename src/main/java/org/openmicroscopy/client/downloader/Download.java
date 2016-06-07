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

import omero.ServerError;
import omero.api.IQueryPrx;
import omero.gateway.Gateway;
import omero.gateway.LoginCredentials;
import omero.gateway.SecurityContext;
import omero.gateway.exception.DSOutOfServiceException;
import omero.gateway.model.ExperimenterData;
import omero.log.Logger;
import omero.log.SimpleLogger;
import omero.model.IObject;
import omero.model.OriginalFile;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * OMERO client for downloading data in bulk from the server.
 * @author m.t.b.carroll@dundee.ac.uk
 */
public class Download {
    public static void main(String argv[]) {
        final Options options = new Options();
        options.addOption("s", "server", true, "OMERO server host name");
        options.addOption("p", "port", true, "OMERO server port number");
        options.addOption("u", "user", true, "OMERO username");
        options.addOption("w", "pass", true, "OMERO password");
        options.addOption("h", "help", false, "help");

        Integer exitCode = null;
        CommandLine cmd = null;
        try {
            final CommandLineParser parser = new DefaultParser();
            cmd = parser.parse(options, argv);
            if (cmd.hasOption('h')) {
                exitCode = 1;
            }
        } catch (ParseException pe) {
            exitCode = 2;
        }
        if (exitCode != null) {
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("download", options);
            System.exit(exitCode);
        }

        String host = cmd.getOptionValue('s');
        String port = cmd.getOptionValue('p');
        final String user = cmd.getOptionValue('u');
        final String pass = cmd.getOptionValue('w');

        if (host == null) {
            host = "localhost";
        }
        if (port == null) {
            port = "4064";
        }

        final LoginCredentials cred = new LoginCredentials(user, pass, host, Integer.parseInt(port));
        final Logger simpleLogger = new SimpleLogger();

        final Gateway gateway = new Gateway(simpleLogger);
        ExperimenterData exp = null;
        try {
            exp = gateway.connect(cred);
        } catch (DSOutOfServiceException ex) {
            simpleLogger.fatal(ex, "cannot connect to server");
            System.exit(3);
        }

        SecurityContext ctx = new SecurityContext(exp.getGroupId());

        try {
            final IQueryPrx iQuery = gateway.getQueryService(ctx);
            for (final IObject result : iQuery.findAllByQuery("FROM OriginalFile", null)) {
                final OriginalFile file = (OriginalFile) result;
                System.out.println("#" + file.getId().getValue() + " " + file.getPath().getValue() + file.getName().getValue());
            }
        } catch (DSOutOfServiceException | ServerError ex) {
            simpleLogger.fatal(ex, "cannot query server");
            System.exit(3);
        }

        gateway.disconnect();
    }
}
