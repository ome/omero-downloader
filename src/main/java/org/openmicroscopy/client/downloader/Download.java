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

import com.google.common.base.Splitter;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    private static final Logger LOGGER = new SimpleLogger();
    private static final Gateway GATEWAY = new Gateway(LOGGER);
    private static final Pattern TARGET_PATTERN = Pattern.compile("([A-Z][A-Za-z]*):(\\d+(,\\d+)*)");

    private static SecurityContext ctx;

    /**
     * Parse the command-line options.
     * Aborts with help message if warranted.
     * @param argv the command-line options
     * @return the parsed options
     */
    private static CommandLine parseOptions(String argv[]) {
        final Options options = new Options();
        options.addOption("s", "server", true, "OMERO server host name");
        options.addOption("p", "port", true, "OMERO server port number");
        options.addOption("u", "user", true, "OMERO username");
        options.addOption("w", "pass", true, "OMERO password");
        options.addOption("k", "key", true, "OMERO session key");
        options.addOption("h", "help", false, "help");

        Integer exitCode = null;
        CommandLine parsed = null;
        try {
            final CommandLineParser parser = new DefaultParser();
            parsed = parser.parse(options, argv);
            if (parsed.hasOption('h')) {
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

        return parsed;
    }

    /**
     * Open the gateway to the OMERO server and connect to set the security context.
     * @param parsedOptions the command-line options
     */
    private static void openGateway(CommandLine parsedOptions) {
        String host = parsedOptions.getOptionValue('s');
        String port = parsedOptions.getOptionValue('p');
        String user = parsedOptions.getOptionValue('u');
        final String pass = parsedOptions.getOptionValue('w');
        final String key = parsedOptions.getOptionValue('k');

        if (host == null) {
            host = "localhost";
        }
        if (port == null) {
            port = "4064";
        }

        if (key == null) {
            if (user == null || pass == null) {
                System.err.println("must offer username and password or session key");
                System.exit(2);
            }
        } else {
            if (user != null || pass != null) {
                LOGGER.warn(null, "username and password ignored if session key is provided");
            }
            user = key;
        }

        final LoginCredentials credentials = new LoginCredentials(user, pass, host, Integer.parseInt(port));

        try {
            final ExperimenterData experimenter = GATEWAY.connect(credentials);
            ctx = new SecurityContext(experimenter.getGroupId());
        } catch (DSOutOfServiceException oose) {
            LOGGER.fatal(oose, "cannot log in to server");
            System.exit(3);
        }

    }

    /**
     * Do an example query from the server and print the results.
     * @throws DSOutOfServiceException if the query service was not available
     * @throws ServerError if the query could not be executed
     */
    private static void doQuery() throws DSOutOfServiceException, ServerError {
        final IQueryPrx iQuery = GATEWAY.getQueryService(ctx);
        for (final IObject result : iQuery.findAllByQuery("FROM OriginalFile", null)) {
            final OriginalFile file = (OriginalFile) result;
            System.out.println("#" + file.getId().getValue() + " " + file.getPath().getValue() + file.getName().getValue());
        }
    }

    /**
     * Perform the query as instructed.
     * @param argv the command-line options
     */
    public static void main(String argv[]) {
        final CommandLine parsedOptions = parseOptions(argv);
        openGateway(parsedOptions);

        final List<String> targetArgs = parsedOptions.getArgList();
        for (final String target : targetArgs) {
            System.out.println("target: " + target);
            final Matcher matcher = TARGET_PATTERN.matcher(target);
            if (matcher.matches()) {
                final String className = matcher.group(1);
                final Iterable<String> ids = Splitter.on(',').split(matcher.group(2));
                for (final String id : ids) {
                    System.out.println("  " + className + ":" + id);
                }
            }
        }

        try {
            doQuery();
        } catch (DSOutOfServiceException | ServerError e) {
            LOGGER.fatal(e, "cannot query server");
            System.exit(3);
        }

        GATEWAY.disconnect();
    }
}
