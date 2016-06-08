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
import omero.RLong;
import omero.RType;

import omero.ServerError;
import omero.api.IQueryPrx;
import omero.cmd.FoundChildren;
import omero.cmd.Response;
import omero.gateway.Gateway;
import omero.gateway.LoginCredentials;
import omero.gateway.SecurityContext;
import omero.gateway.exception.DSOutOfServiceException;
import omero.gateway.model.ExperimenterData;
import omero.gateway.util.Requests;
import omero.log.Logger;
import omero.log.SimpleLogger;
import omero.model.Image;
import omero.model.Roi;
import omero.sys.ParametersI;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.springframework.util.CollectionUtils;

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
     * Perform the query as instructed.
     * @param argv the command-line options
     */
    public static void main(String argv[]) {
        final CommandLine parsedOptions = parseOptions(argv);
        openGateway(parsedOptions);

        final Requests.FindChildrenBuilder finder = Requests.findChildren().childType(Image.class).stopBefore(Roi.class);
        final List<String> targetArgs = parsedOptions.getArgList();
        if (targetArgs.isEmpty()) {
            LOGGER.fatal(null, "no download targets specified");
            System.exit(2);
        }
        for (final String target : targetArgs) {
            final Matcher matcher = TARGET_PATTERN.matcher(target);
            if (matcher.matches()) {
                finder.target(matcher.group(1));
                final Iterable<String> ids = Splitter.on(',').split(matcher.group(2));
                for (final String id : ids) {
                    finder.id(Long.parseLong(id));
                }
            }
        }

        FoundChildren found = null;
        try {
            final Response response = GATEWAY.submit(ctx, finder.build()).loop(250, 250);
            if (response instanceof FoundChildren) {
                found = (FoundChildren) response;
            } else {
                LOGGER.fatal(response, "failed to identify targets for download");
                System.exit(3);
            }
        } catch (Throwable t) {
            LOGGER.fatal(t, "failed to identify targets for download");
            System.exit(3);
        }
        IQueryPrx iQuery = null;
        try {
            iQuery = GATEWAY.getQueryService(ctx);
        } catch (DSOutOfServiceException oose) {
            LOGGER.fatal(oose, "cannot access query service");
            System.exit(3);
        }
        final List<Long> imageIds = found.children.get(ome.model.core.Image.class.getName());
        if (CollectionUtils.isEmpty(imageIds)) {
            LOGGER.fatal(null, "no images found");
            System.exit(3);
        }
        try {
            for (final List<RType> result : iQuery.projection(
                    "SELECT DISTINCT fileset.id FROM Image WHERE fileset IS NOT NULL AND id IN (:ids)",
                    new ParametersI().addIds(imageIds))) {
                final long filesetId = ((RLong) result.get(0)).getValue();
                System.out.println("need fileset #" + filesetId);
            }
        } catch (ServerError se) {
            LOGGER.fatal(se, "cannot use query service");
            System.exit(3);
        }

        GATEWAY.disconnect();
    }
}
