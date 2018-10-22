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

package org.openmicroscopy.client.downloader.options;

import com.google.common.base.Joiner;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Parse the command-line options for the downloader.
 * @author m.t.b.carroll@dundee.ac.uk
 */
public class OptionParser {

    /**
     * The user's chosen options.
     * @author m.t.b.carroll@dundee.ac.uk
     */
    public static class Chosen {

        private CommandLine chosenOptions;
        private OptionSet.Chosen chosenFileTypes;
        private OptionSet.Chosen chosenObjectTypes;
        private OptionSet.Chosen chosenLinkTypes;

        /**
         * @return the OMERO server host name, may be {@code null}
         */
        public String getHostName() {
            return chosenOptions.getOptionValue('s');
        }

        /**
         * @return the OMERO server port number, may be {@code null}
         */
        public String getHostPort() {
            return chosenOptions.getOptionValue('p');
        }

        /**
         * @return the OMERO user name, may be {@code null}
         */
        public String getUserName() {
            return chosenOptions.getOptionValue('u');
        }

        /**
         * @return the OMERO user's password, may be {@code null}
         */
        public String getPassword() {
            return chosenOptions.getOptionValue('w');
        }

        /**
         * @return the OMERO session to which to attach, may be {@code null}
         */
        public String getSessionKey() {
            return chosenOptions.getOptionValue('k');
        }

        /**
         * @param type a type of file to download or export
         * @return if that file type was selected
         */
        public boolean isFileType(String type) {
            return chosenFileTypes.isOptionSet(type);
        }

        /**
         * @param type a type of OMERO model object to write as XML
         * @return if that object type was selected
         */
        public boolean isObjectType(String type) {
            return chosenObjectTypes.isOptionSet(type);
        }

        /**
         * @param type a type of link to write to the local repository
         * @return if that link type was selected
         */
        public boolean isLinkType(String type) {
            return chosenLinkTypes.isOptionSet(type);
        }

        /**
         * @return if the files of all the images of selected images' filesets should be included in download
         */
        public boolean isAllFileset() {
            return chosenOptions.hasOption('a');
        }

        /**
         * @return the root of the local repository, may be {@code null}
         */
        public String getBaseDirectory() {
            return chosenOptions.getOptionValue('d');
        }

        /**
         * @return if command-line help is requested
         */
        public boolean isHelp() {
            return chosenOptions.hasOption('h');
        }

        /**
         * @return the command-line arguments that are not options
         */
        public List<String> getArguments() {
            return chosenOptions.getArgList();
        }
    }

    private static final OptionSet fileTypes = new OptionSet(Arrays.asList(
            "binary", "companion", "tiff", "ome-tiff", "ome-xml-whole", "ome-xml-parts"),
            Collections.<String>emptySet());
    private static final OptionSet objectTypes = new OptionSet(Arrays.asList(
            "project", "dataset", "folder", "experiment", "instrument", "image", "screen", "plate", "annotation", "roi"),
            Arrays.asList("image", "annotation", "roi"));
    private static final OptionSet linkTypes = new OptionSet(Arrays.asList(
            "fileset", "image"),
            Arrays.asList("fileset", "image"));

    private static final Options options = new Options()
            .addOption("s", "server", true, "OMERO server host name")
            .addOption("p", "port", true, "OMERO server port number")
            .addOption("u", "user", true, "OMERO username")
            .addOption("w", "pass", true, "OMERO password")
            .addOption("k", "key", true, "OMERO session key")
            .addOption("f", "file", true, "file types to download, may be "
                    + Joiner.on(',').join(fileTypes.getOptionNames()) + " or " + OptionSet.NONE + ", default is "
                    + (fileTypes.getDefaultNames().isEmpty() ? OptionSet.NONE
                                                             : Joiner.on(',').join(fileTypes.getDefaultNames())))
            .addOption("x", "xml", true, "object types for which to write XML, may be "
                    + Joiner.on(',').join(objectTypes.getOptionNames()) + " or " + OptionSet.NONE + ", default is "
                    + (objectTypes.getDefaultNames().isEmpty() ? OptionSet.NONE
                                                               : Joiner.on(',').join(objectTypes.getDefaultNames())))
            .addOption("a", "all-fileset", false, "download files for whole fileset")
            .addOption("d", "base", true, "base directory for download")
            .addOption("l", "links", true, "links to create, may be "
                    + Joiner.on(',').join(linkTypes.getOptionNames()) + " or " + OptionSet.NONE + ", default is "
                    + (linkTypes.getDefaultNames().isEmpty() ? OptionSet.NONE
                                                             : Joiner.on(',').join(linkTypes.getDefaultNames())))
            .addOption("h", "help", false, "show usage summary");

    private static final CommandLineParser parser = new DefaultParser();
    private static final HelpFormatter formatter = new HelpFormatter();

    /**
     * Parse the command-line options. Aborts with help message if warranted.
     * @param argv the command-line options
     * @return the parsed options
     */
    public static Chosen parse(String argv[]) {
        final Chosen chosen = new Chosen();
        Integer exitCode = null;
        try {
            chosen.chosenOptions = parser.parse(options, argv);
            if (chosen.isHelp()) {
                exitCode = 1;
            }
        } catch (ParseException pe) {
            exitCode = 2;
        }
        if (exitCode != null) {
            formatter.printHelp("download Target:ID", options);
            System.exit(exitCode);
        }
        final String fileTypeNames = chosen.chosenOptions.getOptionValue('f');
        final String objectTypeNames = chosen.chosenOptions.getOptionValue('x');
        final String linkTypeNames = chosen.chosenOptions.getOptionValue('l');
        chosen.chosenFileTypes = fileTypes.parse(fileTypeNames == null ? "" : fileTypeNames);
        chosen.chosenObjectTypes = objectTypes.parse(objectTypeNames == null ? "" : objectTypeNames);
        chosen.chosenLinkTypes = linkTypes.parse(linkTypeNames == null ? "" : linkTypeNames);
        return chosen;
    }
}
