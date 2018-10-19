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

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import loci.common.DebugTools;
import loci.common.services.DependencyException;
import loci.common.services.ServiceException;
import loci.common.services.ServiceFactory;
import loci.formats.FormatException;
import loci.formats.ome.OMEXMLMetadata;
import loci.formats.out.OMETiffWriter;
import loci.formats.out.TiffWriter;
import loci.formats.services.OMEXMLService;

import omero.RLong;
import omero.RString;
import omero.RType;
import omero.ServerError;
import omero.api.IConfigPrx;
import omero.api.IQueryPrx;
import omero.api.RawFileStorePrx;
import omero.api.RawPixelsStorePrx;
import omero.cmd.FoundChildren;
import omero.cmd.UsedFilesRequest;
import omero.cmd.UsedFilesResponse;
import omero.cmd.UsedFilesResponsePreFs;
import omero.gateway.Gateway;
import omero.gateway.LoginCredentials;
import omero.gateway.SecurityContext;
import omero.gateway.exception.DSOutOfServiceException;
import omero.gateway.util.Requests;
import omero.grid.SharedResourcesPrx;
import omero.log.Logger;
import omero.log.SimpleLogger;
import omero.model.Annotation;
import omero.model.Dataset;
import omero.model.Experiment;
import omero.model.Folder;
import omero.model.IObject;
import omero.model.Image;
import omero.model.Instrument;
import omero.model.Plate;
import omero.model.Project;
import omero.model.Roi;
import omero.model.Screen;
import omero.sys.ParametersI;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import org.openmicroscopy.client.downloader.options.OptionParser;

/**
 * OMERO client for downloading data in bulk from the server.
 * @author m.t.b.carroll@dundee.ac.uk
 */
public class Download {

    private static final Logger LOGGER = new SimpleLogger();
    private static final Gateway GATEWAY = new Gateway(LOGGER);
    private static final Pattern TARGET_PATTERN = Pattern.compile("([A-Z][A-Za-z]*):(\\d+(,\\d+)*)");

    /**
     * Maps parent-child relationships among model objects: parent to child; types and IDs.
     */
    private static class ParentChildMap implements XmlGenerator.ContainmentListener {

        /**
         * The parent-child relationships.
         */
        final Map<Map.Entry<ModelType, ModelType>, SetMultimap<Long, Long>> containment = new HashMap<>();

        private final Set<ModelType> wantedTypes;

        /**
         * Construct a mapper for parent-child relationships.
         * @param wantedTypes the model object types among which to find relationships
         */
        ParentChildMap(Iterable<ModelType> wantedTypes) {
            this.wantedTypes = ImmutableSet.copyOf(wantedTypes);
        }

        /**
         * The parent model objects from which to find children recursively.
         * @param objects some model objects
         * @return this mapper
         */
        ParentChildMap build(SetMultimap<ModelType, Long> objects) throws ServerError {
            xmlGenerator.queryRelationships(this, objects);
            return this;
        }

        @Override
        public void contains(ModelType containerType, long containerId, ModelType containedType, long containedId) {
            if (wantedTypes.contains(containerType) && wantedTypes.contains(containedType)) {
                final Map.Entry<ModelType, ModelType> types = Maps.immutableEntry(containerType, containedType);
                SetMultimap<Long, Long> ids = containment.get(types);
                if (ids == null) {
                    ids = HashMultimap.create();
                    containment.put(types, ids);
                }
                ids.put(containerId, containedId);
            }
        }

        @Override
        public boolean isWanted(ModelType modelType) {
            return wantedTypes.contains(modelType);
        }
    }

    private static OMEXMLService omeXmlService = null;
    private static SecurityContext ctx = null;
    private static IConfigPrx iConfig = null;
    private static IQueryPrx iQuery = null;
    private static RawFileStorePrx remoteFiles = null;
    private static RawPixelsStorePrx remotePixels = null;
    private static RequestManager requests = null;
    private static FileManager files = null;
    private static LocalPaths paths = null;
    private static LinkMakerPaths links = null;
    private static XmlGenerator xmlGenerator = null;

    /**
     * Open the gateway to the OMERO server and connect to set the security context.
     * @param parsedOptions the command-line options
     */
    private static void openGateway(OptionParser.Chosen parsedOptions) {
        String host = parsedOptions.getHostName();
        String port = parsedOptions.getHostPort();
        String user = parsedOptions.getUserName();
        final String pass = parsedOptions.getPassword();
        final String key = parsedOptions.getSessionKey();

        final int portNumber;
        if (host == null) {
            host = "localhost";
        }
        if (port == null) {
            portNumber = omero.constants.GLACIER2PORT.value;
        } else {
            portNumber = Integer.parseInt(port);
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

        final LoginCredentials credentials = new LoginCredentials(user, pass, host, portNumber);
        String sessionId = null;
        try {
            GATEWAY.connect(credentials);
            sessionId = GATEWAY.getSessionId(GATEWAY.getLoggedInUser());
        } catch (DSOutOfServiceException oose) {
            LOGGER.fatal(oose, "cannot log in to server");
            System.exit(3);
        }
        ctx = new SecurityContext(-1);
    }

    /**
     * Set up various services that may be used by this downloader.
     * Stateful services may be closed down with {@link #closeDownServices()}.
     * @param baseDirectory the root of the repository into which to download
     */
    private static void setUpServices(String baseDirectory) {
        try {
            omeXmlService = new ServiceFactory().getInstance(OMEXMLService.class);
        } catch (DependencyException de) {
            LOGGER.fatal(de, "cannot access OME-XML service");
            System.exit(3);
        }

        SharedResourcesPrx sharedResources = null;
        try {
            iConfig = GATEWAY.getConfigService(ctx);
            iQuery = GATEWAY.getQueryService(ctx);
            remoteFiles = GATEWAY.getRawFileService(ctx);
            remotePixels = GATEWAY.getPixelsStore(ctx);
            sharedResources = GATEWAY.getSharedResources(ctx);
        } catch (DSOutOfServiceException oose) {
            LOGGER.fatal(oose, "cannot access OMERO services");
            System.exit(3);
        }

        final Map<String, Long> repositoryIds = getRepositoryIds();
        try {
            paths = baseDirectory == null ? new LocalPaths(repositoryIds) : new LocalPaths(repositoryIds, baseDirectory);
        } catch (IOException ioe) {
            LOGGER.fatal(ioe, "cannot access base download directory");
            System.exit(3);
        }
        if (!paths.isBaseDirectory()) {
            LOGGER.fatal(null, "base download directory must already exist");
            System.exit(3);
        }

        try {
            files = new FileManager(sharedResources.repositories(), iQuery);
            xmlGenerator = new XmlGenerator(omeXmlService, iConfig, iQuery);
        } catch (ServerError se) {
            LOGGER.fatal(se, "failed to use services");
            System.exit(3);
        }

        links = new LinkMakerPaths(paths);
        requests = new RequestManager(GATEWAY, ctx, 250);
    }

    /**
     * Close down the stateful services that were set up by {@link #setUpServices(java.lang.String)}.
     */
    private static void closeDownServices() {
        try {
            remoteFiles.close();
            remotePixels.close();
        } catch (ServerError se) {
            LOGGER.fatal(se, "failed to close OMERO services");
            System.exit(3);
        }
    }

    /**
     * @return the map of repository UUIDs to original file IDs
     */
    private static Map<String, Long> getRepositoryIds() {
        List<List<RType>> results = null;
        try {
            results = iQuery.projection(
                    "SELECT id, hash FROM OriginalFile WHERE hash IS NOT NULL AND mimetype = :repo",
                    new ParametersI().add("repo", omero.rtypes.rstring("Repository")));
            if (CollectionUtils.isEmpty(results)) {
                LOGGER.error(null, "cannot retrieve repositories");
                return Collections.emptyMap();
            }
        } catch (ServerError se) {
            LOGGER.fatal(se, "cannot use query service");
            System.exit(3);
        }
        final ImmutableMap.Builder<String, Long> repositoryIds = ImmutableMap.builder();
        for (final List<RType> result : results) {
            final long id = ((RLong) result.get(0)).getValue();
            final String hash = ((RString) result.get(1)).getValue();
            repositoryIds.put(hash, id);
        }
        return repositoryIds.build();
    }

    /**
     * Determine the model classes selected for writing as XML.
     * @param parsedOptions the selected command-line options
     * @return the selected model classes, never {@code null}
     */
    private static Set<Class<? extends IObject>> getModelClasses(OptionParser.Chosen parsedOptions) {
        final Set<Class<? extends IObject>> modelClasses = new HashSet<>();
        modelClasses.add(Project.class);
        modelClasses.add(Dataset.class);
        modelClasses.add(Folder.class);
        modelClasses.add(Experiment.class);
        modelClasses.add(Instrument.class);
        modelClasses.add(Image.class);
        modelClasses.add(Screen.class);
        modelClasses.add(Plate.class);
        modelClasses.add(Annotation.class);
        modelClasses.add(Roi.class);
        final Iterator<Class<? extends IObject>> modelClassIterator = modelClasses.iterator();
        while (modelClassIterator.hasNext()) {
            final Class<? extends IObject> modelClass = modelClassIterator.next();
            if (!parsedOptions.isObjectType(modelClass.getSimpleName().toLowerCase())) {
                modelClassIterator.remove();
            }
        }
        return modelClasses;
    }

    /**
     * Download the originally uploaded image files.
     * @param fileMapper the file mapper
     * @param imageIds the IDs of the images whose files should be downloaded
     * @param isBinary if to include the binary files
     * @param isCompanion if to include the companion files
     */
    private static void downloadFiles(FileMapper fileMapper, Set<Long> imageIds, boolean isBinary, boolean isCompanion,
            boolean isLinkFilesets, boolean isLinkImages) {
        /* map the files of the targeted images */
        final int totalImageCount = imageIds.size();
        int currentImageCount = 1;
        for (final long imageId : Ordering.natural().immutableSortedCopy(imageIds)) {
            System.out.print("(" + currentImageCount + "/" + totalImageCount + ") ");

            /* determine the used files */
            final Set<Long> binaryFiles = new HashSet<>();
            final Set<Long> companionFiles = new HashSet<>();
            final Long filesetId = fileMapper.getFilesetId(imageId);
            if (filesetId != null) {
                final UsedFilesResponse usedFiles = requests.submit("determining files used by image " + imageId,
                        new UsedFilesRequest(imageId), UsedFilesResponse.class);
                if (isBinary) {
                    binaryFiles.addAll(usedFiles.binaryFilesThisSeries);
                }
                if (isCompanion) {
                    companionFiles.addAll(usedFiles.companionFilesThisSeries);
                }
            } else {
                final UsedFilesResponsePreFs usedFiles = requests.submit("determining files used by image " + imageId,
                        new UsedFilesRequest(imageId), UsedFilesResponsePreFs.class);
                if (isBinary) {
                    binaryFiles.addAll(usedFiles.archivedFiles);
                    binaryFiles.removeAll(usedFiles.companionFiles);
                }
                if (isCompanion) {
                    companionFiles.addAll(usedFiles.companionFiles);
                }
            }

            /* download the files */
            final Set<Long> wantedFileIds = Sets.union(binaryFiles, companionFiles);
            final Set<Long> failedFileIds = new HashSet<>();
            final int totalFileCount = wantedFileIds.size();
            int currentFileCount = 1;
            for (final long fileId : Ordering.natural().immutableSortedCopy(wantedFileIds)) {
                System.out.print("(" + currentImageCount + "/" + totalImageCount + ", " +
                        currentFileCount++ + "/" + totalFileCount + ") ");
                final File file = fileMapper.getRepositoryFile(fileId);
                files.download(remoteFiles, fileId, file);
                if (file.isFile() && file.length() > 0) {
                    links.noteRepositoryFile(fileId, file);
                    final File imageFile = fileMapper.getImageFile(imageId, fileId, isCompanion);
                    links.noteModelObjectFile(ModelType.IMAGE, imageId, fileId, imageFile);
                    if (filesetId != null) {
                        final File filesetFile = fileMapper.getFilesetFile(fileId, isCompanion);
                        links.noteModelObjectFile(ModelType.FILESET, filesetId, fileId, filesetFile);
                    }
                } else {
                    failedFileIds.add(fileId);
                }
            }

            /* create symbolic links to the downloaded files */
            final Set<Long> downloadedFileIds = Sets.difference(wantedFileIds, failedFileIds);
            try {
                if (filesetId != null) {
                    if (isLinkFilesets) {
                        links.linkRepositoryFiles(ModelType.FILESET, filesetId, downloadedFileIds);
                        links.linkModelObjectFiles(ModelType.IMAGE, imageId, ModelType.FILESET, filesetId, downloadedFileIds);
                        links.linkModelObjects(ModelType.FILESET, filesetId, ModelType.IMAGE, imageId);
                    }
                } else {
                    if (isLinkImages) {
                        links.linkRepositoryFiles(ModelType.IMAGE, imageId, downloadedFileIds);
                    }
                }
            } catch (IOException ioe) {
                LOGGER.fatal(ioe, "cannot create repository links");
                System.exit(3);
            }
            currentImageCount++;
        }
    }

    /**
     * Write image data via Bio-Formats writers.
     * @param fileMapper the file mapper
     * @param containment the parent-child relationships
     * @param imageIds the IDs of the images that should be exported
     * @param isTiff if to write TIFF files
     * @param isOmeTiff if to write OME-TIFF files
     */
    private static void exportImages(FileMapper fileMapper,
            Map<Map.Entry<ModelType, ModelType>, SetMultimap<Long, Long>> containment,
            Collection<Long> imageIds, boolean isTiff, boolean isOmeTiff) {
        final int totalCount = imageIds.size();
        int currentCount = 1;
        for (final long imageId : imageIds) {
            final String countPrefix = "(" + currentCount++ + "/" + totalCount + ") ";

            /* obtain the metadata for the image */
            OMEXMLMetadata metadata = null;
            try {
                metadata = omeXmlService.createOMEXMLMetadata();
                metadata.createRoot();
                final SetMultimap<Long, Long> roiMap = containment.get(Maps.immutableEntry(ModelType.IMAGE, ModelType.ROI));
                final Set<Long> roiIds = roiMap == null ? Collections.<Long>emptySet() : ImmutableSet.copyOf(roiMap.get(imageId));
                xmlGenerator.writeImages(Collections.singletonList(imageId), metadata);
                xmlGenerator.writeRois(ImmutableList.copyOf(roiIds), metadata);
                final Map<ModelType, Map<Long, Integer>> indices = xmlGenerator.getMetadataIndicesForModelObjects(metadata);
                final Map<Long, Integer> imageIndices = indices.get(ModelType.IMAGE);
                final Map<Long, Integer> roiIndices = indices.get(ModelType.ROI);
                final int imageIndex = imageIndices.get(imageId);
                for (final long roiId : roiIds) {
                    final int roiIndex = roiIndices.get(roiId);
                    metadata.setImageROIRef(metadata.getROIID(roiIndex), imageIndex, roiIndex);
                }
                /* TODO: Workaround for a curious legacy issue that may yet be fixed. */
                metadata.setPixelsBigEndian(true, 0);
            } catch (ServerError se) {
                LOGGER.fatal(se, "failed to fetch images from server");
                System.exit(3);
            } catch (ServiceException se) {
                LOGGER.fatal(se, "failed to create OME-XML metadata");
                System.exit(3);
            }

            /* do the download and assembly */
            try {
                /* choose filenames and writers */
                final File tileFile = paths.getExportFile(ModelType.IMAGE, imageId, "downloaded-tiles.bin");
                final String imageName = metadata.getImageName(0);
                final String filename = StringUtils.isBlank(imageName) ? "image" : imageName;
                final Map<File, TiffWriter> tiffFiles = new HashMap<>();
                if (isTiff) {
                    final TiffWriter writer = new TiffWriter();
                    final String writeName = writer.isThisType(filename) ? filename : filename + ".tiff";
                    tiffFiles.put(paths.getExportFile(ModelType.IMAGE, imageId, writeName), writer);
                }
                if (isOmeTiff) {
                    final TiffWriter writer = new OMETiffWriter();
                    final String writeName = writer.isThisType(filename) ? filename : filename + ".ome.tiff";
                    tiffFiles.put(paths.getExportFile(ModelType.IMAGE, imageId, writeName), writer);
                }
                final Iterator<File> tiffFileIterator = tiffFiles.keySet().iterator();
                while (tiffFileIterator.hasNext()) {
                    final File tiffFile = tiffFileIterator.next();
                    if (tiffFile.exists()) {
                        if (tileFile.exists()) {
                            /* may be a partial assembly so restart */
                            tiffFile.delete();
                        } else {
                            /* already assembled */
                            tiffFileIterator.remove();
                        }
                    }
                }
                System.out.print(countPrefix);
                if (tiffFiles.isEmpty()) {
                    System.out.println("already assembled image " + imageId);
                    continue;
                }

                /* actually download and assemble */
                final Long pixelsId = fileMapper.getPixelsId(imageId);
                if (pixelsId == null) {
                    LOGGER.warn(null, "failed to find pixel data for image " + imageId);
                    continue;
                }
                tileFile.getParentFile().mkdirs();
                final LocalPixels localPixels = new LocalPixels(pixelsId, metadata, tileFile, remotePixels);
                localPixels.downloadTiles();
                for (final Map.Entry<File, TiffWriter> tiffFileAndWriter : tiffFiles.entrySet()) {
                    System.out.print(countPrefix);
                    final File tiffFile = tiffFileAndWriter.getKey();
                    final TiffWriter writer = tiffFileAndWriter.getValue();
                    writer.setCompression(TiffWriter.COMPRESSION_J2K);
                    writer.setMetadataRetrieve(metadata);
                    writer.setId(tiffFile.getPath());
                    localPixels.writeTiles(writer);
                }
                tileFile.delete();
            } catch (FormatException | IOException | ServerError e) {
                LOGGER.error(e, "cannot assemble downloaded image");
            }
        }
    }

    /**
     * Write the given model objects as XML.
     * @param containment the parent-child relationships
     * @param objects the model objects to write
     */
    private static void writeXmlObjects(Map<Map.Entry<ModelType, ModelType>, SetMultimap<Long, Long>> containment,
            final SetMultimap<ModelType, Long> objects) {
        if (objects.containsKey(ModelType.IMAGE)) {
            xmlGenerator.writeImages(ImmutableList.copyOf(objects.get(ModelType.IMAGE)), new Function<Long, File>() {
                @Override
                public File apply(Long id) {
                    final File file = paths.getMetadataFile(ModelType.IMAGE, id);
                    file.getParentFile().mkdirs();
                    return file;
                }
            });
        }
        if (objects.containsKey(ModelType.ROI)) {
            xmlGenerator.writeRois(ImmutableList.copyOf(objects.get(ModelType.ROI)), new Function<Long, File>() {
                @Override
                public File apply(Long id) {
                    final File file = paths.getMetadataFile(ModelType.ROI, id);
                    file.getParentFile().mkdirs();
                    return file;
                }
            });
        }
        try {
            for (final Map.Entry<Map.Entry<ModelType, ModelType>,
                    SetMultimap<Long, Long>> containmentEntry : containment.entrySet()) {
                final ModelType containerType = containmentEntry.getKey().getKey();
                final ModelType containedType = containmentEntry.getKey().getValue();
                for (final Map.Entry<Long, Long> containmentIds : containmentEntry.getValue().entries()) {
                    final long containerId = containmentIds.getKey();
                    final long containedId = containmentIds.getValue();
                    links.linkModelObjects(containerType, containerId, containedType, containedId);
                }
            }
        } catch (IOException ioe) {
            LOGGER.fatal(ioe, "cannot create repository links");
            System.exit(3);
        }
    }

    /**
     * Perform the download as instructed.
     * @param argv the command-line options
     */
    public static void main(String argv[]) {
        DebugTools.enableLogging("ERROR");

        /* parse and validate the command-line options and connect to the OMERO server */
        final OptionParser.Chosen parsedOptions = OptionParser.parse(argv);
        openGateway(parsedOptions);
        setUpServices(parsedOptions.getBaseDirectory());

        /* determine which objects are targeted */
        Requests.FindChildrenBuilder finder = Requests.findChildren().childType(Image.class);
        for (final Class<? extends IObject> classToFind : getModelClasses(parsedOptions)) {
            finder.childType(classToFind);
        }
        final List<String> targetArgs = parsedOptions.getArguments();
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
            } else {
                System.err.println("cannot parse Target:ids argument: " + target);
                System.exit(2);
            }
        }

        /* find the images and other objects for those targets */
        FoundChildren found = requests.submit("finding target images", finder.build(), FoundChildren.class);
        Set<Long> imageIds = ImmutableSet.copyOf(found.children.get(ome.model.core.Image.class.getName()));

        /* process filesets */
        FileMapper fileMapper = null;
        if (CollectionUtils.isNotEmpty(imageIds)) {
            fileMapper = new FileMapper(iQuery, paths, imageIds);
            if (parsedOptions.isAllFileset()) {
                final Set<Long> newImageIds = fileMapper.completeFilesets(imageIds);
                if (!newImageIds.equals(imageIds)) {
                    finder.target(Image.class).id(newImageIds);
                    found = requests.submit("finding target images", finder.build(), FoundChildren.class);
                    imageIds = ImmutableSet.copyOf(found.children.get(ome.model.core.Image.class.getName()));
                }
            }
        }

        /* organize all the targeted objects by model type */
        final SetMultimap<ModelType, Long> toWrite = HashMultimap.create();
        for (final Map.Entry<String, List<Long>> childrenOneType : found.children.entrySet()) {
            final String typeName = childrenOneType.getKey();
            final List<Long> ids = childrenOneType.getValue();
            final String blitzName = "omero.model." + typeName.substring(typeName.lastIndexOf('.') + 1);
            Class<? extends IObject> modelObjectClass = null;
            try {
                modelObjectClass = Class.forName(blitzName).asSubclass(IObject.class);
            } catch (ClassNotFoundException cnfe) {
                LOGGER.fatal(cnfe, "failed to process model object class: " + typeName);
                System.exit(3);
            }
            try {
                final ModelType modelType = ModelType.getEnumValueFor(modelObjectClass);
                toWrite.putAll(modelType, ids);
            } catch (IllegalArgumentException iae) {
                LOGGER.fatal(iae, "failed to process model object class: " + modelObjectClass);
                System.exit(3);
            }
        }

        /* map parent-child relationships */
        Map<Map.Entry<ModelType, ModelType>, SetMultimap<Long, Long>> containment = null;
        try {
            containment = new ParentChildMap(toWrite.keySet()).build(toWrite).containment;
        } catch (ServerError se) {
            LOGGER.fatal(se, "cannot use query service");
            System.exit(3);
        }

        /* write the requested files */
        if (CollectionUtils.isNotEmpty(imageIds)) {
            if (parsedOptions.isFileType("binary") || parsedOptions.isFileType("companion")) {
                /* download the image files from the remote repository */
                downloadFiles(fileMapper, imageIds, parsedOptions.isFileType("binary"), parsedOptions.isFileType("companion"),
                        parsedOptions.isLinkType("fileset"), parsedOptions.isLinkType("image"));
            }

            if (parsedOptions.isFileType("tiff") || parsedOptions.isFileType("ome-tiff")) {
                /* export the images via Bio-Formats */
                exportImages(fileMapper, containment, imageIds,
                        parsedOptions.isFileType("tiff"), parsedOptions.isFileType("ome-tiff"));
            }
        }

        /* write the requested metadata */
        if (!parsedOptions.isObjectType("image")) {
            toWrite.removeAll(ModelType.IMAGE);
        }
        if (!toWrite.isEmpty()) {
            if (parsedOptions.isFileType("ome-xml-whole")) {
                /* write model objects into one XML file with Ref elements */
                LOGGER.error(null, "OME-XML cross-referenced file export is not yet implemented");
            } else if (parsedOptions.isFileType("ome-xml-parts")) {
                /* write model objects split into separate XML files using symbolic links to show cross-references */
                writeXmlObjects(containment, toWrite);
            }
        }

        /* all done with the server */
        closeDownServices();
        GATEWAY.disconnect();
    }
}
