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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import ome.services.blitz.repo.path.FsFile;

import omero.RLong;
import omero.RString;
import omero.RType;
import omero.ServerError;
import omero.api.IQueryPrx;
import omero.log.Logger;
import omero.log.SimpleLogger;
import omero.sys.ParametersI;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

/**
 * Track the relationships among filesets, images, pixels and original files.
 * @author m.t.b.carroll@dundee.ac.uk
 */
public class FileMapper {

    private static final Logger LOGGER = new SimpleLogger();


    /* the batch size for querying model object properties from OMERO */
    private static final int BATCH_SIZE = 512;

    /**
     * The relevant details of an original file.
     */
    private class OriginalFile {
        final long id;
        final String name;
        final File repositoryFile;

        /**
         * Construct a non-FS original file.
         * @param id the original file ID
         * @param name the name of the file
         */
        OriginalFile(long id, String name) {
            this.id = id;
            this.name = name;
            this.repositoryFile = paths.getRepositoryFile(id);
        }

        /**
         * Construct an original file with the given local location.
         * @param id the original file ID
         * @param name the name of the file
         * @param repositoryFile the local location of the file
         */
        private OriginalFile(long id, String name, File repositoryFile) {
            this.id = id;
            this.name = name;
            this.repositoryFile = repositoryFile;
        }
    }

    /**
     * The relevant details of an original file that is in a managed repository.
     */
    private class OriginalFileFs extends OriginalFile {
        final String repository;
        final String fullPath;
        String shortPath;

        /**
         * Construct an FS original file.
         * Subsequently {@link #shortenPaths(java.lang.Iterable)} can adjust {@link #shortPath}.
         * @param id the original file ID
         * @param repository the repository UUID of the file
         * @param path the path of the file
         * @param name the name of the file
         */
        OriginalFileFs(long id, String repository, String path, String name) {
            super(id, name, paths.getRepositoryFile(repository, path, name));
            this.repository = repository;
            this.fullPath = path;
            this.shortPath = path;
        }
    }

    /**
     * Remove any common prefix from the paths of the given FS files.
     * @param files some FS files
     */
    private static void shortenPaths(Iterable<OriginalFileFs> files) {
        while (true) {
            final Set<String> nextComponent = new HashSet<>();
            for (final OriginalFileFs file : files) {
                final int index = file.shortPath.indexOf(FsFile.separatorChar, 1);
                if (index > 0) {
                    nextComponent.add(file.shortPath.substring(0, index));
                } else {
                    return;
                }
            }
            if (nextComponent.size() == 1) {
                /* the current first component is common so trim it */
                final int length = nextComponent.iterator().next().length() + 1;
                for (final OriginalFileFs file : files) {
                    file.shortPath = file.shortPath.substring(length);
                }
            } else {
                return;
            }
        }
    }

    private final IQueryPrx iQuery;
    private final LocalPaths paths;

    /* indices */
    private final Map<Long, OriginalFile> files = new HashMap<>();
    private final SetMultimap<Long, Long> filesOfFilesets = HashMultimap.create();
    private final SetMultimap<Long, Long> imagesOfFilesets = HashMultimap.create();
    private final Map<Long, Long> filesetOfFiles = new HashMap<>();
    private final Map<Long, Long> filesetOfImages = new HashMap<>();
    private final Map<Long, Long> pixelsOfImages = new HashMap<>();

    /**
     * Map the filesets, pixels and original files of the given images.
     * @param iQuery the query service
     * @param paths the local paths provider
     * @param imageIds the IDs of the images to map
     */
    public FileMapper(IQueryPrx iQuery, LocalPaths paths, Collection<Long> imageIds) {
        this.iQuery = iQuery;
        this.paths = paths;

        /* map the filesets of the targeted images */
        System.out.print("mapping filesets of images...");
        System.out.flush();
        try {
            for (final List<Long> imageIdBatch : Lists.partition(ImmutableList.copyOf(imageIds), BATCH_SIZE)) {
                final Set<Long> filesetsThisBatch = new HashSet<>();
                for (final List<RType> result : iQuery.projection(
                        "SELECT fileset.id, id FROM Image WHERE fileset IS NOT NULL AND id IN (:ids)",
                        new ParametersI().addIds(imageIdBatch), Download.ALL_GROUPS_CONTEXT)) {
                    final long filesetId = ((RLong) result.get(0)).getValue();
                    final long imageId = ((RLong) result.get(1)).getValue();
                    imagesOfFilesets.put(filesetId, imageId);
                    filesetOfImages.put(imageId, filesetId);
                    filesetsThisBatch.add(filesetId);
                }
                if (!filesetsThisBatch.isEmpty()) {
                    for (final List<RType> result : iQuery.projection(
                            "SELECT fileset.id, originalFile.id, originalFile.repo, originalFile.path, originalFile.name " +
                            "FROM FilesetEntry WHERE fileset.id IN (:ids)",
                            new ParametersI().addIds(filesetsThisBatch), Download.ALL_GROUPS_CONTEXT)) {
                        final long filesetId = ((RLong) result.get(0)).getValue();
                        final long fileId = ((RLong) result.get(1)).getValue();
                        final String repository = ((RString) result.get(2)).getValue();
                        final String path = ((RString) result.get(3)).getValue();
                        final String name = ((RString) result.get(4)).getValue();
                        final OriginalFile file;
                        if (repository == null) {
                            file = new OriginalFile(fileId, name);
                        } else {
                            file = new OriginalFileFs(fileId, repository, path, name);
                        }
                        filesOfFilesets.put(filesetId, fileId);
                        filesetOfFiles.put(fileId, filesetId);
                        files.put(fileId, file);
                    }
                }
                queryPixels(imageIdBatch);
            }
        } catch (ServerError se) {
            LOGGER.fatal(se, "cannot use query service");
            Download.abortOnFatalError(3);
        }
        FILESET:
        for (final Collection<Long> fileIds : filesOfFilesets.asMap().values()) {
            final List<OriginalFileFs> fsFiles = new ArrayList<>(fileIds.size());
            for (final Long fileId : fileIds) {
                final OriginalFile file = files.get(fileId);
                if (file instanceof OriginalFileFs) {
                    fsFiles.add((OriginalFileFs) file);
                } else {
                    break FILESET;
                }
            }
            shortenPaths(fsFiles);
        }
        System.out.println(" done");
    }

    /**
     * Map the pixels of the given images.
     * @param imageIds the IDs of the images to map
     * @throws ServerError if a query failed
     */
    private void queryPixels(Collection<Long> imageIds) throws ServerError {
        final Set<Long> pixelsIds = new HashSet<>();
        for (final List<RType> result : iQuery.projection(
                "SELECT image.id, id FROM Pixels WHERE image.id IN (:ids))",
                new ParametersI().addIds(imageIds), Download.ALL_GROUPS_CONTEXT)) {
            final long imageId = ((RLong) result.get(0)).getValue();
            final long pixelsId = ((RLong) result.get(1)).getValue();
            pixelsOfImages.put(imageId, pixelsId);
            pixelsIds.add(pixelsId);
        }
        if (pixelsIds.isEmpty()) {
            return;
        }
        for (final List<RType> result : iQuery.projection(
                "SELECT parent.id, parent.name FROM PixelsOriginalFileMap WHERE child.id IN (:ids))",
                new ParametersI().addIds(pixelsIds), Download.ALL_GROUPS_CONTEXT)) {
            final long fileId = ((RLong) result.get(0)).getValue();
            final String name = ((RString) result.get(1)).getValue();
            final OriginalFile file = new OriginalFile(fileId, name);
            files.put(fileId, file);
        }
    }

    /**
     * Provide the IDs of all the images of all the filesets that have an image whose ID among those given.
     * @param imageIds some image IDs
     * @return a superset of the image IDs, completing any partial filesets
     */
    public Set<Long> completeFilesets(Set<Long> imageIds) {
        final Set<Long> filesetIds = new HashSet<>();
        for (final long imageId : imageIds) {
            final Long filesetId = filesetOfImages.get(imageId);
            if (filesetId != null) {
                filesetIds.add(filesetId);
            }
        }
        final ImmutableSet.Builder<Long> builder = ImmutableSet.builder();
        builder.addAll(imageIds);
        for (final long filesetId : filesetIds) {
            builder.addAll(imagesOfFilesets.get(filesetId));
        }
        final Set<Long> newImageIds = builder.build();
        if (!newImageIds.equals(imageIds)) {
            final List<Long> toQuery = ImmutableList.copyOf(Sets.difference(newImageIds, imageIds));
            try {
                for (final List<Long> imageIdBatch : Lists.partition(ImmutableList.copyOf(toQuery), BATCH_SIZE)) {
                    queryPixels(imageIdBatch);
                }
            } catch (ServerError se) {
                LOGGER.fatal(se, "cannot use query service");
                Download.abortOnFatalError(3);
            }
        }
        return newImageIds;
    }

    /**
     * Get the local location of the given file associated with its fileset.
     * @param fileId an original file ID
     * @param isCompanion if the file is a companion file, containing metadata only
     * @return the file's location, or {@code null} if it is not in a fileset
     */
    public File getFilesetFile(long fileId, boolean isCompanion) {
        final OriginalFile file = files.get(fileId);
        if (file instanceof OriginalFileFs) {
           final OriginalFileFs fsFile =  (OriginalFileFs) file;
           final long filesetId = filesetOfFiles.get(fileId);
           return paths.getModelObjectFile(ModelType.FILESET, filesetId, fsFile.shortPath, fsFile.name, isCompanion);
        } else {
            return null;
        }
    }

    /**
     * Get the local location of the given file associated with the given image.
     * @param imageId an image ID
     * @param fileId an original file ID
     * @param isCompanion if the file is a companion file, containing metadata only
     * @return the file's location
     */
    public File getImageFile(long imageId, long fileId, boolean isCompanion) {
        final OriginalFile file = files.get(fileId);
        if (file instanceof OriginalFileFs) {
            final OriginalFileFs fsFile = (OriginalFileFs) file;
            return paths.getModelObjectFile(ModelType.IMAGE, imageId, fsFile.shortPath, fsFile.name, isCompanion);
        } else {
            return paths.getModelObjectFile(ModelType.IMAGE, imageId, "", file.name, isCompanion);
        }
    }

    /**
     * @param imageId an image ID
     * @return the ID of the image's fileset, or {@code null} if it is not in a fileset
     */
    public Long getFilesetId(long imageId) {
        return filesetOfImages.get(imageId);
    }

    /**
     * @param imageId an image ID
     * @return the ID of the image's pixel data, or {@code null} if it has no associated pixels
     */
    public Long getPixelsId(long imageId) {
        return pixelsOfImages.get(imageId);
    }

    /**
     * @param fileId an original file ID
     * @return the location of the file in the repository
     */
    public File getRepositoryFile(long fileId) {
        return files.get(fileId).repositoryFile;
    }
}
