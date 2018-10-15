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
import java.io.IOException;
import java.util.Map;

import ome.services.blitz.repo.path.FilePathRestrictionInstance;
import ome.services.blitz.repo.path.FsFile;
import ome.services.blitz.repo.path.MakePathComponentSafe;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;

/**
 * Provide the local {@link File} objects.
 * @author m.t.b.carroll@dundee.ac.uk
 */
public class LocalPaths {

    private static final Function<String, String> SANITIZER = new MakePathComponentSafe(
            FilePathRestrictionInstance.getFilePathRestrictions(FilePathRestrictionInstance.LOCAL_REQUIRED));

    private final Map<String, Long> repositoryIds;

    private final File base;

    /**
     * Provide local paths in the current directory.
     * @param repositoryIds the map of repository UUIDs to original file IDs
     * @throws IOException if the current directory cannot be used
     */
    public LocalPaths(Map<String, Long> repositoryIds) throws IOException {
        this(repositoryIds, ".");
    }

    /**
     * Provide local paths relative to the given directory.
     * @param repositoryIds the map of repository UUIDs to original file IDs
     * @param base the base directory for download
     * @throws IOException if the given directory cannot be used
     */
    public LocalPaths(Map<String, Long> repositoryIds, String base) throws IOException {
        this.repositoryIds = ImmutableMap.copyOf(repositoryIds);
        this.base = new File(base);
    }

    /**
     * @return if the base path is a directory
     */
    public boolean isBaseDirectory() {
        return base.isDirectory();
    }

    /**
     * Assemble a {@link File} by sanitizing the given child components then anchoring them to the given parent.
     * @param parentPath a parent path
     * @param remainder child components to sanitize
     * @return the {@link File} for the assembled path
     */
    private File getSanitizedFile(String parentPath, String... remainder) {
        final FsFile[] fsFiles = new FsFile[remainder.length];
        for (int index = 0; index < remainder.length; index++) {
            fsFiles[index] = new FsFile(remainder[index]);
        }
        return FsFile.concatenate(fsFiles).transform(SANITIZER).toFile(new File(base, parentPath));
    }

    /**
     * Get the download directory for the given model object.
     * @param objectType an object type
     * @param objectId an object ID
     * @return the object's download directory 
     */
    private static StringBuilder getModelObjectPrefix(ModelType objectType, long objectId) {
        final StringBuilder localFile = new StringBuilder();
        localFile.append(objectType);
        localFile.append(File.separatorChar);
        localFile.append(objectId);
        localFile.append(File.separatorChar);
        return localFile;
    }

    /**
     * Get the {@link File} for the original file with the given ID and <code>repo == null</code>.
     * @param id an original file ID
     * @return the corresponding {@link File}
     */
    public File getRepositoryFile(long id) {
        final StringBuilder localFile = new StringBuilder();
        localFile.append("Repository");
        localFile.append(File.separatorChar);
        localFile.append("Legacy");
        localFile.append(File.separatorChar);
        localFile.append(id);
        return new File(base, localFile.toString());
    }

    /**
     * Get the {@link File} for the original file with the given ID and <code>repo != null</code>.
     * @param repo the original file's {@code repo} value
     * @param path the original file's {@code path} value
     * @param name the original file's {@code name} value
     * @return the corresponding {@link File}
     */
    public File getRepositoryFile(String repo, String path, String name) {
        final Long repoId = repositoryIds.get(repo);
        if (repoId == null) {
            throw new IllegalStateException("no repository known with hash " + repo);
        }
        final StringBuilder localFile = new StringBuilder();
        localFile.append("Repository");
        localFile.append(File.separatorChar);
        localFile.append(repoId);
        return getSanitizedFile(localFile.toString(), path, name);
    }

    /**
     * Get the {@link File} for the file associated with the given model object with the given path and name.
     * @param objectType an object type
     * @param objectId an object ID
     * @param path the file's path
     * @param name the file's name
     * @param isCompanion if the file is a companion file, containing metadata only
     * @return the corresponding {@link File}
     */
    public File getModelObjectFile(ModelType objectType, long objectId, String path, String name, boolean isCompanion) {
        final StringBuilder localFile = getModelObjectPrefix(objectType, objectId);
        localFile.append(isCompanion ? "Companion" : "Binary");
        return getSanitizedFile(localFile.toString(), path, name);
    }

    /**
     * Get the {@link File} to which to export the given model object.
     * @param objectType an object type
     * @param objectId an object ID
     * @param name the export file's name
     * @return the corresponding {@link File}
     */
    public File getExportFile(ModelType objectType, long objectId, String name) {
        final StringBuilder localFile = getModelObjectPrefix(objectType, objectId);
        localFile.append("Export");
        return getSanitizedFile(localFile.toString(), name);
    }

    /**
     * Get the {@link File} for storing the given model object's OME-XML metadata.
     * @param objectType an object type
     * @param objectId an object ID
     * @return the corresponding {@link File}
     */
    public File getMetadataFile(ModelType objectType, long objectId) {
        final StringBuilder localFile = getModelObjectPrefix(objectType, objectId);
        localFile.append("Metadata");
        localFile.append(File.separatorChar);
        localFile.append(objectType.toString().toLowerCase());
        localFile.append('-');
        localFile.append(objectId);
        localFile.append(".ome.xml");
        return new File(base, localFile.toString());
    }

    /**
     * Get the {@link File} for the directory that holds the files for the given model object.
     * @param objectType an object type
     * @param objectId an object ID
     * @return the corresponding {@link File}
     */
    public File getModelObjectDirectory(ModelType objectType, long objectId) {
        final StringBuilder localFile = getModelObjectPrefix(objectType, objectId);
        return new File(base, localFile.toString());
    }

    /**
     * Get the {@link File} for the directory that holds the files for the given inner model object 
     * as contained by the given outer model object.
     * @param outerObjectType the outer object type
     * @param outerObjectId the outer object ID
     * @param innerObjectType the inner object type
     * @param innerObjectId the inner object ID
     * @return the corresponding {@link File}
     */
    public File getModelObjectDirectory(ModelType outerObjectType, long outerObjectId,
            ModelType innerObjectType, long innerObjectId) {
        final StringBuilder outerFile = getModelObjectPrefix(outerObjectType, outerObjectId);
        final StringBuilder innerFile = getModelObjectPrefix(innerObjectType, innerObjectId);
        return new File(base, outerFile.toString() + innerFile.toString());
    }
}
