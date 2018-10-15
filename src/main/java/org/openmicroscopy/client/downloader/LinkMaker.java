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
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Maps;

/**
 * Manage the symbolic links among the local mirror of the server repository and exported model objects.
 * @author m.t.b.carroll@dundee.ac.uk
 */
public class LinkMaker {

    private final LocalPaths localPaths;

    private final Map<Long, File> repositoryFiles = new HashMap<>();
    private final Map<Map.Entry<Map.Entry<ModelType, Long>, Long>, File> modelObjectFiles = new HashMap<>();

    /**
     * Create a new symbolic link maker.
     * @param localPaths the local paths provider
     */
    public LinkMaker(LocalPaths localPaths) {
        this.localPaths = localPaths;
    }

    /**
     * Create a symbolic link, creating parent directories where necessary.
     * @param from where to link from
     * @param to where to link to; parent directories need not yet exist
     * @throws IOException if the link creation fails
     */
    private static void link(File from, File to) throws IOException {
        final Path fromPath = from.toPath();
        final Path fromParent = fromPath.getParent();
        final Path toPath = fromParent.relativize(to.toPath());
        if (!Files.exists(fromPath, LinkOption.NOFOLLOW_LINKS)) {
            Files.createDirectories(fromParent);
            Files.createSymbolicLink(fromPath, toPath);
        }
    }

    /**
     * For future linking note where the given original file is located in the repository mirror on the local filesystem.
     * @param fileId the ID of an original file
     * @param file the local repository location for the original file
     */
    public void noteRepositoryFile(long fileId, File file) {
        repositoryFiles.put(fileId, file);
    }

    /**
     * For future linking note where the given original file is located for a model object on the local filesystem.
     * @param objectType an object type
     * @param objectId an object ID
     * @param fileId the ID of an original file
     * @param file the local model object location for the original file
     */
    public void noteModelObjectFile(ModelType objectType, long objectId, long fileId, File file) {
        final Map.Entry<ModelType, Long> modelObject = Maps.immutableEntry(objectType, objectId);
        modelObjectFiles.put(Maps.immutableEntry(modelObject, fileId), file);
    }

    /**
     * Ensure that symbolic links exist from the model object's files to their location in the repository.
     * Requires prior use of {@link #noteRepositoryFile(long, java.io.File)} and
     * {@link #noteModelObjectFile(org.openmicroscopy.client.downloader.ModelType, long, long, java.io.File)}.
     * @param objectType an object type
     * @param objectId an object ID
     * @param fileIds the IDs of the original files to link
     * @throws IOException if the link creation fails
     */
    public void linkRepositoryFiles(ModelType objectType, long objectId, Iterable<Long> fileIds) throws IOException {
        final Map.Entry<ModelType, Long> modelObject = Maps.immutableEntry(objectType, objectId);
        for (final Long fileId : fileIds) {
            final File repositoryFile = repositoryFiles.get(fileId);
            final File modelObjectFile = modelObjectFiles.get(Maps.immutableEntry(modelObject, fileId));
            link(modelObjectFile, repositoryFile);
        }
    }

    /**
     * Ensure that symbolic links exist from one model object's files to another's.
     * Requires prior use of {@link #noteModelObjectFile(org.openmicroscopy.client.downloader.ModelType, long, long, java.io.File)}.
     * @param fromObjectType the object type to link from
     * @param fromObjectId the object ID to link from
     * @param toObjectType the object type to link to
     * @param toObjectId the object ID to link to
     * @param fileIds the IDs of the original files to link
     * @throws IOException if the link creation fails
     */
    public void linkModelObjectFiles(ModelType fromObjectType, long fromObjectId, ModelType toObjectType, long toObjectId,
            Iterable<Long> fileIds) throws IOException {
        final Map.Entry<ModelType, Long> modelObjectFrom = Maps.immutableEntry(fromObjectType, fromObjectId);
        final Map.Entry<ModelType, Long> modelObjectTo = Maps.immutableEntry(toObjectType, toObjectId);
        for (final Long fileId : fileIds) {
            final File modelObjectFromFile = modelObjectFiles.get(Maps.immutableEntry(modelObjectFrom, fileId));
            final File modelObjectToFile = modelObjectFiles.get(Maps.immutableEntry(modelObjectTo, fileId));
            link(modelObjectFromFile, modelObjectToFile);
        }
    }

    /**
     * Link the directory for a model object from the directory of another that contains it.
     * @param fromObjectType the container object type
     * @param fromObjectId the container object type
     * @param toObjectType the contained object type
     * @param toObjectId the contained object type
     * @throws IOException if the link creation fails
     */
    public void linkModelObjects(ModelType fromObjectType, long fromObjectId, ModelType toObjectType, long toObjectId)
            throws IOException {
        final File modelObjectFrom = localPaths.getModelObjectDirectory(fromObjectType, fromObjectId, toObjectType, toObjectId);
        final File modelObjectTo = localPaths.getModelObjectDirectory(toObjectType, toObjectId);
        link(modelObjectFrom, modelObjectTo);
    }
}
