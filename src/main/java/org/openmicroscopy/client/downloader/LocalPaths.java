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

import com.google.common.base.Function;

import java.io.File;
import java.io.IOException;

import ome.services.blitz.repo.path.FilePathRestrictionInstance;
import ome.services.blitz.repo.path.FsFile;
import ome.services.blitz.repo.path.MakePathComponentSafe;

/**
 * Provide the local {@link File} objects.
 * @author m.t.b.carroll@dundee.ac.uk
 */
public class LocalPaths {

    private static final Function<String, String> SANITIZER = new MakePathComponentSafe(
            FilePathRestrictionInstance.getFilePathRestrictions(FilePathRestrictionInstance.LOCAL_REQUIRED));

    private final File base;

    /**
     * Provide local paths in the current directory.
     */
    public LocalPaths() {
        this.base = new File(".");
    }

    /**
     * Provide local paths relative to the given directory.
     * @param base the base directory for download
     * @throws IOException if the directory cannot be used
     */
    public LocalPaths(String base) throws IOException {
        this.base = new File(base);
    }

    /**
     * @param repoId a repository ID
     * @return the name of the corresponding repository directory
     */
    private static String getNameOfRepository(long repoId) {
        return "Repository " + repoId;
    }

    /**
     * @param imageId an image ID
     * @return the name of the corresponding image directory
     */
    private static String getNameOfImage(long imageId) {
        return "Image " + imageId;
    }

    /**
     * @param filesetId a fileset ID
     * @return the name of the corresponding fileset directory
     */
    private static String getNameOfFileset(long filesetId) {
        return "Fileset " + filesetId;
    }

    /**
     * Get the local path for the given file.
     * @param repoId the file's repository ID
     * @param path the file's path in the repository
     * @param name the file's name in the repository
     * @return the file's local path
     */
    public File getFile(long repoId, String path, String name) {
        final File repository = new File(base, getNameOfRepository(repoId));
        return FsFile.concatenate(new FsFile(path), new FsFile(name)).transform(SANITIZER).toFile(repository);
    }

    /**
     * Get the local directory for the given fileset.
     * @param filesetId the fileset ID
     * @return the fileset's local directory
     */
    public File getFileset(long filesetId) {
        return new File(base, getNameOfFileset(filesetId));
    }

    /**
     * Get the local directory for the given image of the given fileset.
     * @param filesetId the fileset ID
     * @param imageId the image ID
     * @return the image's local directory within the fileset
     */
    public File getFilesetImage(long filesetId, long imageId) {
        return new File(base, getNameOfFileset(filesetId) + File.separatorChar + getNameOfImage(imageId));
    }

    /**
     * Get the local directory for the given image.
     * @param imageId the image ID
     * @return the image's local directory
     */
    public File getImage(long imageId) {
        return new File(base, getNameOfImage(imageId));
    }
}
