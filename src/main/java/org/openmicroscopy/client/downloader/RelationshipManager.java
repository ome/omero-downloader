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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Manage the local mirror of the server repository and its symbolic links.
 * @author m.t.b.carroll@dundee.ac.uk
 */
public class RelationshipManager {
    private final SetMultimap<Long, Long> filesOfImage = HashMultimap.create();
    private final SetMultimap<Long, Long> imagesOfFileset = HashMultimap.create();
    private final Map<Long, Long> filesetOfImage = new HashMap<>();
    private final Set<Long> desiredImages = new HashSet<>();

    /**
     * Ensure that {@link #getWantedFiles()} returns the given file if the given image is wanted.
     * @param image an image ID
     * @param file an original file ID
     */
    public void assertImageHasFile(long image, long file) {
        filesOfImage.put(image, file);
    }

    /**
     * Ensure that {@link #getWantedFiles()} returns the given files if the given image is wanted.
     * @param image an image ID
     * @param files some original file IDs
     */
    public void assertImageHasFiles(long image, Collection<Long> files) {
        filesOfImage.putAll(image, files);
    }

    /**
     * Provide information about which filesets contain which images.
     * Determines the return values from {@link #isFsImage(long)} and {@link #isWholeFilesetWanted(long)}.
     * The given image is part of the given fileset.
     * @param fileset a fileset ID
     * @param image an image ID
     */
    public void assertFilesetHasImage(long fileset, long image) {
        imagesOfFileset.put(fileset, image);
        filesetOfImage.put(image, fileset);
    }

    /**
     * Assert that the given image should be included among {@link #getWantedImages()}
     * and its files from {@link #assertImageHasFile(long, long)} should be included
     * among {@link #getWantedFiles()}.
     * @param image an image ID
     */
    public void assertWantImage(long image) {
        desiredImages.add(image);
    }

    /**
     * Assert that the given images should be included among {@link #getWantedImages()}
     * and their files from {@link #assertImageHasFile(long, long)} should be included
     * among {@link #getWantedFiles()}.
     * @param images some image IDs
     */
    public void assertWantImages(Collection<Long> images) {
        desiredImages.addAll(images);
    }

    /**
     * Get the set of wanted images.
     * @return the wanted images, with no duplicates
     */
    public ImmutableSet<Long> getWantedImages() {
        return ImmutableSet.copyOf(desiredImages);
    }

    /**
     * Get the set of wanted files.
     * @return the wanted files, with no duplicates
     */
    public ImmutableSet<Long> getWantedFiles() {
        final ImmutableSet.Builder<Long> wantedFiles = ImmutableSet.builder();
        for (final Long image : desiredImages) {
            wantedFiles.addAll(filesOfImage.get(image));
        }
        return wantedFiles.build();
    }

    /**
     * Determine if an image is in a fileset.
     * @param image an image ID
     * @return if the image is in a fileset
     */
    public boolean isFsImage(long image) {
        return filesetOfImage.containsKey(image);
    }

    /**
     * Determine if all the images of an image's fileset are wanted.
     * @param image an image ID
     * @return if the image's whole fileset is wanted
     */
    public boolean isWholeFilesetWanted(long image) {
        final long fileset = filesetOfImage.get(image);
        return Sets.difference(imagesOfFileset.get(fileset), desiredImages).isEmpty();
    }
}
