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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Manage the local mirror of the server repository and its symbolic links.
 * @author m.t.b.carroll@dundee.ac.uk
 */
public class RelationshipManager {

    private final LocalPaths localPaths;

    private final SetMultimap<Long, Long> imagesOfFiles = HashMultimap.create();
    private final Map<Long, Long> filesetOfImage = new HashMap<>();
    private final SetMultimap<Long, Long> imagesOfFileset = HashMultimap.create();
    private final SetMultimap<Long, Long> filesOfImages = HashMultimap.create();
    private final Set<Long> desiredImages = new HashSet<>();

    private final Map<Long, Path> filePaths = new HashMap<>();

    /**
     * Create a new repository relationship manager.
     * @param localPaths the local paths provider
     */
    public RelationshipManager(LocalPaths localPaths) {
        this.localPaths = localPaths;
    }

    /**
     * Ensure that {@link #getWantedFiles()} returns the given file if the given image is wanted.
     * @param image an image ID
     * @param file an original file ID
     */
    public void assertImageHasFile(long image, long file) {
        filesOfImages.put(image, file);
        imagesOfFiles.put(file, image);
    }

    /**
     * Ensure that {@link #getWantedFiles()} returns the given files if the given image is wanted.
     * @param image an image ID
     * @param files some original file IDs
     */
    public void assertImageHasFiles(long image, Collection<Long> files) {
        filesOfImages.putAll(image, files);
        for (final Long file : files) {
            imagesOfFiles.put(file, image);
        }
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
     * Provide information about where downloaded files are located locally.
     * @param file a file ID
     * @param path a local path on the filesystem
     */
    public void assertFileHasPath(long file, Path path) {
        filePaths.put(file, path);
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
            wantedFiles.addAll(filesOfImages.get(image));
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
     * Remove a common prefix from a set of paths.
     * If given a single path, returns its last component.
     * @param paths some paths
     * @return the differing suffixes of the paths, in the same order
     */
    private static List<Path> shortenPaths(Iterable<Path> paths) {
        final List<Iterator<Path>> iterators = new ArrayList<>();
        for (final Path path : paths) {
            iterators.add(path.iterator());
        }
        int skipCount = 0;
        while (true) {
            Path component = null;
            for (final Iterator<Path> iterator : iterators) {
                if (!iterator.hasNext()) {
                    skipCount--;
                    component = null;
                    break;
                }
                if (component == null) {
                    component = iterator.next();
                } else if (!iterator.next().equals(component)) {
                    component = null;
                    break;
                }
            }
            if (component != null) {
                skipCount++;
            } else {
                break;
            }
        }
        final List<Path> shortened = new ArrayList<>(iterators.size());
        for (final Path path : paths) {
            shortened.add(path.subpath(skipCount, path.getNameCount()));
        }
        return shortened;
    }

    /**
     * Link to files from another directory.
     * @param from the directory from which to link (created if necessary)
     * @param fileIds the files to which to link
     * @throws IOException if the links could not be created
     */
    private void linkFiles(Path from, Iterable<Long> fileIds) throws IOException {
        final List<Path> files = new ArrayList<>();
        for (final Long fileId : fileIds) {
            final Path file = filePaths.get(fileId);
            if (file != null) {
                files.add(file);
            }
        }
        if (files.isEmpty()) {
            return;
        }
        Files.createDirectories(from);
        final Iterator<Path> fullFiles = files.iterator();
        final Iterator<Path> shortFiles = shortenPaths(files).iterator();
        while (fullFiles.hasNext()) {
            final Path fromPath = from.resolve(shortFiles.next());
            final Path toPath = fromPath.getParent().relativize(fullFiles.next());
            if (!Files.exists(fromPath, LinkOption.NOFOLLOW_LINKS)) {
                Files.createDirectories(fromPath.getParent());
                Files.createSymbolicLink(fromPath, toPath);
            }
        }
    }

    /**
     * Ensure that links exist from Image directories to the downloaded files.
     * @throws IOException if the links could not be created
     */
    public void ensureImageFileLinks() throws IOException {
        for (final Map.Entry<Long, Collection<Long>> imageAndFiles : filesOfImages.asMap().entrySet()) {
            final Long image = imageAndFiles.getKey();
            final Collection<Long> files = imageAndFiles.getValue();
            final Path imagePath = localPaths.getImage(image).toPath();
            linkFiles(imagePath, files);
        }
    }

    /**
     * Ensure that links exist from Fileset directories to the downloaded files.
     * @throws IOException if the links could not be created
     */
    public void ensureFilesetFileLinks() throws IOException {
        for (final Map.Entry<Long, Collection<Long>> filesetAndImages : imagesOfFileset.asMap().entrySet()) {
            final Long fileset = filesetAndImages.getKey();
            final Collection<Long> images = filesetAndImages.getValue();
            final Path filesetPath = localPaths.getFileset(fileset).toPath().resolve("Complete");
            final Set<Long> files = new HashSet<>();
            for (final Long image : images) {
                files.addAll(filesOfImages.get(image));
            }
            linkFiles(filesetPath, files);
        }
    }

    /**
     * Ensure that links exist from Fileset Image directories to Image directories.
     * @throws IOException if the links could not be ensured
     */
    public void ensureFilesetImageLinks() throws IOException {
        for (final Map.Entry<Long, Long> imageAndFileset : filesetOfImage.entrySet()) {
            final Long image = imageAndFileset.getKey();
            final Long fileset = imageAndFileset.getValue();
            final Path filesetPath = localPaths.getFileset(fileset).toPath();
            final Path imagePath = localPaths.getImage(image).toPath();
            if (Files.exists(filesetPath) && Files.exists(imagePath)) {
                final Path filesetImagePath = filesetPath.resolve(imagePath.getFileName());
                if (!Files.exists(filesetImagePath, LinkOption.NOFOLLOW_LINKS)) {
                    final Path imageFromFilesetPath = filesetPath.relativize(imagePath);
                    Files.createSymbolicLink(filesetImagePath, imageFromFilesetPath);
                }
            }
        }
    }
}
