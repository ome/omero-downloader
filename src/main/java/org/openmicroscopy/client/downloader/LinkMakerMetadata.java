/*
 * Copyright (C) 2018 University of Dundee & Open Microscopy Environment.
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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import loci.formats.meta.IMetadata;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

/**
 * Add references among OMERO objects in a metadata store.
 * @author m.t.b.carroll@dundee.ac.uk
 */
public class LinkMakerMetadata {

    private final IMetadata metadata;
    private final Set<Map.Entry<Map.Entry<ModelType, Long>, Map.Entry<ModelType, Long>>> existingLinks = new HashSet<>();
    private final Map<ModelType, Map<Long, Integer>> indices;

    /**
     * From the given metadata store determine its corresponding index argument for each model object.
     * @param metadata a metadata store
     */
    public LinkMakerMetadata(final IMetadata metadata) {
        this.metadata = metadata;
        final ImmutableMap.Builder<ModelType, Map<Long, Integer>> indexMap = ImmutableMap.builder();
        for (final ModelType modelType : ModelType.values()) {
            switch (modelType) {
                case IMAGE:
                case ROI:
                    /* will be populated below */
                    continue;
                default:
                    indexMap.put(modelType, Collections.<Long, Integer>emptyMap());
            }
        }
        int imageCount;
        try {
            imageCount = metadata.getImageCount();
        } catch (NullPointerException npe) {
            imageCount = 0;
        }
        indexMap.put(ModelType.IMAGE, imageCount == 0 ? Collections.<Long, Integer>emptyMap() :
                 buildMetadataIndex(ImmutableMap.<Long, Integer>builder(), new Function<Integer, String>() {
                    @Override
                    public String apply(Integer index) {
                        return metadata.getImageID(index);
                    }
                }, imageCount).build());
        int roiCount;
        try {
            roiCount = metadata.getROICount();
        } catch (NullPointerException npe) {
            roiCount = 0;
        }
        indexMap.put(ModelType.ROI, roiCount == 0 ? Collections.<Long, Integer>emptyMap() :
                buildMetadataIndex(ImmutableMap.<Long, Integer>builder(), new Function<Integer, String>() {
                    @Override
                    public String apply(Integer index) {
                        return metadata.getROIID(index);
                    }
                }, roiCount).build());
        indices = indexMap.build();
    }

    /**
     * Construct a directory of metadata LSIDs for model object IDs.
     * @param getLsid gets the LSID for the given index
     * @param count how many LSIDs are available
     * @return a directory of from model object ID to metadata index
     * @see #getLsid(omero.model.IObject)
     */
    private ImmutableMap.Builder<Long, Integer> buildMetadataIndex(ImmutableMap.Builder<Long, Integer> idsToIndices,
            Function<Integer, String> getLsid, int count) {
        for (int index = 0; index < count; index++) {
            final String lsid = getLsid.apply(index);
            final int underscore = lsid.lastIndexOf('_');
            final int colon = lsid.lastIndexOf(':');
            final long id = Long.parseLong(lsid.substring(underscore + 1, colon));
            idsToIndices.put(id, index);
        }
        return idsToIndices;
    }

    /**
     * Create a reference from one object to another in the metadata store.
     * @param fromObjectType the container object type
     * @param fromObjectId the container object type
     * @param toObjectType the contained object type
     * @param toObjectId the contained object type
     */
    public void linkModelObjects(ModelType fromObjectType, long fromObjectId, ModelType toObjectType, long toObjectId) {
        final Map.Entry<ModelType, Long> fromObject = Maps.immutableEntry(fromObjectType, fromObjectId);
        final Map.Entry<ModelType, Long> toObject = Maps.immutableEntry(toObjectType, toObjectId);
        final Map.Entry<Map.Entry<ModelType, Long>, Map.Entry<ModelType, Long>> link = Maps.immutableEntry(fromObject, toObject);
        if (!existingLinks.add(link)) {
            return;
        }
        final int fromIndex, toIndex;
        try {
            fromIndex = indices.get(fromObjectType).get(fromObjectId);
            toIndex = indices.get(toObjectType).get(toObjectId);
        } catch (NullPointerException npe) {
            /* model objects not indexed by constructor */
            return;
        }
        final String lsid;
        switch (toObjectType) {
            case IMAGE:
                lsid = metadata.getImageID(toIndex);
                break;
            case ROI:
                lsid = metadata.getROIID(toIndex);
                break;
            default:
                return;
        }
        switch (fromObjectType) {
            case IMAGE:
                switch (toObjectType) {
                    case ROI:
                        metadata.setImageROIRef(lsid, fromIndex, metadata.getImageROIRefCount(fromIndex));
                        break;
                    default:
                        return;
                }
                break;
            default:
                return;
        }
    }
}
