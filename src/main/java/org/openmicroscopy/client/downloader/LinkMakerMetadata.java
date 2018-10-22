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
import java.util.HashMap;
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

    private static enum AnnotationType {
        BOOLEAN, COMMENT, DOUBLE, LONG, TAG, TERM, TIMESTAMP, XML;
    }

    private final IMetadata metadata;
    private final Set<Map.Entry<Map.Entry<ModelType, Long>, Map.Entry<ModelType, Long>>> existingLinks = new HashSet<>();
    private final Map<Long, AnnotationType> annotationTypes = new HashMap<>();
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
                case ANNOTATION:
                case IMAGE:
                case ROI:
                    /* will be populated below */
                    continue;
                default:
                    indexMap.put(modelType, Collections.<Long, Integer>emptyMap());
            }
        }
        final ImmutableMap.Builder<Long, Integer> annotationMap = ImmutableMap.builder();
        try {
            buildMetadataIndex(annotationMap, AnnotationType.BOOLEAN, new Function<Integer, String>() {
                @Override
                public String apply(Integer index) {
                    return metadata.getBooleanAnnotationID(index);
                }
            }, metadata.getBooleanAnnotationCount());
        } catch (NullPointerException npe) {
            /* count is zero so move on to next */
        }
        try {
            buildMetadataIndex(annotationMap, AnnotationType.COMMENT, new Function<Integer, String>() {
                @Override
                public String apply(Integer index) {
                    return metadata.getCommentAnnotationID(index);
                }
            }, metadata.getCommentAnnotationCount());
        } catch (NullPointerException npe) {
            /* count is zero so move on to next */
        }
        try {
            buildMetadataIndex(annotationMap, AnnotationType.DOUBLE, new Function<Integer, String>() {
                @Override
                public String apply(Integer index) {
                    return metadata.getDoubleAnnotationID(index);
                }
            }, metadata.getDoubleAnnotationCount());
        } catch (NullPointerException npe) {
            /* count is zero so move on to next */
        }
        try {
            buildMetadataIndex(annotationMap, AnnotationType.LONG, new Function<Integer, String>() {
                @Override
                public String apply(Integer index) {
                    return metadata.getLongAnnotationID(index);
                }
            }, metadata.getLongAnnotationCount());
        } catch (NullPointerException npe) {
            /* count is zero so move on to next */
        }
        try {
            buildMetadataIndex(annotationMap, AnnotationType.TAG, new Function<Integer, String>() {
                @Override
                public String apply(Integer index) {
                    return metadata.getTagAnnotationID(index);
                }
            }, metadata.getTagAnnotationCount());
        } catch (NullPointerException npe) {
            /* count is zero so move on to next */
        }
        try {
            buildMetadataIndex(annotationMap, AnnotationType.TERM, new Function<Integer, String>() {
                @Override
                public String apply(Integer index) {
                    return metadata.getTermAnnotationID(index);
                }
            }, metadata.getTermAnnotationCount());
        } catch (NullPointerException npe) {
            /* count is zero so move on to next */
        }
        try {
            buildMetadataIndex(annotationMap, AnnotationType.TIMESTAMP, new Function<Integer, String>() {
                @Override
                public String apply(Integer index) {
                    return metadata.getTimestampAnnotationID(index);
                }
            }, metadata.getTimestampAnnotationCount());
        } catch (NullPointerException npe) {
            /* count is zero so move on to next */
        }
        try {
            buildMetadataIndex(annotationMap, AnnotationType.XML, new Function<Integer, String>() {
                @Override
                public String apply(Integer index) {
                    return metadata.getXMLAnnotationID(index);
                }
            }, metadata.getXMLAnnotationCount());
        } catch (NullPointerException npe) {
            /* count is zero so move on to next */
        }
        indexMap.put(ModelType.ANNOTATION, annotationMap.build());
        int imageCount;
        try {
            imageCount = metadata.getImageCount();
        } catch (NullPointerException npe) {
            imageCount = 0;
        }
        indexMap.put(ModelType.IMAGE, imageCount == 0 ? Collections.<Long, Integer>emptyMap() :
                 buildMetadataIndex(ImmutableMap.<Long, Integer>builder(), null, new Function<Integer, String>() {
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
                buildMetadataIndex(ImmutableMap.<Long, Integer>builder(), null, new Function<Integer, String>() {
                    @Override
                    public String apply(Integer index) {
                        return metadata.getROIID(index);
                    }
                }, roiCount).build());
        indices = indexMap.build();
    }

    /**
     * Construct a directory of metadata indices for model object IDs.
     * @param idsToIndices a map from OMERO object ID to index in metadata store
     * @param annotationType the type of annotation that these objects are or {@code null} if they are not annotations
     * @param getLsid gets the LSID for a given index
     * @param count how many LSIDs are available
     * @return a directory from model object ID to metadata index
     * @see #getLsid(omero.model.IObject)
     */
    private ImmutableMap.Builder<Long, Integer> buildMetadataIndex(ImmutableMap.Builder<Long, Integer> idsToIndices,
            AnnotationType annotationType, Function<Integer, String> getLsid, int count) {
        for (int index = 0; index < count; index++) {
            final String lsid = getLsid.apply(index);
            final int underscore = lsid.lastIndexOf('_');
            final int colon = lsid.lastIndexOf(':');
            final long id = Long.parseLong(lsid.substring(underscore + 1, colon));
            idsToIndices.put(id, index);
            if (annotationType != null) {
                annotationTypes.put(id, annotationType);
            }
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
            case ANNOTATION:
                switch (annotationTypes.get(toObjectId)) {
                    case BOOLEAN:
                        lsid = metadata.getBooleanAnnotationID(toIndex);
                        break;
                    case COMMENT:
                        lsid = metadata.getCommentAnnotationID(toIndex);
                        break;
                    case DOUBLE:
                        lsid = metadata.getDoubleAnnotationID(toIndex);
                        break;
                    case LONG:
                        lsid = metadata.getLongAnnotationID(toIndex);
                        break;
                    case TAG:
                        lsid = metadata.getTagAnnotationID(toIndex);
                        break;
                    case TERM:
                        lsid = metadata.getTermAnnotationID(toIndex);
                        break;
                    case TIMESTAMP:
                        lsid = metadata.getTimestampAnnotationID(toIndex);
                        break;
                    case XML:
                        lsid = metadata.getXMLAnnotationID(toIndex);
                        break;
                    default:
                        return;
                }
                break;
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
            case ANNOTATION:
                switch (fromObjectType) {
                    case ANNOTATION:
                        switch (annotationTypes.get(fromObjectId)) {
                            case BOOLEAN:
                                metadata.setBooleanAnnotationAnnotationRef(lsid, fromIndex,
                                        metadata.getBooleanAnnotationAnnotationCount(fromIndex));
                                break;
                            case COMMENT:
                                metadata.setCommentAnnotationAnnotationRef(lsid, fromIndex,
                                        metadata.getCommentAnnotationAnnotationCount(fromIndex));
                                break;
                            case DOUBLE:
                                metadata.setDoubleAnnotationAnnotationRef(lsid, fromIndex,
                                        metadata.getDoubleAnnotationAnnotationCount(fromIndex));
                                break;
                            case LONG:
                                metadata.setLongAnnotationAnnotationRef(lsid, fromIndex,
                                        metadata.getLongAnnotationAnnotationCount(fromIndex));
                                break;
                            case TAG:
                                metadata.setTagAnnotationAnnotationRef(lsid, fromIndex,
                                        metadata.getTagAnnotationAnnotationCount(fromIndex));
                                break;
                            case TERM:
                                metadata.setTermAnnotationAnnotationRef(lsid, fromIndex,
                                        metadata.getTermAnnotationAnnotationCount(fromIndex));
                                break;
                            case TIMESTAMP:
                                metadata.setTimestampAnnotationAnnotationRef(lsid, fromIndex,
                                        metadata.getTimestampAnnotationAnnotationCount(fromIndex));
                                break;
                            case XML:
                                metadata.setXMLAnnotationAnnotationRef(lsid, fromIndex,
                                        metadata.getXMLAnnotationAnnotationCount(fromIndex));
                                break;
                            default:
                                return;
                        }
                        break;
                }
                break;
            case IMAGE:
                switch (toObjectType) {
                    case ANNOTATION:
                        metadata.setImageAnnotationRef(lsid, fromIndex, metadata.getImageAnnotationRefCount(fromIndex));
                        break;
                    case ROI:
                        metadata.setImageROIRef(lsid, fromIndex, metadata.getImageROIRefCount(fromIndex));
                        break;
                    default:
                        return;
                }
                break;
            case ROI:
                switch (toObjectType) {
                    case ANNOTATION:
                        metadata.setROIAnnotationRef(lsid, fromIndex, metadata.getROIAnnotationRefCount(fromIndex));
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
