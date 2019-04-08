/*
 * Copyright (C) 2016-2019 University of Dundee & Open Microscopy Environment.
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import loci.common.services.ServiceException;
import loci.common.xml.XMLTools;
import loci.formats.meta.MetadataStore;
import loci.formats.ome.OMEXMLMetadata;
import loci.formats.services.OMEXMLService;

import ome.xml.model.OME;
import ome.xml.model.OMEModelObject;

import omero.RLong;
import omero.RType;
import omero.ServerError;
import omero.api.IConfigPrx;
import omero.api.IQueryPrx;
import omero.log.Logger;
import omero.log.SimpleLogger;
import omero.model.Annotation;
import omero.model.BooleanAnnotation;
import omero.model.CommentAnnotation;
import omero.model.DoubleAnnotation;
import omero.model.IObject;
import omero.model.Image;
import omero.model.LongAnnotation;
import omero.model.MapAnnotation;
import omero.model.Mask;
import omero.model.Roi;
import omero.model.Shape;
import omero.model.TagAnnotation;
import omero.model.TermAnnotation;
import omero.model.TimestampAnnotation;
import omero.model.XmlAnnotation;
import omero.sys.Parameters;
import omero.sys.ParametersI;

import org.openmicroscopy.client.downloader.metadata.AnnotationMetadata;
import org.openmicroscopy.client.downloader.metadata.ImageMetadata;
import org.openmicroscopy.client.downloader.metadata.RoiMetadata;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * Convert OMERO model objects to XML.
 * @author m.t.b.carroll@dundee.ac.uk
 */
public class XmlGenerator {

    private static final Logger LOGGER = new SimpleLogger();

    /* the batch size for retrieving model objects from OMERO */
    private static final int BATCH_SIZE = 512;

    private static DocumentBuilder DOCUMENT_BUILDER = null;

    static {
        try {
            DOCUMENT_BUILDER = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        } catch (ParserConfigurationException pce) {
            LOGGER.fatal(pce, "cannot build XML documents");
            Download.abortOnFatalError(3);
        }
    }

    private static final Multimap<ModelType, Map.Entry<ModelType, String>> CONTAINER_QUERIES;

    static {
        final ImmutableMultimap.Builder<ModelType, Map.Entry<ModelType, String>> builder = ImmutableMultimap.builder();
        builder.put(ModelType.PROJECT, Maps.immutableEntry(ModelType.DATASET,
                "SELECT parent.id, child.id FROM ProjectDatasetLink WHERE parent.id IN (:ids)"));
        builder.put(ModelType.PROJECT, Maps.immutableEntry(ModelType.ANNOTATION,
                "SELECT parent.id, child.id FROM ProjectAnnotationLink WHERE parent.id IN (:ids)"));
        builder.put(ModelType.DATASET, Maps.immutableEntry(ModelType.IMAGE,
                "SELECT parent.id, child.id FROM DatasetImageLink WHERE parent.id IN (:ids)"));
        builder.put(ModelType.DATASET, Maps.immutableEntry(ModelType.ANNOTATION,
                "SELECT parent.id, child.id FROM DatasetAnnotationLink WHERE parent.id IN (:ids)"));
        builder.put(ModelType.FOLDER, Maps.immutableEntry(ModelType.IMAGE,
                "SELECT parent.id, child.id FROM FolderImageLink WHERE parent.id IN (:ids)"));
        builder.put(ModelType.FOLDER, Maps.immutableEntry(ModelType.ROI,
                "SELECT parent.id, child.id FROM FolderRoiLink WHERE parent.id IN (:ids)"));
        builder.put(ModelType.FOLDER, Maps.immutableEntry(ModelType.FOLDER,
                "SELECT parentFolder.id, id FROM Folder WHERE parentFolder.id IN (:ids)"));
        builder.put(ModelType.FOLDER, Maps.immutableEntry(ModelType.ANNOTATION,
                "SELECT parent.id, child.id FROM FolderAnnotationLink WHERE parent.id IN (:ids)"));
        builder.put(ModelType.SCREEN, Maps.immutableEntry(ModelType.PLATE,
                "SELECT parent.id, child.id FROM ScreenPlateLink WHERE parent.id IN (:ids)"));
        builder.put(ModelType.SCREEN, Maps.immutableEntry(ModelType.ANNOTATION,
                "SELECT parent.id, child.id FROM ScreenAnnotationLink WHERE parent.id IN (:ids)"));
        builder.put(ModelType.PLATE, Maps.immutableEntry(ModelType.IMAGE,
                "SELECT well.plate.id, image.id FROM WellSample WHERE well.plate.id IN (:ids)"));
        builder.put(ModelType.PLATE, Maps.immutableEntry(ModelType.ANNOTATION,
                "SELECT parent.id, child.id FROM PlateAnnotationLink WHERE parent.id IN (:ids)"));
        builder.put(ModelType.IMAGE, Maps.immutableEntry(ModelType.ROI,
                "SELECT image.id, id FROM Roi WHERE image.id IN (:ids)"));
        builder.put(ModelType.IMAGE, Maps.immutableEntry(ModelType.INSTRUMENT,
                "SELECT id, instrument.id FROM Image WHERE id IN (:ids)"));
        builder.put(ModelType.IMAGE, Maps.immutableEntry(ModelType.ANNOTATION,
                "SELECT parent.id, child.id FROM ImageAnnotationLink WHERE parent.id IN (:ids)"));
        builder.put(ModelType.INSTRUMENT, Maps.immutableEntry(ModelType.ANNOTATION,
                "SELECT parent.id, child.id FROM InstrumentAnnotationLink WHERE parent.id IN (:ids)"));
        builder.put(ModelType.ROI, Maps.immutableEntry(ModelType.ANNOTATION,
                "SELECT parent.id, child.id FROM RoiAnnotationLink WHERE parent.id IN (:ids)"));
        builder.put(ModelType.ANNOTATION, Maps.immutableEntry(ModelType.ANNOTATION,
                "SELECT parent.id, child.id FROM AnnotationAnnotationLink WHERE parent.id IN (:ids)"));
        CONTAINER_QUERIES = builder.build();
    }

    /**
     * Receives notification that one model object contains another.
     * @see #queryRelationships(org.openmicroscopy.client.downloader.XmlGenerator.ContainmentListener,
     * com.google.common.collect.SetMultimap)
     */
    public interface ContainmentListener {
        /**
         * The container model object contains the contained model object.
         * @param containerType the type of the model object that contains the other
         * @param containerId the ID of the model object that contains the other
         * @param containedType the type of the model object that is contained by the other
         * @param containedId the ID of the model object that is contained by the other
         */
        void contains(ModelType containerType, long containerId, ModelType containedType, long containedId);

        /**
         * Report if this containment listener requests to be notified of relationships involving the given model object type.
         * @param modelType a model object type
         * @return if this listener requests notifications involving the given model object type.
         */
        boolean isWanted(ModelType modelType);
    }

    private final OMEXMLService omeXmlService;
    private final IQueryPrx iQuery;
    private final String format;

    private final Function<IObject, String> lsidGetter = new Function<IObject, String>() {
        @Override
        public String apply(IObject object) {
            return getLsid(object);
        }
    };

    /**
     * Query the parent-child relationships among model objects.
     * @param listener the listener to notify of parent-child relationships
     * @param objects the parents from which to start the query
     * @throws ServerError if a query failed
     */
    public void queryRelationships(ContainmentListener listener, SetMultimap<ModelType, Long> objects) throws ServerError {
        SetMultimap<ModelType, Long> toQuery = objects;
        final SetMultimap<ModelType, Long> queried = HashMultimap.create();
        while (!toQuery.isEmpty()) {
            final SetMultimap<ModelType, Long> nextToQuery = HashMultimap.create();
            for (final Map.Entry<ModelType, Collection<Long>> toQueryOneType : toQuery.asMap().entrySet()) {
                final ModelType parentType = toQueryOneType.getKey();
                final List<Long> parentIds = ImmutableList.copyOf(toQueryOneType.getValue());
                if (listener.isWanted(parentType)) {
                    for (final Map.Entry<ModelType, String> relationship : CONTAINER_QUERIES.get(parentType)) {
                        final ModelType childType = relationship.getKey();
                        final String hql = relationship.getValue();
                        if (listener.isWanted(childType)) {
                            for (final List<Long> idBatch : Lists.partition(parentIds, BATCH_SIZE)) {
                                final Parameters params = new ParametersI().addIds(idBatch);
                                for (final List<RType> result : iQuery.projection(hql, params, Download.ALL_GROUPS_CONTEXT)) {
                                    final long parentId = ((RLong) result.get(0)).getValue();
                                    final long childId = ((RLong) result.get(1)).getValue();
                                    listener.contains(parentType, parentId, childType, childId);
                                    nextToQuery.put(childType, childId);
                                }
                            }
                        }
                    }
                }
                queried.putAll(parentType, parentIds);
            }
            toQuery = nextToQuery;
            final SetMultimap<ModelType, Long> toRemove = HashMultimap.create();
            for (final ModelType toQueryType : toQuery.keySet()) {
                toRemove.putAll(toQueryType, queried.get(toQueryType));
            }
            final Map<ModelType, Collection<Long>> queryMap = toQuery.asMap();
            for (final Map.Entry<ModelType, Collection<Long>> toRemoveOneType : toRemove.asMap().entrySet()) {
                queryMap.get(toRemoveOneType.getKey()).removeAll(toRemoveOneType.getValue());
            }
        }
    }

    /**
     * Helper for writing OMERO model objects as locally as XML.
     */
    private interface ModelObjectWriter {
        /**
         * @return the name of the kind of objects that this writer can write
         */
        String getObjectsName();

        /**
         * Write the objects of the given IDs to the given files.
         * @param toWrite the object IDs and corresponding files
         */
        void writeObjects(Map<Long, File> toWrite) throws IOException, ServerError, ServiceException, TransformerException;
    }

    /**
     * Find the LSID of the given OMERO model object.
     * @param object an OMERO model object, hydrated with its update event
     * @return the LSID for that object
     */
    private String getLsid(IObject object) {
        Class<? extends IObject> objectClass = object.getClass();
        if (objectClass == IObject.class) {
            throw new IllegalArgumentException("must be of a specific model object type");
        }
        while (objectClass.getSuperclass() != IObject.class) {
            objectClass = objectClass.getSuperclass().asSubclass(IObject.class);
        }
        final long objectId = object.getId().getValue();
        final long updateId = object.getDetails().getUpdateEvent().getId().getValue();
        return String.format(format, objectClass.getSimpleName(), objectId, updateId);
    }

    /**
     * Construct a new generator of XML for OMERO model objects.
     * @param omeXmlService the OME-XML service
     * @param iConfig the configuration service
     * @param iQuery the query service
     * @throws ServerError if the configuration service could not be used
     */
    public XmlGenerator(OMEXMLService omeXmlService, IConfigPrx iConfig, IQueryPrx iQuery) throws ServerError {
        this.omeXmlService = omeXmlService;
        this.iQuery = iQuery;
        this.format = String.format("urn:lsid:%s:%%s:%s_%%s:%%s",
                iConfig.getConfigValue("omero.db.authority"),
                iConfig.getDatabaseUuid());
    }

    /**
     * Query the server for the given annotations.
     * @param ids the IDs of the annotations to retrieve from OMERO
     * @return the annotations, hydrated sufficiently for conversion to XML
     * @throws ServerError if the annotations could not be retrieved
     */
    private List<Annotation> getAnnotations(Collection<Long> ids) throws ServerError {
        if (ids.isEmpty()) {
            return Collections.emptyList();
        }
        final List<Annotation> annotations = new ArrayList<>(ids.size());
        for (final IObject result : iQuery.findAllByQuery(
                "FROM Annotation a " +
                "WHERE a.id IN (:ids)", new ParametersI().addIds(ids), Download.ALL_GROUPS_CONTEXT)) {
            annotations.add((Annotation) result);
        }
        return annotations;
    }

    /**
     * Query the server for the given images.
     * @param ids the IDs of the images to retrieve from OMERO
     * @return the images, hydrated sufficiently for conversion to XML
     * @throws ServerError if the images could not be retrieved
     */
    private List<Image> getImages(Collection<Long> ids) throws ServerError {
        if (ids.isEmpty()) {
            return Collections.emptyList();
        }
        final List<Image> images = new ArrayList<>(ids.size());
        for (final IObject result : iQuery.findAllByQuery(
                "FROM Image i " +
                "LEFT OUTER JOIN FETCH i.pixels AS p " +
                "LEFT OUTER JOIN FETCH p.channels AS c " +
                "LEFT OUTER JOIN FETCH c.logicalChannel AS l " +
                "LEFT OUTER JOIN FETCH p.pixelsType " +
                "LEFT OUTER JOIN FETCH p.planeInfo " +
                "LEFT OUTER JOIN FETCH l.illumination " +
                "LEFT OUTER JOIN FETCH l.mode " +
                "LEFT OUTER JOIN FETCH p.details.updateEvent " +
                "LEFT OUTER JOIN FETCH c.details.updateEvent " +
                "WHERE i.id IN (:ids)", new ParametersI().addIds(ids), Download.ALL_GROUPS_CONTEXT)) {
            images.add((Image) result);
        }
        return images;
    }

    /**
     * Query the server for the given ROIs.
     * @param ids the IDs of the ROIs to retrieve from OMERO
     * @return the ROIs, hydrated sufficiently for conversion to XML
     * @throws ServerError if the ROIs could not be retrieved
     */
    private List<Roi> getRois(Collection<Long> ids) throws ServerError {
        if (ids.isEmpty()) {
            return Collections.emptyList();
        }
        final List<Roi> rois = new ArrayList<>(ids.size());
        for (final IObject result : iQuery.findAllByQuery(
                "FROM Roi r " +
                "LEFT OUTER JOIN FETCH r.shapes AS s " +
                "LEFT OUTER JOIN FETCH r.details.updateEvent " +
                "LEFT OUTER JOIN FETCH s.details.updateEvent " +
                "WHERE r.id IN (:ids)", new ParametersI().addIds(ids), Download.ALL_GROUPS_CONTEXT)) {
            rois.add((Roi) result);
        }
        return rois;
    }

    /**
     * Write the given OME model object as XML to the given file.
     * @param modelObject the model object to write
     * @param destination the file into which to write the XML
     * @throws IOException if the file could not be written
     * @throws TransformerException if the XML could not be generated
     */
    private static void writeElement(OMEModelObject modelObject, File destination) throws IOException, TransformerException {
        final File temporaryFile = new File(destination.getParentFile(), "temp-" + UUID.randomUUID());
        final Document document = DOCUMENT_BUILDER.newDocument();
        final Element xmlElement = modelObject.asXMLElement(document);
        document.appendChild(xmlElement);
        try (final OutputStream out = new FileOutputStream(temporaryFile)) {
            XMLTools.writeXML(out, document, false);
        } catch (IOException | TransformerException e) {
            temporaryFile.delete();
            throw e;
        }
        temporaryFile.renameTo(destination);
    }

    /**
     * Determine which model object files still require writing.
     * @param ids the IDs of the objects
     * @param destinations how to determine the files in which the objects should be found
     * @return the IDs of the objects and to where to write them, may be empty but never {@code null}
     */
    private static Map<Long, File> getFileMap(List<Long> ids, Function<Long, File> destinations) {
        final Map<Long, File> files = new HashMap<>();
        for (final Long id : ids) {
            final File destination = destinations.apply(id);
            if (!destination.exists()) {
                files.put(id, destination);
            }
        }
        return files;
    }

    /**
     * Write the given annotations from the server into the given metadata store.
     * @param ids the IDs of the annotations to write
     * @param destination the metadata store into which to write the annotations
     * @throws ServerError if the annotations could not be read
     */
    public void writeAnnotations(List<Long> ids, MetadataStore destination) throws ServerError {
        for (final List<Long> annotationIdBatch : Lists.partition(ids, BATCH_SIZE)) {
            final List<Annotation> annotations = getAnnotations(annotationIdBatch);
            omeXmlService.convertMetadata(new AnnotationMetadata(lsidGetter, annotations), destination);
        }
    }

    /**
     * Write the given images from the server into the given metadata store.
     * @param ids the IDs of the images to write
     * @param destination the metadata store into which to write the images
     * @throws ServerError if the images could not be read
     */
    public void writeImages(List<Long> ids, MetadataStore destination) throws ServerError {
        for (final List<Long> imageIdBatch : Lists.partition(ids, BATCH_SIZE)) {
            final List<Image> images = getImages(imageIdBatch);
            omeXmlService.convertMetadata(new ImageMetadata(lsidGetter, images), destination);
        }
    }

    /**
     * Write the given ROIs from the server into the given metadata store.
     * @param ids the IDs of the ROIs to write
     * @param destination the metadata store into which to write the ROIs
     * @throws ServerError if the ROIs could not be read
     */
    public void writeRois(List<Long> ids, MetadataStore destination) throws ServerError {
        for (final List<Long> roiIdBatch : Lists.partition(ids, BATCH_SIZE)) {
            final List<Roi> rois = new ArrayList<>(getRois(roiIdBatch));
            final Iterator<Roi> roiIterator = rois.iterator();
            while (roiIterator.hasNext()) {
                final Roi roi = roiIterator.next();
                /* TODO: Understand how to include Mask objects. */
                removeMasks(roi);
                if (roi.sizeOfShapes() == 0) {
                    /* ROIs must have shapes */
                    roiIterator.remove();
                }
            }
            omeXmlService.convertMetadata(new RoiMetadata(lsidGetter, rois), destination);
        }
    }

    /**
     * Remove any masks from the given ROI as they cannot yet be serialized.
     * @param roi a ROI
     */
   private void removeMasks(Roi roi) {
        if (roi.sizeOfShapes() > 0) {
            /* Omit any masks from the ROI's shapes. */
            boolean isChanged = false;
            final List<Shape> shapes = new ArrayList<>(roi.copyShapes());
            final Iterator<Shape> shapeIterator = shapes.iterator();
            while (shapeIterator.hasNext()) {
                if (shapeIterator.next() instanceof Mask) {
                    shapeIterator.remove();
                    isChanged = true;
                }
            }
            if (isChanged) {
                roi.clearShapes();
                roi.addAllShapeSet(shapes);
            }
        }
    }

    /**
     * Write the given annotations locally as XML.
     * @param ids the IDs of the annotations to write
     * @param destinations how to determine the files into which to write the annotations
     */
    public void writeAnnotations(List<Long> ids, Function<Long, File> destinations) {
        writeElements(ids, destinations, new ModelObjectWriter() {
            @Override
            public String getObjectsName() {
                return "annotations";
            }

            @Override
            public void writeObjects(Map<Long, File> toWrite)
                    throws IOException, ServerError, ServiceException, TransformerException {
                for (final Annotation annotation : getAnnotations(toWrite.keySet())) {
                    final OMEXMLMetadata metadata = omeXmlService.createOMEXMLMetadata();
                    metadata.createRoot();
                    omeXmlService.convertMetadata(new AnnotationMetadata(lsidGetter, Collections.singletonList(annotation)),
                            metadata);
                    final OME omeElement = (OME) metadata.getRoot();
                    final ome.xml.model.Annotation annotationElement;
                    if (annotation instanceof BooleanAnnotation) {
                        annotationElement = omeElement.getStructuredAnnotations().getBooleanAnnotation(0);
                    } else if (annotation instanceof CommentAnnotation) {
                        annotationElement = omeElement.getStructuredAnnotations().getCommentAnnotation(0);
                    } else if (annotation instanceof DoubleAnnotation) {
                        annotationElement = omeElement.getStructuredAnnotations().getDoubleAnnotation(0);
                    } else if (annotation instanceof LongAnnotation) {
                        annotationElement = omeElement.getStructuredAnnotations().getLongAnnotation(0);
                    } else if (annotation instanceof MapAnnotation) {
                        annotationElement = omeElement.getStructuredAnnotations().getMapAnnotation(0);
                    } else if (annotation instanceof TagAnnotation) {
                        annotationElement = omeElement.getStructuredAnnotations().getTagAnnotation(0);
                    } else if (annotation instanceof TermAnnotation) {
                        annotationElement = omeElement.getStructuredAnnotations().getTermAnnotation(0);
                    } else if (annotation instanceof TimestampAnnotation) {
                        annotationElement = omeElement.getStructuredAnnotations().getTimestampAnnotation(0);
                    } else if (annotation instanceof XmlAnnotation) {
                        annotationElement = omeElement.getStructuredAnnotations().getXMLAnnotation(0);
                    } else {
                        continue;
                    }
                    writeElement(annotationElement, toWrite.get(annotation.getId().getValue()));
                }
            }
        });
    }

    /**
     * Write the given images locally as XML.
     * @param ids the IDs of the images to write
     * @param destinations how to determine the files into which to write the images
     */
    public void writeImages(List<Long> ids, Function<Long, File> destinations) {
        writeElements(ids, destinations, new ModelObjectWriter() {
            @Override
            public String getObjectsName() {
                return "images";
            }

            @Override
            public void writeObjects(Map<Long, File> toWrite)
                    throws IOException, ServerError, ServiceException, TransformerException {
                for (final Image image : getImages(toWrite.keySet())) {
                    final OMEXMLMetadata metadata = omeXmlService.createOMEXMLMetadata();
                    metadata.createRoot();
                    omeXmlService.convertMetadata(new ImageMetadata(lsidGetter, Collections.singletonList(image)), metadata);
                    final OME omeElement = (OME) metadata.getRoot();
                    final ome.xml.model.Image imageElement = omeElement.getImage(0);
                    final ome.xml.model.Pixels pixels = imageElement.getPixels();
                    if (pixels != null) {
                        pixels.setMetadataOnly(new ome.xml.model.MetadataOnly());
                    }
                    writeElement(imageElement, toWrite.get(image.getId().getValue()));
                }
            }
        });
    }

    /**
     * Write the given ROIs locally as XML.
     * @param ids the IDs of the ROIs to write
     * @param destinations how to determine the files into which to write the ROIs
     */
    public void writeRois(List<Long> ids, Function<Long, File> destinations) {
        writeElements(ids, destinations, new ModelObjectWriter() {
            @Override
            public String getObjectsName() {
                return "ROIs";
            }

            @Override
            public void writeObjects(Map<Long, File> toWrite)
                    throws IOException, ServerError, ServiceException, TransformerException {
                for (final Roi roi : getRois(toWrite.keySet())) {
                    /* TODO: Understand how to include Mask objects. */
                    removeMasks(roi);
                    if (roi.sizeOfShapes() == 0) {
                        /* ROIs must have shapes */
                        continue;
                    }
                    final OMEXMLMetadata xmlMeta = omeXmlService.createOMEXMLMetadata();
                    xmlMeta.createRoot();
                    omeXmlService.convertMetadata(new RoiMetadata(lsidGetter, Collections.singletonList(roi)), xmlMeta);
                    final OME omeElement = (OME) xmlMeta.getRoot();
                    final ome.xml.model.ROI roiElement = omeElement.getROI(0);
                    writeElement(roiElement, toWrite.get(roi.getId().getValue()));
                }
            }
        });
    }

    /**
     * Write the given model objects locally as XML.
     * @param ids the IDs of the objects to write
     * @param destinations how to determine the files into which to write the ROIs
     * @param objectWriter the helper that does the actual class-specific writing
     */
    private void writeElements(List<Long> ids, Function<Long, File> destinations, ModelObjectWriter objectWriter) {
        final Map<Long, File> files = getFileMap(ids, destinations);
        if (files.isEmpty()) {
            System.out.println("written " + objectWriter.getObjectsName() +" as XML, already have " + ids.size());
            return;
        }
        System.out.print("writing " + objectWriter.getObjectsName() +" as XML, need up to " + ids.size());
        final int alreadyHave = ids.size() - files.size();
        if (alreadyHave > 0) {
            System.out.print(", already have " + alreadyHave);
        }
        System.out.print("..");
        try {
            for (final List<Long> idBatch : Lists.partition(ImmutableList.copyOf(files.keySet()), BATCH_SIZE)) {
                System.out.print('.');
                System.out.flush();
                final ImmutableMap.Builder<Long, File> toWrite = ImmutableMap.builder();
                for (final Long id : idBatch) {
                    toWrite.put(id, files.get(id));
                }
                objectWriter.writeObjects(toWrite.build());
            }
            System.out.println(" done");
        } catch (IOException | ServerError | ServiceException | TransformerException e) {
            System.out.println(" failed");
            LOGGER.error(e, "failed to obtain " + objectWriter.getObjectsName() +" and write as XML");
        }
    }
}
