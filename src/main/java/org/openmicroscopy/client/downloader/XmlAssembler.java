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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stream.StreamResult;

import loci.common.services.ServiceException;
import loci.common.xml.XMLTools;
import loci.formats.services.OMEXMLService;

import ome.xml.meta.OMEXMLMetadata;
import ome.xml.model.Annotation;
import ome.xml.model.AnnotationRef;
import ome.xml.model.BooleanAnnotation;
import ome.xml.model.CommentAnnotation;
import ome.xml.model.DoubleAnnotation;
import ome.xml.model.LongAnnotation;
import ome.xml.model.OME;
import ome.xml.model.OMEModel;
import ome.xml.model.OMEModelObject;
import ome.xml.model.ROIRef;
import ome.xml.model.Reference;
import ome.xml.model.TagAnnotation;
import ome.xml.model.TermAnnotation;
import ome.xml.model.TimestampAnnotation;
import ome.xml.model.XMLAnnotation;
import ome.xml.model.enums.EnumerationException;

import omero.log.Logger;
import omero.log.SimpleLogger;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import org.openmicroscopy.client.downloader.util.TruncatingOutputStream;

/**
 * A stateless barebones OME model sufficient for deserializing from XML.
 * @author m.t.b.carroll@dundee.ac.uk
 */
class StatelessModel implements OMEModel {

    @Override
    public OMEModelObject addModelObject(String id, OMEModelObject object) {
        return object;
    }

    @Override
    public OMEModelObject removeModelObject(String id) {
        throw new UnsupportedOperationException();
    }

    @Override
    public OMEModelObject getModelObject(String id) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, OMEModelObject> getModelObjects() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addReference(OMEModelObject a, Reference b) {
        return false;
    }

    @Override
    public Map<OMEModelObject, List<Reference>> getReferences() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int resolveReferences() {
        throw new UnsupportedOperationException();
    }
}

/**
 * Assemble OMERO model objects into cross-referenced XML.
 * @author m.t.b.carroll@dundee.ac.uk
 */
public class XmlAssembler implements Closeable {

    private static final Logger LOGGER = new SimpleLogger();

    private static final String OME_XML_CREATOR = "OMERO.downloader";

    private static final OMEModel STATELESS_MODEL = new StatelessModel();

    private static final OME OME_OBJECT;

    static {
        OME_OBJECT = new ome.xml.model.OME();
        OME_OBJECT.setCreator(OME_XML_CREATOR);
    }

    private DocumentBuilder documentBuilder = null;

    private byte[] omeXmlHeader;
    private byte[] omeXmlFooter;

    private int omeXmlHeaderSkip;
    private int omeXmlFooterSkip;

    private final Map<Map.Entry<ModelType, ModelType>, SetMultimap<Long, Long>> containment;
    private final Map<Long, Class<? extends Annotation>> annotationTypes = new HashMap<>();
    private final Map<Map.Entry<ModelType, Long>, String> lsids = new HashMap<>();
    private final Function<Map.Entry<ModelType, Long>, File> xmlFiles;
    private final OutputStream out;

    /**
     * Create a new XML assembler for OME metadata export.
     * The writer methods are used to provide content from already-exported XML fragments in between the {@code OME}-element
     * open-and-close provided by this class.
     * @param omeXmlService the OME-XML service
     * @param containment the parent-child relationships to use in adding {@link Reference} elements
     * @param metadataFiles a locator for files containing XML fragments
     * @param destination where to write the assembled XML
     * @throws IOException if the destination was not writable
     */
    public XmlAssembler(OMEXMLService omeXmlService, Map<Map.Entry<ModelType, ModelType>, SetMultimap<Long, Long>> containment,
            Function<Map.Entry<ModelType, Long>, File> metadataFiles, OutputStream destination) throws IOException {
        ensureOmeXmlHeaderFooter(omeXmlService);
        this.containment = containment;
        this.xmlFiles = metadataFiles;
        this.out = destination;
        this.out.write(omeXmlHeader);
    }

    /**
     * Set up the document builder. Note the XML header and footer to use for OME documents.
     * Note the size of the headers and footers for {@link #writeModelObject(ome.xml.model.OMEModelObject)} to trim.
     */
    private void ensureOmeXmlHeaderFooter(OMEXMLService omeXmlService) {
        if (documentBuilder != null) {
            /* already done */
            return;
        }
        /* create document builder */
        try {
            documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        } catch (ParserConfigurationException pce) {
            LOGGER.fatal(pce, "cannot build XML documents");
            System.exit(3);
        }

        /* determine full headers as to be written */
        OMEXMLMetadata metadata = null;
        try {
            metadata = omeXmlService.createOMEXMLMetadata();
        } catch (ServiceException se) {
            LOGGER.fatal(se, "cannot create metadata object");
            System.exit(3);
        }
        metadata.createRoot();
        metadata.setImageID("image", 0);
        String documentText = metadata.dumpXML();
        int declarationLength = documentText.indexOf('>') + 1;
        int footerLength = documentText.length() - documentText.lastIndexOf('<');
        int headerLength = documentText.indexOf('>', declarationLength) + 1 - declarationLength;
        omeXmlHeader = documentText.substring(0, declarationLength + headerLength).getBytes(StandardCharsets.UTF_8);
        omeXmlFooter = documentText.substring(documentText.length() - footerLength).getBytes(StandardCharsets.UTF_8);

        /* determine headers to be skipped by writeModelObject */
        final Document documentDOM = documentBuilder.newDocument();
        final Node omeElement = OME_OBJECT.asXMLElement(documentDOM);
        final Node textNode = documentDOM.createTextNode(XmlAssembler.class.getName());
        documentDOM.appendChild(omeElement);
        omeElement.appendChild(textNode);
        final StringWriter documentWriter = new StringWriter();
        try {
            XMLTools.writeXML(new StreamResult(documentWriter), documentDOM, true);
        } catch (TransformerException te) {
            LOGGER.fatal(te, "cannot write XML documents");
            System.exit(3);
        }
        documentText = documentWriter.toString();
        declarationLength = documentText.indexOf('>') + 1;
        footerLength = documentText.length() - documentText.lastIndexOf('<');
        headerLength = documentText.indexOf('>', declarationLength) + 1 - declarationLength;
        omeXmlHeaderSkip = headerLength;
        omeXmlFooterSkip = footerLength;
    }

    /**
     * Write the given model object as XML.
     * @param object a model object
     * @throws IOException if the XML writing failed
     */
    private void writeModelObject(OMEModelObject object) throws IOException {
        /* To omit xmlns attribute actually wraps the object in an OME element that is then stripped. */
        final Document document = documentBuilder.newDocument();
        final Node omeElement = OME_OBJECT.asXMLElement(document);
        final Node givenElement = object.asXMLElement(document);
        document.appendChild(omeElement);
        omeElement.appendChild(givenElement);
        final OutputStream truncater = new TruncatingOutputStream(omeXmlHeaderSkip, omeXmlFooterSkip, out);
        try {
            XMLTools.writeXML(truncater, document, false);
        } catch (TransformerException te) {
            LOGGER.fatal(te, "cannot write XML fragment");
            System.exit(3);
        }
    }

    @Override
    public void close() throws IOException {
        out.write(omeXmlFooter);
    }

    /**
     * Obtain a model object's LSID from its metadata file. Maintains a cache from past reads.
     * @param objectType the type of a model object
     * @param objectId the ID of a model object
     * @return an LSID for the model object
     * @throws IOException if the object's metadata file could not be read
     */
    private String getLsid(ModelType objectType, long objectId) throws IOException {
        final Map.Entry<ModelType, Long> object = Maps.immutableEntry(objectType, objectId);
        String lsid = lsids.get(object);
        if (lsid == null) {
            Document document = null;
            try {
                document = XMLTools.parseDOM(xmlFiles.apply(Maps.immutableEntry(objectType, objectId)));
            } catch (ParserConfigurationException | SAXException e) {
                LOGGER.fatal(e, "cannot read XML document");
                System.exit(3);
            }
            lsid = document.getDocumentElement().getAttribute("ID");
            lsids.put(object, lsid);
        }
        return lsid;
    }

    /**
     * Construct a concrete annotation of the same type as that from the local metadata file of a given annotation.
     * @param id an annotation ID with a local metadata file
     * @param element the annotation's DOM node already parsed from the metadata file, may be {@code null}
     * @return a new instance of the annotation's class in the OME-XML model
     * @throws IOException if the metadata file could not be read
     */
    private ome.xml.model.Annotation getAnnotationObject(long id, Element element) throws IOException {
        Class<? extends Annotation> annotationType = annotationTypes.get(id);
        if (annotationType == null) {
            if (element == null) {
                try {
                    final Document document = XMLTools.parseDOM(xmlFiles.apply(Maps.immutableEntry(ModelType.ANNOTATION, id)));
                    element = document.getDocumentElement();
                } catch (ParserConfigurationException | SAXException e) {
                    LOGGER.fatal(e, "cannot read XML document");
                    System.exit(3);
                }
            }
            switch (element.getNodeName()) {
                case "BooleanAnnotation":
                    annotationType = BooleanAnnotation.class;
                    break;
                case "CommentAnnotation":
                    annotationType = CommentAnnotation.class;
                    break;
                case "DoubleAnnotation":
                    annotationType = DoubleAnnotation.class;
                    break;
                case "LongAnnotation":
                    annotationType = LongAnnotation.class;
                    break;
                case "TagAnnotation":
                    annotationType = TagAnnotation.class;
                    break;
                case "TermAnnotation":
                    annotationType = TermAnnotation.class;
                    break;
                case "TimestampAnnotation":
                    annotationType = TimestampAnnotation.class;
                    break;
                case "XMLAnnotation":
                    annotationType = XMLAnnotation.class;
                    break;
                default:
                    throw new IllegalArgumentException("annotation " + id + " has element " + element);
            }
            annotationTypes.put(id, annotationType);
        }
        try {
            return annotationType.newInstance();
        } catch (ReflectiveOperationException roe) {
            LOGGER.fatal(roe, "cannot instantiate " + annotationType);
            System.exit(3);
            return null;

        }
    }

    /**
     * Write the XML element for the given annotation.
     * @param annotationId an annotation ID
     * @throws IOException if the annotation's metadata file could not be read or the annotation's element could not be written
     */
    private void writeAnnotationElement(long annotationId) throws IOException {
        /* read annotation metadata */
        Document document = null;
        try {
            document = XMLTools.parseDOM(xmlFiles.apply(Maps.immutableEntry(ModelType.ANNOTATION, annotationId)));
        } catch (ParserConfigurationException | SAXException e) {
            LOGGER.fatal(e, "cannot read XML document");
            System.exit(3);
        }
        final Element element = document.getDocumentElement();
        final OMEModelObject annotation = getAnnotationObject(annotationId, element);
        try {
            annotation.update(element, STATELESS_MODEL);
        } catch (EnumerationException e) {
            LOGGER.fatal(e, "cannot process XML document");
            System.exit(3);
        }
        /* write annotation element */
        writeModelObject(annotation);
    }

    /**
     * Write the XML {@code Image} element for the given image.
     * @param imageId an image ID
     * @throws IOException if the image's metadata file could not be read or the {@code Image} element could not be written
     */
    private void writeImageElement(long imageId) throws IOException {
        /* read image metadata */
        Document document = null;
        try {
            document = XMLTools.parseDOM(xmlFiles.apply(Maps.immutableEntry(ModelType.IMAGE, imageId)));
        } catch (ParserConfigurationException | SAXException e) {
            LOGGER.fatal(e, "cannot read XML document");
            System.exit(3);
        }
        final OMEModelObject image = new ome.xml.model.Image();
        try {
            image.update(document.getDocumentElement(), STATELESS_MODEL);
        } catch (EnumerationException e) {
            LOGGER.fatal(e, "cannot process XML document");
            System.exit(3);
        }
        /* note cross-references */
        final SetMultimap<Long, Long> imageAnnotationMap = containment.get(
                Maps.immutableEntry(ModelType.IMAGE, ModelType.ANNOTATION));
        final SetMultimap<Long, Long> imageRoiMap = containment.get(
                Maps.immutableEntry(ModelType.IMAGE, ModelType.ROI));
        for (final long annotationId : imageAnnotationMap.get(imageId)) {
            final ome.xml.model.Annotation annotation = getAnnotationObject(annotationId, null);
            final Reference ref = new AnnotationRef();
            annotation.setID(getLsid(ModelType.ANNOTATION, annotationId));
            image.link(ref, annotation);
        }
        for (final long roiId : imageRoiMap.get(imageId)) {
            final ome.xml.model.ROI roi = new ome.xml.model.ROI();
            final Reference ref = new ROIRef();
            roi.setID(getLsid(ModelType.ROI, roiId));
            image.link(ref, roi);
        }
        /* write Image element */
        writeModelObject(image);
    }

    /**
     * Write the XML {@code ROI} element for the given ROI.
     * @param roiId a ROI ID
     * @throws IOException if the ROI's metadata file could not be read or the {@code ROI} element could not be written
     */
    private void writeRoiElement(long roiId) throws IOException {
        /* read ROI metadata */
        Document document = null;
        try {
            document = XMLTools.parseDOM(xmlFiles.apply(Maps.immutableEntry(ModelType.ROI, roiId)));
        } catch (ParserConfigurationException | SAXException e) {
            LOGGER.fatal(e, "cannot read XML document");
            System.exit(3);
        }
        final OMEModelObject roi = new ome.xml.model.ROI();
        try {
            roi.update(document.getDocumentElement(), STATELESS_MODEL);
        } catch (EnumerationException e) {
            LOGGER.fatal(e, "cannot process XML document");
            System.exit(3);
        }
        /* note cross-references */
        final SetMultimap<Long, Long> roiAnnotationMap = containment.get(
                Maps.immutableEntry(ModelType.ROI, ModelType.ANNOTATION));
        for (final long annotationId : roiAnnotationMap.get(roiId)) {
            final ome.xml.model.Annotation annotation = getAnnotationObject(annotationId, null);
            final Reference ref = new AnnotationRef();
            annotation.setID(getLsid(ModelType.ANNOTATION, annotationId));
            roi.link(ref, annotation);
        }
        /* write ROI element */
        writeModelObject(roi);
    }

    /**
     * Write the OME-XML document for the given image.
     * @param imageId an image ID
     * @throws IOException if the document could not be written
     */
    public void writeImage(long imageId) throws IOException {
        /* determine what to write */
        final SetMultimap<Long, Long> imageAnnotationMap = containment.get(
                Maps.immutableEntry(ModelType.IMAGE, ModelType.ANNOTATION));
        final SetMultimap<Long, Long> imageRoiMap = containment.get(
                Maps.immutableEntry(ModelType.IMAGE, ModelType.ROI));
        final SetMultimap<Long, Long> roiAnnotationMap = containment.get(
                Maps.immutableEntry(ModelType.ROI, ModelType.ANNOTATION));
        final Set<Long> annotationIds = new HashSet<>();
        final Set<Long> roiIds = new HashSet<>();
        annotationIds.addAll(imageAnnotationMap.get(imageId));
        roiIds.addAll(imageRoiMap.get(imageId));
        for (final long roiId : roiIds) {
            annotationIds.addAll(roiAnnotationMap.get(roiId));
        }
        /* perform writes */
        System.out.print("assembling metadata for image " + imageId + "...");
        final DotBumper dots = new DotBumper(1024);
        writeImageElement(imageId);
        dots.bump();
        if (!annotationIds.isEmpty()) {
            out.write("<StructuredAnnotations>".getBytes());
            for (final long annotationId : annotationIds) {
                writeAnnotationElement(annotationId);
                dots.bump();
            }
            out.write("</StructuredAnnotations>".getBytes());
        }
        for (final long roiId : roiIds) {
            writeRoiElement(roiId);
            dots.bump();
        }
    }
}
