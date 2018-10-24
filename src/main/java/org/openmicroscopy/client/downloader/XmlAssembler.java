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
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stream.StreamResult;

import loci.common.xml.XMLTools;

import ome.xml.model.OME;
import ome.xml.model.OMEModel;
import ome.xml.model.OMEModelObject;
import ome.xml.model.ROIRef;
import ome.xml.model.Reference;
import ome.xml.model.enums.EnumerationException;

import omero.log.Logger;
import omero.log.SimpleLogger;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;

import org.w3c.dom.Document;
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

    private static DocumentBuilder DOCUMENT_BUILDER = null;

    private static final OME OME_OBJECT;

    private static byte[] OME_XML_HEADER;
    private static byte[] OME_XML_FOOTER;

    private static int HEADER_SKIP;
    private static int FOOTER_SKIP;

    /**
     * Set up the document builder and note the XML header and footer to use for OME documents.
     */
    static {
        try {
            DOCUMENT_BUILDER = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        } catch (ParserConfigurationException pce) {
            LOGGER.fatal(pce, "cannot build XML documents");
            System.exit(3);
        }
        final Document documentDOM = DOCUMENT_BUILDER.newDocument();
        OME_OBJECT = new ome.xml.model.OME();
        OME_OBJECT.setCreator(OME_XML_CREATOR);
        final Node omeElement = OME_OBJECT.asXMLElement(documentDOM);
        final String textContent = XmlAssembler.class.getName();
        final Node textNode = documentDOM.createTextNode(textContent);
        documentDOM.appendChild(omeElement);
        omeElement.appendChild(textNode);
        final StringWriter documentWriter = new StringWriter();
        try {
            XMLTools.writeXML(new StreamResult(documentWriter), documentDOM, true);
        } catch (TransformerException te) {
            LOGGER.fatal(te, "cannot write XML documents");
            System.exit(3);
        }
        final String documentText = documentWriter.toString();
        final int declarationLength = documentText.indexOf('>') + 1;
        final int footerLength = documentText.length() - documentText.lastIndexOf('<');
        final int headerLength = documentText.length() - declarationLength - textContent.length() - footerLength;
        OME_XML_HEADER = documentText.substring(0, declarationLength + headerLength).getBytes(StandardCharsets.UTF_8);
        OME_XML_FOOTER = documentText.substring(documentText.length() - footerLength).getBytes(StandardCharsets.UTF_8);
        HEADER_SKIP = headerLength;
        FOOTER_SKIP = footerLength;
    }

    private final Map<Map.Entry<ModelType, ModelType>, SetMultimap<Long, Long>> containment;
    private final Map<Map.Entry<ModelType, Long>, String> lsids = new HashMap<>();
    private final Function<Map.Entry<ModelType, Long>, File> xmlFiles;
    private final OutputStream out;

    /**
     * Create a new XML assembler for OME metadata export.
     * The writer methods are used to provide content from already-exported XML fragments in between the {@code OME}-element
     * open-and-close provided by this class.
     * @param containment the parent-child relationships to use in adding {@link Reference} elements
     * @param metadataFiles a locator for files containing XML fragments
     * @param destination where to write the assembled XML
     * @throws IOException if the destination was not writable
     */
    public XmlAssembler(Map<Map.Entry<ModelType, ModelType>, SetMultimap<Long, Long>> containment,
            Function<Map.Entry<ModelType, Long>, File> metadataFiles, OutputStream destination) throws IOException {
        this.containment = containment;
        this.xmlFiles = metadataFiles;
        this.out = destination;
        this.out.write(OME_XML_HEADER);
    }

    /**
     * Write the given model object as XML.
     * @param object a model object
     * @throws IOException if the XML writing failed
     */
    private void writeModelObject(OMEModelObject object) throws IOException {
        /* To omit xmlns attribute actually wraps the object in an OME element that is then stripped. */
        final Document document = DOCUMENT_BUILDER.newDocument();
        final Node omeElement = OME_OBJECT.asXMLElement(document);
        final Node givenElement = object.asXMLElement(document);
        document.appendChild(omeElement);
        omeElement.appendChild(givenElement);
        final OutputStream truncater = new TruncatingOutputStream(HEADER_SKIP, FOOTER_SKIP, out);
        try {
            XMLTools.writeXML(truncater, document, false);
        } catch (TransformerException te) {
            LOGGER.fatal(te, "cannot write XML fragment");
            System.exit(3);
        }
    }

    @Override
    public void close() throws IOException {
        out.write(OME_XML_FOOTER);
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
     * Write the XML {@code Image} element for the given image.
     * @param imageId an image ID
     * @throws IOException if the image's metadata file could not be read or the {@code Image} element could not be written
     */
    public void writeImage(long imageId) throws IOException {
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
        SetMultimap<Long, Long> children;
        children = containment.get(Maps.immutableEntry(ModelType.IMAGE, ModelType.ROI));
        if (children != null) {
            for (final long roiId : children.get(imageId)) {
                final ome.xml.model.ROI roi = new ome.xml.model.ROI();
                final Reference ref = new ROIRef();
                roi.setID(getLsid(ModelType.ROI, roiId));
                image.link(ref, roi);
            }
        }
        /* write Image element */
        writeModelObject(image);
    }

    /**
     * Write the XML {@code ROI} element for the given ROI.
     * @param roiId a ROI ID
     * @throws IOException if the ROI's metadata file could not be read or the {@code ROI} element could not be written
     */
    public void writeRoi(long roiId) throws IOException {
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
        /* write ROI element */
        writeModelObject(roi);
    }
}
