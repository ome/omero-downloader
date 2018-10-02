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

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import ome.xml.model.ROI;

import omero.ServerError;
import omero.api.IConfigPrx;
import omero.api.IQueryPrx;
import omero.log.Logger;
import omero.log.SimpleLogger;
import omero.model.IObject;
import omero.model.Image;
import omero.model.Roi;
import omero.sys.ParametersI;

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
    private static final int BATCH_SIZE = 1024;

    private static DocumentBuilder DOCUMENT_BUILDER = null;

    static {
      try {
        DOCUMENT_BUILDER = DocumentBuilderFactory.newInstance().newDocumentBuilder();
      } catch (ParserConfigurationException pce) {
        LOGGER.fatal(pce, "cannot build XML documents");
        System.exit(3);
      }
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
     * Find the LSID of the given OMERO model object.
     * @param object an OMERO model object, hydrated with its update event
     * @return the LSID for that object
     */
    private String getLsid(IObject object) {
      String objectClass = object.getClass().getSimpleName();
      final int lastChar = objectClass.length() - 1;
      if (objectClass.charAt(lastChar) == 'I') {
        objectClass = objectClass.substring(0, lastChar);
      }
      final long objectId = object.getId().getValue();
      final long updateId = object.getDetails().getUpdateEvent().getId().getValue();
      return String.format(format, objectClass, objectId, updateId);
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
     * Query the server for the given images.
     * @param ids the IDs of the images to retrieve from OMERO
     * @return the images, hydrated sufficiently for conversion to XML
     * @throws ServerError if the images could not be retrieved
     */
    private List<Image> getImages(Collection<Long> ids) throws ServerError {
      final List<Image> images = new ArrayList<>(ids.size());
      for (final IObject result : iQuery.findAllByQuery("FROM Image i " +
          "LEFT OUTER JOIN FETCH i.pixels AS p " +
          "LEFT OUTER JOIN FETCH i.annotationLinks AS i_a_link " +
          "LEFT OUTER JOIN FETCH i_a_link.child AS i_a " +
          "LEFT OUTER JOIN FETCH i.rois AS r " +
          "LEFT OUTER JOIN FETCH p.channels AS c " +
          "LEFT OUTER JOIN FETCH i.instrument " +
          "LEFT OUTER JOIN FETCH p.pixelsType " +
          "LEFT OUTER JOIN FETCH p.planeInfo " +
          "LEFT OUTER JOIN FETCH c.logicalChannel " +
          "LEFT OUTER JOIN FETCH i.details.updateEvent " +
          "LEFT OUTER JOIN FETCH p.details.updateEvent " +
          "LEFT OUTER JOIN FETCH r.details.updateEvent " +
          "LEFT OUTER JOIN FETCH c.details.updateEvent " +
          "LEFT OUTER JOIN FETCH i_a.details.updateEvent " +
          "WHERE i.id IN (:ids)", new ParametersI().addIds(ids))) {
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
      final List<Roi> rois = new ArrayList<>(ids.size());
      for (final IObject result : iQuery.findAllByQuery("FROM Roi r " +
          "LEFT OUTER JOIN FETCH r.shapes AS s " +
          "LEFT OUTER JOIN FETCH r.annotationLinks AS r_a_link " +
          "LEFT OUTER JOIN FETCH r_a_link.child AS r_a " +
          "LEFT OUTER JOIN FETCH s.annotationLinks AS s_a_link " +
          "LEFT OUTER JOIN FETCH s_a_link.child AS s_a " +
          "LEFT OUTER JOIN FETCH r.details.updateEvent " +
          "LEFT OUTER JOIN FETCH s.details.updateEvent " +
          "LEFT OUTER JOIN FETCH r_a.details.updateEvent " +
          "LEFT OUTER JOIN FETCH s_a.details.updateEvent " +
          "WHERE r.id IN (:ids)", new ParametersI().addIds(ids))) {
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
        final File temporaryFile = new File(destination.getParentFile(), "temp");
        final Document document = DOCUMENT_BUILDER.newDocument();
        final Element xmlElement = modelObject.asXMLElement(document);
        document.appendChild(xmlElement);
        try (final OutputStream out = new FileOutputStream(temporaryFile)) {
            XMLTools.writeXML(out, document, false);
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
     * Write the given images into the given metadata store.
     * @param ids the IDs of the images to write
     * @param destination the metadata store into which to write the images
     * @throws ServerError if the ROIs could not be read
     */
    public void writeImages(List<Long> ids, MetadataStore destination) throws ServerError {
        for (final List<Long> idBatch : Lists.partition(ids, BATCH_SIZE)) {
            omeXmlService.convertMetadata(new ImageMetadata(lsidGetter, getImages(idBatch)), destination);
        }
    }

    /**
     * Write the given ROIs locally as XML.
     * @param ids the IDs of the ROIs to write
     * @param destinations how to determine the files into which to write the ROIs
     */
    public void writeRois(List<Long> ids, Function<Long, File> destinations) {
        final Map<Long, File> files = getFileMap(ids, destinations);
        if (files.isEmpty()) {
            System.out.println("written ROIs as XML, already have " + ids.size());
            return;
        }
        System.out.print("writing ROIs as XML, need " + ids.size());
        final int alreadyHave = ids.size() - files.size();
        if (alreadyHave > 0) {
            System.out.print(", already have " + alreadyHave);
        }
        System.out.print("..");
        try {
            for (final List<Long> idBatch : Lists.partition(ids, BATCH_SIZE)) {
                System.out.print('.');
                System.out.flush();
                for (final Roi roi : getRois(idBatch)) {
                    final OMEXMLMetadata xmlMeta = omeXmlService.createOMEXMLMetadata();
                    xmlMeta.createRoot();
                    omeXmlService.convertMetadata(new RoiMetadata(lsidGetter, Collections.singletonList(roi)), xmlMeta);
                    final OME omeElement = (OME) xmlMeta.getRoot();
                    final ROI roiElement = omeElement.getROI(0);
                    writeElement(roiElement, files.get(roi.getId().getValue()));
                }
            }
            System.out.println(" done");
        } catch (IOException | ServerError | ServiceException | TransformerException e) {
            System.out.println(" failed");
            LOGGER.error(e, "failed to obtain ROIs and write as XML");
        }
    }
}
