/*
 * Copyright (C) 2009-2016 Glencoe Software, Inc., University of Dundee
 * and Open Microscopy Environment. All rights reserved.
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

package org.openmicroscopy.client.downloader.metadata;

import java.util.List;
import java.util.function.Function;

import static ome.formats.model.UnitsFactory.convertLength;

import ome.units.quantity.Time;
import ome.xml.model.enums.AcquisitionMode;
import ome.xml.model.enums.ContrastMethod;
import ome.xml.model.enums.DimensionOrder;
import ome.xml.model.enums.EnumerationException;
import ome.xml.model.enums.IlluminationType;
import ome.xml.model.enums.PixelType;
import ome.xml.model.primitives.Color;
import ome.xml.model.primitives.NonNegativeInteger;
import ome.xml.model.primitives.PositiveInteger;
import ome.xml.model.primitives.Timestamp;

import omero.RTime;
import omero.model.Annotation;
import omero.model.Channel;
import omero.model.IObject;
import omero.model.Image;
import omero.model.Pixels;
import omero.model.PlaneInfo;
import omero.model.Roi;

import org.joda.time.Instant;

/**
 * An instance of {@link loci.formats.meta.MetadataRetrieve} that provides metadata about OMERO images.
 * Differs from {@link ome.services.blitz.impl.OmeroMetadata} in that {@link #getPixelsDimensionOrder(int)}
 * reflects {@link loci.formats.in.TiffReader}'s ImageJ convention for 5D data for ease of exporting plain TIFFs.
 * @author m.t.b.carroll@dundee.ac.uk
 * @author Josh Moore josh at glencoesoftware.com
 * @author Chris Allan callan at blackcat.ca
 * @author Curtis Rueden ctrueden at wisc.edu
 */
public class ImageMetadata extends MetadataBase {

    private final List<Image> imageList;

    public ImageMetadata(Function<IObject, String> lsids, List<Image> images) {
        super(lsids);
        this.imageList = images;
    }

    private Image _getImage(int imageIndex) {
        try
        {
            return imageList.get(imageIndex);
        }
        catch (ArrayIndexOutOfBoundsException e)
        {
            return null;
        }
    }

    @Override
    public Timestamp getImageAcquisitionDate(int imageIndex)
    {
        Image o = _getImage(imageIndex);
        if (o != null) {
            final RTime acquisitionDate = o.getAcquisitionDate();
            if (acquisitionDate != null) {
                return new Timestamp(new Instant(acquisitionDate.getValue()));
            }
        }
        return null;
    }

    @Override
    public String getImageAnnotationRef(int imageIndex, int annotationRefIndex)
    {
        Image o = _getImage(imageIndex);
        if (o == null)
        {
            return null;
        }
        try
        {
            Annotation annotation =
                o.linkedAnnotationList().get(annotationRefIndex);
            return getLsid(annotation);
        }
        catch (ArrayIndexOutOfBoundsException e)
        {
            return null;
        }
    }

    @Override
    public int getImageAnnotationRefCount(int imageIndex)
    {
        Image o = _getImage(imageIndex);
        if (o == null)
        {
            return -1;
        }
        return o.sizeOfAnnotationLinks();
    }

    @Override
    public int getImageCount()
    {
        return imageList.size();
    }

    @Override
    public String getImageDescription(int imageIndex)
    {
        Image o = _getImage(imageIndex);
        return o != null? fromRType(o.getDescription()) : null;
    }

    @Override
    public String getImageID(int imageIndex)
    {
        Image o = _getImage(imageIndex);
        return o != null? getLsid(o) : null;
    }

    @Override
    public String getImageName(int imageIndex)
    {
        Image o = _getImage(imageIndex);
        return o != null? fromRType(o.getName()) : null;
    }

    @Override
    public String getImageROIRef(int imageIndex, int ROIRefIndex) {
        if (imageIndex < 0 || ROIRefIndex < 0 || imageIndex >= imageList.size()) {
            return null;
        }
        final Image image = imageList.get(imageIndex);
        final List<Roi> rois = image.copyRois();
        if (ROIRefIndex >= rois.size()) {
            return null;
        }
        final Roi roi = rois.get(ROIRefIndex);
        return getLsid(roi);
    }

    @Override
    public int getImageROIRefCount(int imageIndex) {
        if (imageIndex < 0 || imageIndex >= imageList.size()) {
            return -1;
        }
        final Image image = imageList.get(imageIndex);
        return image.sizeOfRois();
    }

    @Override
    public Boolean getPixelsBinDataBigEndian(int imageIndex, int binDataIndex)
    {
        return true;
    }

    @Override
    public DimensionOrder getPixelsDimensionOrder(int imageIndex)
    {
        return DimensionOrder.XYCZT;
    }

    @Override
    public String getPixelsID(int imageIndex)
    {
        Image o = _getImage(imageIndex);
        return o != null? getLsid(o.getPrimaryPixels()) : null;
    }

    @Override
    public ome.units.quantity.Length getPixelsPhysicalSizeX(int imageIndex)
    {
        Image o = _getImage(imageIndex);
        return o != null? convertLength(
                o.getPrimaryPixels().getPhysicalSizeX()) : null;
    }

    @Override
    public ome.units.quantity.Length getPixelsPhysicalSizeY(int imageIndex)
    {
        Image o = _getImage(imageIndex);
        return o != null? convertLength(
                o.getPrimaryPixels().getPhysicalSizeY()) : null;
    }

    @Override
    public ome.units.quantity.Length getPixelsPhysicalSizeZ(int imageIndex)
    {
        Image o = _getImage(imageIndex);
        return o != null? convertLength(
                o.getPrimaryPixels().getPhysicalSizeZ()) : null;
    }

    @Override
    public PositiveInteger getPixelsSizeC(int imageIndex)
    {
        Image o = _getImage(imageIndex);
        return o != null? toPositiveInteger(
                o.getPrimaryPixels().getSizeC()) : null;
    }

    @Override
    public PositiveInteger getPixelsSizeT(int imageIndex)
    {
        Image o = _getImage(imageIndex);
        return o != null? toPositiveInteger(
                o.getPrimaryPixels().getSizeT()) : null;
    }

    @Override
    public PositiveInteger getPixelsSizeX(int imageIndex)
    {
        Image o = _getImage(imageIndex);
        return o != null? toPositiveInteger(
                o.getPrimaryPixels().getSizeX()) : null;
    }

    @Override
    public PositiveInteger getPixelsSizeY(int imageIndex)
    {
        Image o = _getImage(imageIndex);
        return o != null? toPositiveInteger(
                o.getPrimaryPixels().getSizeY()) : null;
    }

    @Override
    public PositiveInteger getPixelsSizeZ(int imageIndex)
    {
        Image o = _getImage(imageIndex);
        return o != null? toPositiveInteger(
                o.getPrimaryPixels().getSizeZ()) : null;
    }

    @Override
    public Time getPixelsTimeIncrement(int imageIndex)
    {
        Image o = _getImage(imageIndex);
        return o != null? fromRType(
                o.getPrimaryPixels().getTimeIncrement()) : null;
    }

    @Override
    public PixelType getPixelsType(int imageIndex)
    {
        Image o = _getImage(imageIndex);
        if (o == null)
        {
            return null;
        }
        omero.model.PixelsType e = o.getPrimaryPixels().getPixelsType();
        try
        {
            return e != null?
                    PixelType.fromString(fromRType(e.getValue()))
                    : null;
        }
        catch (EnumerationException ex)
        {
            return null;
        }
    }

    private Channel getChannel(int imageIndex, int channelIndex)
    {
        Image i = _getImage(imageIndex);
        if (i == null)
        {
            return null;
        }
        Pixels p = i.getPrimaryPixels();
        if (p == null)
        {
            return null;
        }
        try
        {
            return p.getChannel(channelIndex);
        }
        catch (ArrayIndexOutOfBoundsException e)
        {
            return null;
        }
    }

    @Override
    public AcquisitionMode getChannelAcquisitionMode(int imageIndex,
            int channelIndex)
    {
        Channel o = getChannel(imageIndex, channelIndex);
        if (o == null)
        {
            return null;
        }
        omero.model.AcquisitionMode e = o.getLogicalChannel().getMode();
        try
        {
            return e != null?
                    AcquisitionMode.fromString(fromRType(e.getValue()))
                    : null;
        }
        catch (EnumerationException ex)
        {
            return null;
        }
    }

    @Override
    public Color getChannelColor(int imageIndex, int channelIndex)
    {
        Channel o = getChannel(imageIndex, channelIndex);
        if (o == null)
        {
            return null;
        }
        try
        {
            return new Color(
                    fromRType(o.getRed()), fromRType(o.getGreen()),
                    fromRType(o.getBlue()), fromRType(o.getAlpha()));
        }
        catch (NullPointerException e)
        {
            return null;
        }
    }

    @Override
    public ContrastMethod getChannelContrastMethod(int imageIndex,
            int channelIndex)
    {
        Channel o = getChannel(imageIndex, channelIndex);
        if (o == null)
        {
            return null;
        }
        omero.model.ContrastMethod e = o.getLogicalChannel().getContrastMethod();
        try
        {
            return e != null?
                    ContrastMethod.fromString(fromRType(e.getValue()))
                    : null;
        }
        catch (EnumerationException ex)
        {
            return null;
        }
    }

    @Override
    public int getChannelCount(int imageIndex)
    {
        Image o = _getImage(imageIndex);
        if (o == null)
        {
            return -1;
        }
        return o.getPrimaryPixels().sizeOfChannels();
    }

    @Override
    public ome.units.quantity.Length getChannelEmissionWavelength(int imageIndex,
            int channelIndex)
    {
        Channel o = getChannel(imageIndex, channelIndex);
        return convertLength(o.getLogicalChannel().getEmissionWave());
    }

    @Override
    public ome.units.quantity.Length getChannelExcitationWavelength(int imageIndex,
            int channelIndex)
    {
        Channel o = getChannel(imageIndex, channelIndex);
        return convertLength(o.getLogicalChannel().getExcitationWave());
    }

    @Override
    public String getChannelFluor(int imageIndex, int channelIndex)
    {
        Channel o = getChannel(imageIndex, channelIndex);
        return o != null? fromRType(o.getLogicalChannel().getFluor()) : null;
    }

    @Override
    public String getChannelID(int imageIndex, int channelIndex)
    {
        Channel o = getChannel(imageIndex, channelIndex);
        return o != null? getLsid(o) : null;
    }

    @Override
    public IlluminationType getChannelIlluminationType(int imageIndex,
            int channelIndex)
    {
        Channel o = getChannel(imageIndex, channelIndex);
        if (o == null)
        {
            return null;
        }
        omero.model.Illumination e = o.getLogicalChannel().getIllumination();
        try
        {
            return e != null?
                    IlluminationType.fromString(fromRType(e.getValue()))
                    : null;
        }
        catch (EnumerationException ex)
        {
            return null;
        }
    }

    @Override
    public String getChannelName(int imageIndex, int channelIndex)
    {
        Channel o = getChannel(imageIndex, channelIndex);
        return o != null? fromRType(o.getLogicalChannel().getName()) : null;
    }

    @Override
    public Double getChannelNDFilter(int imageIndex, int channelIndex)
    {
        Channel o = getChannel(imageIndex, channelIndex);
        return o != null? fromRType(o.getLogicalChannel().getNdFilter()) : null;
    }

    @Override
    public ome.units.quantity.Length getChannelPinholeSize(int imageIndex, int channelIndex)
    {
        Channel o = getChannel(imageIndex, channelIndex);
        return o != null? convertLength(
                o.getLogicalChannel().getPinHoleSize()) : null;
    }

    @Override
    public Integer getChannelPockelCellSetting(int imageIndex, int channelIndex)
    {
        Channel o = getChannel(imageIndex, channelIndex);
        return o != null? fromRType(
                o.getLogicalChannel().getPockelCellSetting()) : null;
    }

    @Override
    public PositiveInteger getChannelSamplesPerPixel(int imageIndex,
            int channelIndex)
    {
        return new PositiveInteger(1);
    }

    private PlaneInfo getPlane(int imageIndex, int planeIndex)
    {
        Image i = _getImage(imageIndex);
        if (i == null)
        {
            return null;
        }
        Pixels p = i.getPrimaryPixels();
        if (p == null)
        {
            return null;
        }
        try
        {
            return p.copyPlaneInfo().get(planeIndex);
        }
        catch (ArrayIndexOutOfBoundsException e)
        {
            return null;
        }
    }

    @Override
    public int getPlaneCount(int imageIndex)
    {
        Image o = _getImage(imageIndex);
        return o == null? 0 : o.getPrimaryPixels().sizeOfPlaneInfo();
    }

    @Override
    public Time getPlaneDeltaT(int imageIndex, int planeIndex)
    {
        PlaneInfo o = getPlane(imageIndex, planeIndex);
        return o != null? fromRType(o.getDeltaT()) : null;
    }

    @Override
    public Time getPlaneExposureTime(int imageIndex, int planeIndex)
    {
        PlaneInfo o = getPlane(imageIndex, planeIndex);
        return o != null? fromRType(o.getExposureTime()) : null;
    }

    @Override
    public ome.units.quantity.Length getPlanePositionX(int imageIndex, int planeIndex)
    {
        PlaneInfo o = getPlane(imageIndex, planeIndex);
        return o != null? convertLength(o.getPositionX()) : null;
    }

    @Override
    public ome.units.quantity.Length getPlanePositionY(int imageIndex, int planeIndex)
    {
        PlaneInfo o = getPlane(imageIndex, planeIndex);
        return o != null? convertLength(o.getPositionY()) : null;
    }

    @Override
    public ome.units.quantity.Length getPlanePositionZ(int imageIndex, int planeIndex)
    {
        PlaneInfo o = getPlane(imageIndex, planeIndex);
        return o != null? convertLength(o.getPositionZ()) : null;
    }

    @Override
    public NonNegativeInteger getPlaneTheC(int imageIndex, int planeIndex)
    {
        PlaneInfo o = getPlane(imageIndex, planeIndex);
        return toNonNegativeInteger(o.getTheC());
    }

    @Override
    public NonNegativeInteger getPlaneTheT(int imageIndex, int planeIndex)
    {
        PlaneInfo o = getPlane(imageIndex, planeIndex);
        return toNonNegativeInteger(o.getTheT());
    }

    @Override
    public NonNegativeInteger getPlaneTheZ(int imageIndex, int planeIndex)
    {
        PlaneInfo o = getPlane(imageIndex, planeIndex);
        return toNonNegativeInteger(o.getTheZ());
    }
}
