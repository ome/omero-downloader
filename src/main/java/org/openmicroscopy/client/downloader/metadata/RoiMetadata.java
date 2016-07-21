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

import com.google.common.base.Function;

import java.util.List;

import ome.formats.model.UnitsFactory;
import ome.units.quantity.Length;
import ome.xml.model.enums.EnumerationException;
import ome.xml.model.enums.FillRule;
import ome.xml.model.enums.FontFamily;
import ome.xml.model.enums.FontStyle;
import ome.xml.model.enums.Marker;
import ome.xml.model.primitives.Color;
import ome.xml.model.primitives.NonNegativeInteger;

import omero.RString;
import omero.model.Annotation;
import omero.model.Ellipse;
import omero.model.IObject;
import omero.model.Label;
import omero.model.Line;
import omero.model.Point;
import omero.model.Polygon;
import omero.model.Polyline;
import omero.model.Rectangle;
import omero.model.Roi;
import omero.model.Shape;

/**
 * An instance of {@link loci.formats.meta.MetadataRetrieve} that provides metadata about OMERO ROIs.
 * @author m.t.b.carroll@dundee.ac.uk
 * @author Josh Moore josh at glencoesoftware.com
 * @author Chris Allan callan at blackcat.ca
 * @author Curtis Rueden ctrueden at wisc.edu
 */
public class RoiMetadata extends MetadataBase {

    private final List<Roi> roiList;

    public RoiMetadata(Function<IObject, String> lsids, List<Roi> rois) {
        super(lsids);
        this.roiList = rois;
    }

    private <X extends Shape> X getShape(int ROIIndex, int shapeIndex, Class<X> expectedSubclass) {
        if (ROIIndex < 0 || shapeIndex < 0 || ROIIndex >= roiList.size()) {
            return null;
        }
        final Roi roi = roiList.get(ROIIndex);
        final List<Shape> shapes = roi.copyShapes();
        if (shapeIndex >= shapes.size()) {
            return null;
        }
        final Shape shape = shapes.get(shapeIndex);
        if (!expectedSubclass.isAssignableFrom(shape.getClass())) {
            return null;
        }
        return expectedSubclass.cast(shape);
    }

    @Override
    public int getROICount() {
        return roiList.size();
    }

    @Override
    public String getROIAnnotationRef(int ROIIndex, int annotationRefIndex) {
        if (ROIIndex < 0 || annotationRefIndex < 0 || ROIIndex >= roiList.size()) {
            return null;
        }
        final Roi roi = roiList.get(ROIIndex);
        final List<Annotation> annotations = roi.linkedAnnotationList();
        if (annotationRefIndex >= annotations.size()) {
            return null;
        }
        final Annotation annotation = annotations.get(annotationRefIndex);
        return getLsid(annotation);
    }

    @Override
    public int getROIAnnotationRefCount(int ROIIndex) {
        if (ROIIndex < 0 || ROIIndex >= roiList.size()) {
            return -1;
        }
        final Roi roi = roiList.get(ROIIndex);
        return roi.sizeOfAnnotationLinks();
    }

    @Override
    public String getROIDescription(int ROIIndex) {
        if (ROIIndex < 0 || ROIIndex >= roiList.size()) {
            return null;
        }
        final Roi roi = roiList.get(ROIIndex);
        return fromRType(roi.getDescription());
    }

    @Override
    public String getROIID(int ROIIndex) {
        if (ROIIndex < 0 || ROIIndex >= roiList.size()) {
            return null;
        }
        final Roi roi = roiList.get(ROIIndex);
        return getLsid(roi);
    }

    @Override
    public String getROIName(int ROIIndex) {
        if (ROIIndex < 0 || ROIIndex >= roiList.size()) {
            return null;
        }
        final Roi roi = roiList.get(ROIIndex);
        return fromRType(roi.getName());
    }

    @Override
    public int getShapeCount(int ROIIndex) {
        if (ROIIndex < 0 || ROIIndex >= roiList.size()) {
            return -1;
        }
        final Roi roi = roiList.get(ROIIndex);
        return roi.sizeOfShapes();
    }

    @Override
    public String getShapeType(int ROIIndex, int shapeIndex) {
        final Shape shape = getShape(ROIIndex, shapeIndex, Shape.class);
        if (shape == null) {
            return null;
        }
        Class<? extends Shape> shapeClass = null;
        Class<? extends Shape> currentClass = shape.getClass();
        while (currentClass != Shape.class) {
            shapeClass = currentClass;
            currentClass = currentClass.getSuperclass().asSubclass(Shape.class);
        }
        if (shapeClass == Rectangle.class) {
            return "Rectangle";
        } else {
            return shapeClass.getSimpleName();
        }
    }

    @Override
    public int getShapeAnnotationRefCount(int ROIIndex, int shapeIndex) {
        final Shape shape = getShape(ROIIndex, shapeIndex, Shape.class);
        if (shape == null) {
            return -1;
        }
        return shape.sizeOfAnnotationLinks();
    }

    private <X extends Shape> String getShapeAnnotationRef(int ROIIndex, int shapeIndex, int annotationRefIndex, Class<X> expectedSubclass) {
        if (annotationRefIndex < 0) {
            return null;
        }
        final X shape = getShape(ROIIndex, shapeIndex, expectedSubclass);
        if (shape == null) {
            return null;
        }
        final List<Annotation> annotations = shape.linkedAnnotationList();
        if (annotationRefIndex >= annotations.size()) {
            return null;
        }
        final Annotation annotation = annotations.get(annotationRefIndex);
        return getLsid(annotation);
    }

    private <X extends Shape> Color getShapeFillColor(int ROIIndex, int shapeIndex, Class<X> expectedSubclass) {
        final X shape = getShape(ROIIndex, shapeIndex, expectedSubclass);
        if (shape == null) {
            return null;
        }
        final Integer color = fromRType(shape.getFillColor());
        if (color == null) {
            return null;
        }
        return new Color(color);
    }

    private <X extends Shape> FillRule getShapeFillRule(int ROIIndex, int shapeIndex, Class<X> expectedSubclass) {
        final X shape = getShape(ROIIndex, shapeIndex, expectedSubclass);
        if (shape == null) {
            return null;
        }
        final String fillRuleName = fromRType(shape.getFillRule());
        if (fillRuleName == null) {
            return null;
        }
        final FillRule fillRule;
        try {
            fillRule = FillRule.fromString(fillRuleName);
        } catch (EnumerationException e) {
            return null;
        }
        return fillRule;
    }

    private <X extends Shape> FontFamily getShapeFontFamily(int ROIIndex, int shapeIndex, Class<X> expectedSubclass) {
        final X shape = getShape(ROIIndex, shapeIndex, expectedSubclass);
        if (shape == null) {
            return null;
        }
        final String fontFamilyName = fromRType(shape.getFontFamily());
        if (fontFamilyName == null) {
            return null;
        }
        final FontFamily fontFamily;
        try {
            fontFamily = FontFamily.fromString(fontFamilyName);
        } catch (EnumerationException e) {
            return null;
        }
        return fontFamily;
    }

    private <X extends Shape> Length getShapeFontSize(int ROIIndex, int shapeIndex, Class<X> expectedSubclass) {
        final X shape = getShape(ROIIndex, shapeIndex, expectedSubclass);
        if (shape == null) {
            return null;
        }
        return UnitsFactory.convertLength(shape.getFontSize());
    }

    private <X extends Shape> FontStyle getShapeFontStyle(int ROIIndex, int shapeIndex, Class<X> expectedSubclass) {
        final X shape = getShape(ROIIndex, shapeIndex, expectedSubclass);
        if (shape == null) {
            return null;
        }
        final String fontStyleName = fromRType(shape.getFontStyle());
        if (fontStyleName == null) {
            return null;
        }
        final FontStyle fontStyle;
        try {
            fontStyle = FontStyle.fromString(fontStyleName);
        } catch (EnumerationException e) {
            return null;
        }
        return fontStyle;
    }

    private <X extends Shape> String getShapeID(int ROIIndex, int shapeIndex, Class<X> expectedSubclass) {
        final X shape = getShape(ROIIndex, shapeIndex, expectedSubclass);
        if (shape == null) {
            return null;
        }
        return getLsid(shape);
    }

    private <X extends Shape> Boolean getShapeLocked(int ROIIndex, int shapeIndex, Class<X> expectedSubclass) {
        final X shape = getShape(ROIIndex, shapeIndex, expectedSubclass);
        if (shape == null) {
            return null;
        }
        return fromRType(shape.getLocked());
    }

    private <X extends Shape> Color getShapeStrokeColor(int ROIIndex, int shapeIndex, Class<X> expectedSubclass) {
        final X shape = getShape(ROIIndex, shapeIndex, expectedSubclass);
        if (shape == null) {
            return null;
        }
        final Integer color = fromRType(shape.getStrokeColor());
        if (color == null) {
            return null;
        }
        return new Color(color);
    }

    private <X extends Shape> String getShapeStrokeDashArray(int ROIIndex, int shapeIndex, Class<X> expectedSubclass) {
        final X shape = getShape(ROIIndex, shapeIndex, expectedSubclass);
        if (shape == null) {
            return null;
        }
        return fromRType(shape.getStrokeDashArray());
    }

    private <X extends Shape> Length getShapeStrokeWidth(int ROIIndex, int shapeIndex, Class<X> expectedSubclass) {
        final X shape = getShape(ROIIndex, shapeIndex, expectedSubclass);
        if (shape == null) {
            return null;
        }
        return UnitsFactory.convertLength(shape.getStrokeWidth());
    }

    private <X extends Shape> NonNegativeInteger getShapeTheC(int ROIIndex, int shapeIndex, Class<X> expectedSubclass) {
        final X shape = getShape(ROIIndex, shapeIndex, expectedSubclass);
        if (shape == null) {
            return null;
        }
        return toNonNegativeInteger(shape.getTheC());
    }

    private <X extends Shape> NonNegativeInteger getShapeTheT(int ROIIndex, int shapeIndex, Class<X> expectedSubclass) {
        final X shape = getShape(ROIIndex, shapeIndex, expectedSubclass);
        if (shape == null) {
            return null;
        }
        return toNonNegativeInteger(shape.getTheT());
    }

    private <X extends Shape> NonNegativeInteger getShapeTheZ(int ROIIndex, int shapeIndex, Class<X> expectedSubclass) {
        final X shape = getShape(ROIIndex, shapeIndex, expectedSubclass);
        if (shape == null) {
            return null;
        }
        return toNonNegativeInteger(shape.getTheZ());
    }

    @Override
    public String getEllipseAnnotationRef(int ROIIndex, int shapeIndex, int annotationRefIndex) {
        return getShapeAnnotationRef(ROIIndex, shapeIndex, annotationRefIndex, Ellipse.class);
    }

    @Override
    public Color getEllipseFillColor(int ROIIndex, int shapeIndex) {
        return getShapeFillColor(ROIIndex, shapeIndex, Ellipse.class);
    }

    @Override
    public FillRule getEllipseFillRule(int ROIIndex, int shapeIndex) {
        return getShapeFillRule(ROIIndex, shapeIndex, Ellipse.class);
    }

    @Override
    public FontFamily getEllipseFontFamily(int ROIIndex, int shapeIndex) {
        return getShapeFontFamily(ROIIndex, shapeIndex, Ellipse.class);
    }

    @Override
    public Length getEllipseFontSize(int ROIIndex, int shapeIndex) {
        return getShapeFontSize(ROIIndex, shapeIndex, Ellipse.class);
    }

    @Override
    public FontStyle getEllipseFontStyle(int ROIIndex, int shapeIndex) {
        return getShapeFontStyle(ROIIndex, shapeIndex, Ellipse.class);
    }

    @Override
    public String getEllipseID(int ROIIndex, int shapeIndex) {
        return getShapeID(ROIIndex, shapeIndex, Ellipse.class);
    }

    @Override
    public Boolean getEllipseLocked(int ROIIndex, int shapeIndex) {
        return getShapeLocked(ROIIndex, shapeIndex, Ellipse.class);
    }

    @Override
    public Color getEllipseStrokeColor(int ROIIndex, int shapeIndex) {
        return getShapeStrokeColor(ROIIndex, shapeIndex, Ellipse.class);
    }

    @Override
    public String getEllipseStrokeDashArray(int ROIIndex, int shapeIndex) {
        return getShapeStrokeDashArray(ROIIndex, shapeIndex, Ellipse.class);
    }

    @Override
    public Length getEllipseStrokeWidth(int ROIIndex, int shapeIndex) {
        return getShapeStrokeWidth(ROIIndex, shapeIndex, Ellipse.class);
    }

    @Override
    public NonNegativeInteger getEllipseTheC(int ROIIndex, int shapeIndex) {
        return getShapeTheC(ROIIndex, shapeIndex, Ellipse.class);
    }

    @Override
    public NonNegativeInteger getEllipseTheT(int ROIIndex, int shapeIndex) {
        return getShapeTheT(ROIIndex, shapeIndex, Ellipse.class);
    }

    @Override
    public NonNegativeInteger getEllipseTheZ(int ROIIndex, int shapeIndex) {
        return getShapeTheZ(ROIIndex, shapeIndex, Ellipse.class);
    }

    @Override
    public Double getEllipseRadiusX(int ROIIndex, int shapeIndex) {
        final Ellipse ellipse = getShape(ROIIndex, shapeIndex, Ellipse.class);
        if (ellipse == null) {
            return null;
        }
        return fromRType(ellipse.getRadiusX());
    }

    @Override
    public Double getEllipseRadiusY(int ROIIndex, int shapeIndex) {
        final Ellipse ellipse = getShape(ROIIndex, shapeIndex, Ellipse.class);
        if (ellipse == null) {
            return null;
        }
        return fromRType(ellipse.getRadiusY());
    }

    @Override
    public String getEllipseText(int ROIIndex, int shapeIndex) {
        final Ellipse ellipse = getShape(ROIIndex, shapeIndex, Ellipse.class);
        if (ellipse == null) {
            return null;
        }
        return fromRType(ellipse.getTextValue());
    }

    @Override
    public Double getEllipseX(int ROIIndex, int shapeIndex) {
        final Ellipse ellipse = getShape(ROIIndex, shapeIndex, Ellipse.class);
        if (ellipse == null) {
            return null;
        }
        return fromRType(ellipse.getX());
    }

    @Override
    public Double getEllipseY(int ROIIndex, int shapeIndex) {
        final Ellipse ellipse = getShape(ROIIndex, shapeIndex, Ellipse.class);
        if (ellipse == null) {
            return null;
        }
        return fromRType(ellipse.getY());
    }

    @Override
    public String getLabelAnnotationRef(int ROIIndex, int shapeIndex, int annotationRefIndex) {
        return getShapeAnnotationRef(ROIIndex, shapeIndex, annotationRefIndex, Label.class);
    }

    @Override
    public Color getLabelFillColor(int ROIIndex, int shapeIndex) {
        return getShapeFillColor(ROIIndex, shapeIndex, Label.class);
    }

    @Override
    public FillRule getLabelFillRule(int ROIIndex, int shapeIndex) {
        return getShapeFillRule(ROIIndex, shapeIndex, Label.class);
    }

    @Override
    public FontFamily getLabelFontFamily(int ROIIndex, int shapeIndex) {
        return getShapeFontFamily(ROIIndex, shapeIndex, Label.class);
    }

    @Override
    public Length getLabelFontSize(int ROIIndex, int shapeIndex) {
        return getShapeFontSize(ROIIndex, shapeIndex, Label.class);
    }

    @Override
    public FontStyle getLabelFontStyle(int ROIIndex, int shapeIndex) {
        return getShapeFontStyle(ROIIndex, shapeIndex, Label.class);
    }

    @Override
    public String getLabelID(int ROIIndex, int shapeIndex) {
        return getShapeID(ROIIndex, shapeIndex, Label.class);
    }

    @Override
    public Boolean getLabelLocked(int ROIIndex, int shapeIndex) {
        return getShapeLocked(ROIIndex, shapeIndex, Label.class);
    }

    @Override
    public Color getLabelStrokeColor(int ROIIndex, int shapeIndex) {
        return getShapeStrokeColor(ROIIndex, shapeIndex, Label.class);
    }

    @Override
    public String getLabelStrokeDashArray(int ROIIndex, int shapeIndex) {
        return getShapeStrokeDashArray(ROIIndex, shapeIndex, Label.class);
    }

    @Override
    public Length getLabelStrokeWidth(int ROIIndex, int shapeIndex) {
        return getShapeStrokeWidth(ROIIndex, shapeIndex, Label.class);
    }

    @Override
    public NonNegativeInteger getLabelTheC(int ROIIndex, int shapeIndex) {
        return getShapeTheC(ROIIndex, shapeIndex, Label.class);
    }

    @Override
    public NonNegativeInteger getLabelTheT(int ROIIndex, int shapeIndex) {
        return getShapeTheT(ROIIndex, shapeIndex, Label.class);
    }

    @Override
    public NonNegativeInteger getLabelTheZ(int ROIIndex, int shapeIndex) {
        return getShapeTheZ(ROIIndex, shapeIndex, Label.class);
    }

    @Override
    public String getLabelText(int ROIIndex, int shapeIndex) {
        final Label label = getShape(ROIIndex, shapeIndex, Label.class);
        if (label == null) {
            return null;
        }
        return fromRType(label.getTextValue());
    }

    @Override
    public Double getLabelX(int ROIIndex, int shapeIndex) {
        final Label label = getShape(ROIIndex, shapeIndex, Label.class);
        if (label == null) {
            return null;
        }
        return fromRType(label.getX());
    }

    @Override
    public Double getLabelY(int ROIIndex, int shapeIndex) {
        final Label label = getShape(ROIIndex, shapeIndex, Label.class);
        if (label == null) {
            return null;
        }
        return fromRType(label.getY());
    }

    @Override
    public String getLineAnnotationRef(int ROIIndex, int shapeIndex, int annotationRefIndex) {
        return getShapeAnnotationRef(ROIIndex, shapeIndex, annotationRefIndex, Line.class);
    }

    @Override
    public Color getLineFillColor(int ROIIndex, int shapeIndex) {
        return getShapeFillColor(ROIIndex, shapeIndex, Line.class);
    }

    @Override
    public FillRule getLineFillRule(int ROIIndex, int shapeIndex) {
        return getShapeFillRule(ROIIndex, shapeIndex, Line.class);
    }

    @Override
    public FontFamily getLineFontFamily(int ROIIndex, int shapeIndex) {
        return getShapeFontFamily(ROIIndex, shapeIndex, Line.class);
    }

    @Override
    public Length getLineFontSize(int ROIIndex, int shapeIndex) {
        return getShapeFontSize(ROIIndex, shapeIndex, Line.class);
    }

    @Override
    public FontStyle getLineFontStyle(int ROIIndex, int shapeIndex) {
        return getShapeFontStyle(ROIIndex, shapeIndex, Line.class);
    }

    @Override
    public String getLineID(int ROIIndex, int shapeIndex) {
        return getShapeID(ROIIndex, shapeIndex, Line.class);
    }

    @Override
    public Boolean getLineLocked(int ROIIndex, int shapeIndex) {
        return getShapeLocked(ROIIndex, shapeIndex, Line.class);
    }

    @Override
    public Color getLineStrokeColor(int ROIIndex, int shapeIndex) {
        return getShapeStrokeColor(ROIIndex, shapeIndex, Line.class);
    }

    @Override
    public String getLineStrokeDashArray(int ROIIndex, int shapeIndex) {
        return getShapeStrokeDashArray(ROIIndex, shapeIndex, Line.class);
    }

    @Override
    public Length getLineStrokeWidth(int ROIIndex, int shapeIndex) {
        return getShapeStrokeWidth(ROIIndex, shapeIndex, Line.class);
    }

    @Override
    public NonNegativeInteger getLineTheC(int ROIIndex, int shapeIndex) {
        return getShapeTheC(ROIIndex, shapeIndex, Line.class);
    }

    @Override
    public NonNegativeInteger getLineTheT(int ROIIndex, int shapeIndex) {
        return getShapeTheT(ROIIndex, shapeIndex, Line.class);
    }

    @Override
    public NonNegativeInteger getLineTheZ(int ROIIndex, int shapeIndex) {
        return getShapeTheZ(ROIIndex, shapeIndex, Line.class);
    }

    @Override
    public Marker getLineMarkerStart(int ROIIndex, int shapeIndex) {
        final Line line = getShape(ROIIndex, shapeIndex, Line.class);
        if (line == null) {
            return null;
        }
        final RString markerStart = line.getMarkerStart();
        if (markerStart == null) {
            return null;
        }
        return Marker.valueOf(markerStart.getValue());
    }

    @Override
    public Marker getLineMarkerEnd(int ROIIndex, int shapeIndex) {
        final Line line = getShape(ROIIndex, shapeIndex, Line.class);
        if (line == null) {
            return null;
        }
        final RString markerEnd = line.getMarkerEnd();
        if (markerEnd == null) {
            return null;
        }
        return Marker.valueOf(markerEnd.getValue());
    }

    @Override
    public String getLineText(int ROIIndex, int shapeIndex) {
        final Line line = getShape(ROIIndex, shapeIndex, Line.class);
        if (line == null) {
            return null;
        }
        return fromRType(line.getTextValue());
    }

    @Override
    public Double getLineX1(int ROIIndex, int shapeIndex) {
        final Line line = getShape(ROIIndex, shapeIndex, Line.class);
        if (line == null) {
            return null;
        }
        return fromRType(line.getX1());
    }

    @Override
    public Double getLineX2(int ROIIndex, int shapeIndex) {
        final Line line = getShape(ROIIndex, shapeIndex, Line.class);
        if (line == null) {
            return null;
        }
        return fromRType(line.getX2());
    }

    @Override
    public Double getLineY1(int ROIIndex, int shapeIndex) {
        final Line line = getShape(ROIIndex, shapeIndex, Line.class);
        if (line == null) {
            return null;
        }
        return fromRType(line.getY1());
    }

    @Override
    public Double getLineY2(int ROIIndex, int shapeIndex) {
        final Line line = getShape(ROIIndex, shapeIndex, Line.class);
        if (line == null) {
            return null;
        }
        return fromRType(line.getY2());
    }

    @Override
    public String getPointAnnotationRef(int ROIIndex, int shapeIndex, int annotationRefIndex) {
        return getShapeAnnotationRef(ROIIndex, shapeIndex, annotationRefIndex, Point.class);
    }

    @Override
    public Color getPointFillColor(int ROIIndex, int shapeIndex) {
        return getShapeFillColor(ROIIndex, shapeIndex, Point.class);
    }

    @Override
    public FillRule getPointFillRule(int ROIIndex, int shapeIndex) {
        return getShapeFillRule(ROIIndex, shapeIndex, Point.class);
    }

    @Override
    public FontFamily getPointFontFamily(int ROIIndex, int shapeIndex) {
        return getShapeFontFamily(ROIIndex, shapeIndex, Point.class);
    }

    @Override
    public Length getPointFontSize(int ROIIndex, int shapeIndex) {
        return getShapeFontSize(ROIIndex, shapeIndex, Point.class);
    }

    @Override
    public FontStyle getPointFontStyle(int ROIIndex, int shapeIndex) {
        return getShapeFontStyle(ROIIndex, shapeIndex, Point.class);
    }

    @Override
    public String getPointID(int ROIIndex, int shapeIndex) {
        return getShapeID(ROIIndex, shapeIndex, Point.class);
    }

    @Override
    public Boolean getPointLocked(int ROIIndex, int shapeIndex) {
        return getShapeLocked(ROIIndex, shapeIndex, Point.class);
    }

    @Override
    public Color getPointStrokeColor(int ROIIndex, int shapeIndex) {
        return getShapeStrokeColor(ROIIndex, shapeIndex, Point.class);
    }

    @Override
    public String getPointStrokeDashArray(int ROIIndex, int shapeIndex) {
        return getShapeStrokeDashArray(ROIIndex, shapeIndex, Point.class);
    }

    @Override
    public Length getPointStrokeWidth(int ROIIndex, int shapeIndex) {
        return getShapeStrokeWidth(ROIIndex, shapeIndex, Point.class);
    }

    @Override
    public NonNegativeInteger getPointTheC(int ROIIndex, int shapeIndex) {
        return getShapeTheC(ROIIndex, shapeIndex, Point.class);
    }

    @Override
    public NonNegativeInteger getPointTheT(int ROIIndex, int shapeIndex) {
        return getShapeTheT(ROIIndex, shapeIndex, Point.class);
    }

    @Override
    public NonNegativeInteger getPointTheZ(int ROIIndex, int shapeIndex) {
        return getShapeTheZ(ROIIndex, shapeIndex, Point.class);
    }

    @Override
    public String getPointText(int ROIIndex, int shapeIndex) {
        final Point point = getShape(ROIIndex, shapeIndex, Point.class);
        if (point == null) {
            return null;
        }
        return fromRType(point.getTextValue());
    }

    @Override
    public Double getPointX(int ROIIndex, int shapeIndex) {
        final Point point = getShape(ROIIndex, shapeIndex, Point.class);
        if (point == null) {
            return null;
        }
        return fromRType(point.getX());
    }

    @Override
    public Double getPointY(int ROIIndex, int shapeIndex) {
        final Point point = getShape(ROIIndex, shapeIndex, Point.class);
        if (point == null) {
            return null;
        }
        return fromRType(point.getY());
    }

    @Override
    public String getPolygonAnnotationRef(int ROIIndex, int shapeIndex, int annotationRefIndex) {
        return getShapeAnnotationRef(ROIIndex, shapeIndex, annotationRefIndex, Polygon.class);
    }

    @Override
    public Color getPolygonFillColor(int ROIIndex, int shapeIndex) {
        return getShapeFillColor(ROIIndex, shapeIndex, Polygon.class);
    }

    @Override
    public FillRule getPolygonFillRule(int ROIIndex, int shapeIndex) {
        return getShapeFillRule(ROIIndex, shapeIndex, Polygon.class);
    }

    @Override
    public FontFamily getPolygonFontFamily(int ROIIndex, int shapeIndex) {
        return getShapeFontFamily(ROIIndex, shapeIndex, Polygon.class);
    }

    @Override
    public Length getPolygonFontSize(int ROIIndex, int shapeIndex) {
        return getShapeFontSize(ROIIndex, shapeIndex, Polygon.class);
    }

    @Override
    public FontStyle getPolygonFontStyle(int ROIIndex, int shapeIndex) {
        return getShapeFontStyle(ROIIndex, shapeIndex, Polygon.class);
    }

    @Override
    public String getPolygonID(int ROIIndex, int shapeIndex) {
        return getShapeID(ROIIndex, shapeIndex, Polygon.class);
    }

    @Override
    public Boolean getPolygonLocked(int ROIIndex, int shapeIndex) {
        return getShapeLocked(ROIIndex, shapeIndex, Polygon.class);
    }

    @Override
    public Color getPolygonStrokeColor(int ROIIndex, int shapeIndex) {
        return getShapeStrokeColor(ROIIndex, shapeIndex, Polygon.class);
    }

    @Override
    public String getPolygonStrokeDashArray(int ROIIndex, int shapeIndex) {
        return getShapeStrokeDashArray(ROIIndex, shapeIndex, Polygon.class);
    }

    @Override
    public Length getPolygonStrokeWidth(int ROIIndex, int shapeIndex) {
        return getShapeStrokeWidth(ROIIndex, shapeIndex, Polygon.class);
    }

    @Override
    public NonNegativeInteger getPolygonTheC(int ROIIndex, int shapeIndex) {
        return getShapeTheC(ROIIndex, shapeIndex, Polygon.class);
    }

    @Override
    public NonNegativeInteger getPolygonTheT(int ROIIndex, int shapeIndex) {
        return getShapeTheT(ROIIndex, shapeIndex, Polygon.class);
    }

    @Override
    public NonNegativeInteger getPolygonTheZ(int ROIIndex, int shapeIndex) {
        return getShapeTheZ(ROIIndex, shapeIndex, Polygon.class);
    }

    @Override
    public String getPolygonPoints(int ROIIndex, int shapeIndex) {
        final Polygon polygon = getShape(ROIIndex, shapeIndex, Polygon.class);
        if (polygon == null) {
            return null;
        }
        return fromRType(polygon.getPoints());
    }

    @Override
    public String getPolygonText(int ROIIndex, int shapeIndex) {
        final Polygon polygon = getShape(ROIIndex, shapeIndex, Polygon.class);
        if (polygon == null) {
            return null;
        }
        return fromRType(polygon.getTextValue());
    }

    @Override
    public String getPolylineAnnotationRef(int ROIIndex, int shapeIndex, int annotationRefIndex) {
        return getShapeAnnotationRef(ROIIndex, shapeIndex, annotationRefIndex, Polyline.class);
    }

    @Override
    public Color getPolylineFillColor(int ROIIndex, int shapeIndex) {
        return getShapeFillColor(ROIIndex, shapeIndex, Polyline.class);
    }

    @Override
    public FillRule getPolylineFillRule(int ROIIndex, int shapeIndex) {
        return getShapeFillRule(ROIIndex, shapeIndex, Polyline.class);
    }

    @Override
    public FontFamily getPolylineFontFamily(int ROIIndex, int shapeIndex) {
        return getShapeFontFamily(ROIIndex, shapeIndex, Polyline.class);
    }

    @Override
    public Length getPolylineFontSize(int ROIIndex, int shapeIndex) {
        return getShapeFontSize(ROIIndex, shapeIndex, Polyline.class);
    }

    @Override
    public FontStyle getPolylineFontStyle(int ROIIndex, int shapeIndex) {
        return getShapeFontStyle(ROIIndex, shapeIndex, Polyline.class);
    }

    @Override
    public String getPolylineID(int ROIIndex, int shapeIndex) {
        return getShapeID(ROIIndex, shapeIndex, Polyline.class);
    }

    @Override
    public Boolean getPolylineLocked(int ROIIndex, int shapeIndex) {
        return getShapeLocked(ROIIndex, shapeIndex, Polyline.class);
    }

    @Override
    public Color getPolylineStrokeColor(int ROIIndex, int shapeIndex) {
        return getShapeStrokeColor(ROIIndex, shapeIndex, Polyline.class);
    }

    @Override
    public String getPolylineStrokeDashArray(int ROIIndex, int shapeIndex) {
        return getShapeStrokeDashArray(ROIIndex, shapeIndex, Polyline.class);
    }

    @Override
    public Length getPolylineStrokeWidth(int ROIIndex, int shapeIndex) {
        return getShapeStrokeWidth(ROIIndex, shapeIndex, Polyline.class);
    }

    @Override
    public NonNegativeInteger getPolylineTheC(int ROIIndex, int shapeIndex) {
        return getShapeTheC(ROIIndex, shapeIndex, Polyline.class);
    }

    @Override
    public NonNegativeInteger getPolylineTheT(int ROIIndex, int shapeIndex) {
        return getShapeTheT(ROIIndex, shapeIndex, Polyline.class);
    }

    @Override
    public NonNegativeInteger getPolylineTheZ(int ROIIndex, int shapeIndex) {
        return getShapeTheZ(ROIIndex, shapeIndex, Polyline.class);
    }

    @Override
    public Marker getPolylineMarkerStart(int ROIIndex, int shapeIndex) {
        final Polyline polyline = getShape(ROIIndex, shapeIndex, Polyline.class);
        if (polyline == null) {
            return null;
        }
        final RString markerStart = polyline.getMarkerStart();
        if (markerStart == null) {
            return null;
        }
        return Marker.valueOf(markerStart.getValue());
    }

    @Override
    public Marker getPolylineMarkerEnd(int ROIIndex, int shapeIndex) {
        final Polyline polyline = getShape(ROIIndex, shapeIndex, Polyline.class);
        if (polyline == null) {
            return null;
        }
        final RString markerEnd = polyline.getMarkerEnd();
        if (markerEnd == null) {
            return null;
        }
        return Marker.valueOf(markerEnd.getValue());
    }

    @Override
    public String getPolylinePoints(int ROIIndex, int shapeIndex) {
        final Polyline polyline = getShape(ROIIndex, shapeIndex, Polyline.class);
        if (polyline == null) {
            return null;
        }
        return fromRType(polyline.getPoints());
    }

    @Override
    public String getPolylineText(int ROIIndex, int shapeIndex) {
        final Polyline polyline = getShape(ROIIndex, shapeIndex, Polyline.class);
        if (polyline == null) {
            return null;
        }
        return fromRType(polyline.getTextValue());
    }

    @Override
    public String getRectangleAnnotationRef(int ROIIndex, int shapeIndex, int annotationRefIndex) {
        return getShapeAnnotationRef(ROIIndex, shapeIndex, annotationRefIndex, Rectangle.class);
    }

    @Override
    public Color getRectangleFillColor(int ROIIndex, int shapeIndex) {
        return getShapeFillColor(ROIIndex, shapeIndex, Rectangle.class);
    }

    @Override
    public FillRule getRectangleFillRule(int ROIIndex, int shapeIndex) {
        return getShapeFillRule(ROIIndex, shapeIndex, Rectangle.class);
    }

    @Override
    public FontFamily getRectangleFontFamily(int ROIIndex, int shapeIndex) {
        return getShapeFontFamily(ROIIndex, shapeIndex, Rectangle.class);
    }

    @Override
    public Length getRectangleFontSize(int ROIIndex, int shapeIndex) {
        return getShapeFontSize(ROIIndex, shapeIndex, Rectangle.class);
    }

    @Override
    public FontStyle getRectangleFontStyle(int ROIIndex, int shapeIndex) {
        return getShapeFontStyle(ROIIndex, shapeIndex, Rectangle.class);
    }

    @Override
    public String getRectangleID(int ROIIndex, int shapeIndex) {
        return getShapeID(ROIIndex, shapeIndex, Rectangle.class);
    }

    @Override
    public Boolean getRectangleLocked(int ROIIndex, int shapeIndex) {
        return getShapeLocked(ROIIndex, shapeIndex, Rectangle.class);
    }

    @Override
    public Color getRectangleStrokeColor(int ROIIndex, int shapeIndex) {
        return getShapeStrokeColor(ROIIndex, shapeIndex, Rectangle.class);
    }

    @Override
    public String getRectangleStrokeDashArray(int ROIIndex, int shapeIndex) {
        return getShapeStrokeDashArray(ROIIndex, shapeIndex, Rectangle.class);
    }

    @Override
    public Length getRectangleStrokeWidth(int ROIIndex, int shapeIndex) {
        return getShapeStrokeWidth(ROIIndex, shapeIndex, Rectangle.class);
    }

    @Override
    public NonNegativeInteger getRectangleTheC(int ROIIndex, int shapeIndex) {
        return getShapeTheC(ROIIndex, shapeIndex, Rectangle.class);
    }

    @Override
    public NonNegativeInteger getRectangleTheT(int ROIIndex, int shapeIndex) {
        return getShapeTheT(ROIIndex, shapeIndex, Rectangle.class);
    }

    @Override
    public NonNegativeInteger getRectangleTheZ(int ROIIndex, int shapeIndex) {
        return getShapeTheZ(ROIIndex, shapeIndex, Rectangle.class);
    }

    @Override
    public String getRectangleText(int ROIIndex, int shapeIndex) {
        final Rectangle rectangle = getShape(ROIIndex, shapeIndex, Rectangle.class);
        if (rectangle == null) {
            return null;
        }
        return fromRType(rectangle.getTextValue());
    }

    @Override
    public Double getRectangleHeight(int ROIIndex, int shapeIndex) {
        final Rectangle rectangle = getShape(ROIIndex, shapeIndex, Rectangle.class);
        if (rectangle == null) {
            return null;
        }
        return fromRType(rectangle.getHeight());
    }

    @Override
    public Double getRectangleWidth(int ROIIndex, int shapeIndex) {
        final Rectangle rectangle = getShape(ROIIndex, shapeIndex, Rectangle.class);
        if (rectangle == null) {
            return null;
        }
        return fromRType(rectangle.getWidth());
    }

    @Override
    public Double getRectangleX(int ROIIndex, int shapeIndex) {
        final Rectangle rectangle = getShape(ROIIndex, shapeIndex, Rectangle.class);
        if (rectangle == null) {
            return null;
        }
        return fromRType(rectangle.getX());
    }

    @Override
    public Double getRectangleY(int ROIIndex, int shapeIndex) {
        final Rectangle rectangle = getShape(ROIIndex, shapeIndex, Rectangle.class);
        if (rectangle == null) {
            return null;
        }
        return fromRType(rectangle.getY());
    }
}
