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

import java.util.ArrayList;
import java.util.List;

import ome.xml.model.primitives.Timestamp;

import omero.model.Annotation;
import omero.model.BooleanAnnotation;
import omero.model.CommentAnnotation;
import omero.model.DoubleAnnotation;
import omero.model.LongAnnotation;
import omero.model.TagAnnotation;
import omero.model.TermAnnotation;
import omero.model.TimestampAnnotation;
import omero.model.XmlAnnotation;
import omero.model.IObject;

import com.google.common.base.Function;

import org.joda.time.Instant;

/**
 * An instance of {@link loci.formats.meta.MetadataRetrieve} that provides metadata about OMERO annotations.
 * @author m.t.b.carroll@dundee.ac.uk
 * @author Josh Moore josh at glencoesoftware.com
 * @author Chris Allan callan at blackcat.ca
 * @author Curtis Rueden ctrueden at wisc.edu
 */
public class AnnotationMetadata extends MetadataBase {

    private final List<BooleanAnnotation> booleanAnnotationList = new ArrayList<>();
    private final List<CommentAnnotation> commentAnnotationList = new ArrayList<>();
    private final List<DoubleAnnotation> doubleAnnotationList = new ArrayList<>();
    private final List<LongAnnotation> longAnnotationList = new ArrayList<>();
    private final List<TagAnnotation> tagAnnotationList = new ArrayList<>();
    private final List<TermAnnotation> termAnnotationList = new ArrayList<>();
    private final List<TimestampAnnotation> timestampAnnotationList = new ArrayList<>();
    private final List<XmlAnnotation> xmlAnnotationList = new ArrayList<>();

    public AnnotationMetadata(Function<IObject, String> lsids, List<Annotation> annotations) {
        super(lsids);
        for (final Annotation annotation : annotations) {
            if (annotation instanceof BooleanAnnotation) {
                booleanAnnotationList.add((BooleanAnnotation) annotation);
            } else if (annotation instanceof CommentAnnotation) {
                commentAnnotationList.add((CommentAnnotation) annotation);
            } else if (annotation instanceof DoubleAnnotation) {
                doubleAnnotationList.add((DoubleAnnotation) annotation);
            } else if (annotation instanceof LongAnnotation) {
                longAnnotationList.add((LongAnnotation) annotation);
            } else if (annotation instanceof TagAnnotation) {
                tagAnnotationList.add((TagAnnotation) annotation);
            } else if (annotation instanceof TermAnnotation) {
                termAnnotationList.add((TermAnnotation) annotation);
            } else if (annotation instanceof TimestampAnnotation) {
                timestampAnnotationList.add((TimestampAnnotation) annotation);
            } else if (annotation instanceof XmlAnnotation) {
                xmlAnnotationList.add((XmlAnnotation) annotation);
            }
        }
    }

    private <T extends Annotation> T getAnnotation(Class<T> klass, int index)
    {
        try
        {
            if (klass.equals(XmlAnnotation.class))
            {
                return (T) xmlAnnotationList.get(index);
            }
            else if (klass.equals(LongAnnotation.class))
            {
                return (T) longAnnotationList.get(index);
            }
            else if (klass.equals(BooleanAnnotation.class))
            {
                return (T) booleanAnnotationList.get(index);
            }
            else if (klass.equals(DoubleAnnotation.class))
            {
                return (T) doubleAnnotationList.get(index);
            }
            else if (klass.equals(CommentAnnotation.class))
            {
                return (T) commentAnnotationList.get(index);
            }
            else if (klass.equals(TimestampAnnotation.class))
            {
                return (T) timestampAnnotationList.get(index);
            }
            else if (klass.equals(TagAnnotation.class))
            {
                return (T) tagAnnotationList.get(index);
            }
            else if (klass.equals(TermAnnotation.class))
            {
                return (T) termAnnotationList.get(index);
            }
            else
            {
                return null;
            }
        }
        catch (ArrayIndexOutOfBoundsException e)
        {
            return null;
        }
    }

    private String getAnnotationDescription(
            Class<? extends Annotation> klass, int index)
    {
        Annotation o = getAnnotation(klass, index);
        return o != null? fromRType(o.getDescription()) : null;
    }

    private String getAnnotationID(
            Class<? extends Annotation> klass, int index)
    {
        Annotation o = getAnnotation(klass, index);
        return o != null? getLsid(o) : null;
    }

    private String getAnnotationNamespace(
            Class<? extends Annotation> klass, int index)
    {
        Annotation o = getAnnotation(klass, index);
        return o != null? fromRType(o.getNs()) : null;
    }

    @Override
    public int getXMLAnnotationCount()
    {
        return xmlAnnotationList.size();
    }

    @Override
    public String getXMLAnnotationDescription(int XMLAnnotationIndex)
    {
        return getAnnotationDescription(
                XmlAnnotation.class, XMLAnnotationIndex);
    }

    @Override
    public String getXMLAnnotationID(int XMLAnnotationIndex)
    {
        return getAnnotationID(XmlAnnotation.class, XMLAnnotationIndex);
    }

    @Override
    public String getXMLAnnotationNamespace(int XMLAnnotationIndex)
    {
        return getAnnotationNamespace(XmlAnnotation.class, XMLAnnotationIndex);
    }

    @Override
    public String getXMLAnnotationValue(int XMLAnnotationIndex)
    {
        XmlAnnotation o = getAnnotation(
                XmlAnnotation.class, XMLAnnotationIndex);
        return o != null? fromRType(o.getTextValue()) : null;
    }

    @Override
    public int getLongAnnotationCount()
    {
        return longAnnotationList.size();
    }

    @Override
    public String getLongAnnotationDescription(int longAnnotationIndex)
    {
        return getAnnotationDescription(
                LongAnnotation.class, longAnnotationIndex);
    }

    @Override
    public String getLongAnnotationID(int longAnnotationIndex)
    {
        return getAnnotationID(LongAnnotation.class, longAnnotationIndex);
    }

    @Override
    public String getLongAnnotationNamespace(int longAnnotationIndex)
    {
        return getAnnotationNamespace(
                LongAnnotation.class, longAnnotationIndex);
    }

    @Override
    public Long getLongAnnotationValue(int longAnnotationIndex)
    {
        LongAnnotation o = getAnnotation(
                LongAnnotation.class, longAnnotationIndex);
        return o != null? fromRType(o.getLongValue()) : null;
    }

    @Override
    public int getBooleanAnnotationCount()
    {
        return booleanAnnotationList.size();
    }

    @Override
    public String getBooleanAnnotationDescription(int booleanAnnotationIndex)
    {
        return getAnnotationDescription(
                BooleanAnnotation.class, booleanAnnotationIndex);
    }

    @Override
    public String getBooleanAnnotationID(int booleanAnnotationIndex)
    {
        return getAnnotationID(BooleanAnnotation.class, booleanAnnotationIndex);
    }

    @Override
    public String getBooleanAnnotationNamespace(int booleanAnnotationIndex)
    {
        return getAnnotationNamespace(
                BooleanAnnotation.class, booleanAnnotationIndex);
    }

    @Override
    public Boolean getBooleanAnnotationValue(int booleanAnnotationIndex)
    {
        BooleanAnnotation o = getAnnotation(
                BooleanAnnotation.class, booleanAnnotationIndex);
        return o != null? fromRType(o.getBoolValue()) : null;
    }

    @Override
    public int getDoubleAnnotationCount()
    {
        return doubleAnnotationList.size();
    }

    @Override
    public String getDoubleAnnotationDescription(int doubleAnnotationIndex)
    {
        return getAnnotationDescription(
                DoubleAnnotation.class, doubleAnnotationIndex);
    }

    @Override
    public String getDoubleAnnotationID(int doubleAnnotationIndex)
    {
        return getAnnotationID(DoubleAnnotation.class, doubleAnnotationIndex);
    }

    @Override
    public String getDoubleAnnotationNamespace(int doubleAnnotationIndex)
    {
        return getAnnotationNamespace(
                DoubleAnnotation.class, doubleAnnotationIndex);
    }

    @Override
    public Double getDoubleAnnotationValue(int doubleAnnotationIndex)
    {
        DoubleAnnotation o = getAnnotation(
                DoubleAnnotation.class, doubleAnnotationIndex);
        return o != null? fromRType(o.getDoubleValue()) : null;
    }

    @Override
    public int getCommentAnnotationCount()
    {
        return commentAnnotationList.size();
    }

    @Override
    public String getCommentAnnotationDescription(int commentAnnotationIndex)
    {
        return getAnnotationDescription(
                CommentAnnotation.class, commentAnnotationIndex);
    }

    @Override
    public String getCommentAnnotationID(int commentAnnotationIndex)
    {
        return getAnnotationID(CommentAnnotation.class, commentAnnotationIndex);
    }

    @Override
    public String getCommentAnnotationNamespace(int commentAnnotationIndex)
    {
        return getAnnotationNamespace(
                CommentAnnotation.class, commentAnnotationIndex);
    }

    @Override
    public String getCommentAnnotationValue(int commentAnnotationIndex)
    {
        CommentAnnotation o = getAnnotation(
                CommentAnnotation.class, commentAnnotationIndex);
        return o != null? fromRType(o.getTextValue()) : null;
    }

    @Override
    public int getTimestampAnnotationCount()
    {
        return timestampAnnotationList.size();
    }

    @Override
    public String getTimestampAnnotationDescription(int timestampAnnotationIndex)
    {
        return getAnnotationDescription(
                TimestampAnnotation.class, timestampAnnotationIndex);
    }

    @Override
    public String getTimestampAnnotationID(int timestampAnnotationIndex)
    {
        return getAnnotationID(
                TimestampAnnotation.class, timestampAnnotationIndex);
    }

    @Override
    public String getTimestampAnnotationNamespace(int timestampAnnotationIndex)
    {
        return getAnnotationNamespace(
                TimestampAnnotation.class, timestampAnnotationIndex);
    }

    @Override
    public Timestamp getTimestampAnnotationValue(int timestampAnnotationIndex)
    {
        TimestampAnnotation o = getAnnotation(
                TimestampAnnotation.class, timestampAnnotationIndex);
        return o != null? new Timestamp(new Instant(o.getTimeValue().getValue())) : null;
    }

    @Override
    public int getTagAnnotationCount()
    {
        return tagAnnotationList.size();
    }

    @Override
    public String getTagAnnotationDescription(int tagAnnotationIndex)
    {
        return getAnnotationDescription(
                TagAnnotation.class, tagAnnotationIndex);
    }

    @Override
    public String getTagAnnotationID(int tagAnnotationIndex)
    {
        return getAnnotationID(TagAnnotation.class, tagAnnotationIndex);
    }

    @Override
    public String getTagAnnotationNamespace(int tagAnnotationIndex)
    {
        return getAnnotationNamespace(TagAnnotation.class, tagAnnotationIndex);
    }

    @Override
    public String getTagAnnotationValue(int tagAnnotationIndex)
    {
        TagAnnotation o = getAnnotation(
                TagAnnotation.class, tagAnnotationIndex);
        return o != null? fromRType(o.getTextValue()) : null;    }

    @Override
    public int getTermAnnotationCount()
    {
        return termAnnotationList.size();
    }

    @Override
    public String getTermAnnotationDescription(int termAnnotationIndex)
    {
        return getAnnotationDescription(
                TermAnnotation.class, termAnnotationIndex);
    }

    @Override
    public String getTermAnnotationID(int termAnnotationIndex)
    {
        return getAnnotationID(TermAnnotation.class, termAnnotationIndex);
    }

    @Override
    public String getTermAnnotationNamespace(int termAnnotationIndex)
    {
        return getAnnotationNamespace(
                TermAnnotation.class, termAnnotationIndex);
    }

    @Override
    public String getTermAnnotationValue(int termAnnotationIndex)
    {
        TermAnnotation o = getAnnotation(
                TermAnnotation.class, termAnnotationIndex);
        return o != null? fromRType(o.getTermValue()) : null;
    }
}
