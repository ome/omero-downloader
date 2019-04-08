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

import java.util.function.Function;

import loci.formats.meta.DummyMetadata;

import ome.formats.model.UnitsFactory;
import ome.units.quantity.Time;
import ome.xml.meta.MetadataRoot;
import ome.xml.model.primitives.NonNegativeInteger;
import ome.xml.model.primitives.PositiveInteger;

import omero.RBool;
import omero.RDouble;
import omero.RInt;
import omero.RLong;
import omero.RString;
import omero.model.IObject;

/**
 * A common base for classes providing metadata about OMERO objects.
 * @author m.t.b.carroll@dundee.ac.uk
 * @author Josh Moore josh at glencoesoftware.com
 * @author Chris Allan callan at blackcat.ca
 * @author Curtis Rueden ctrueden at wisc.edu
 */
abstract class MetadataBase extends DummyMetadata {

    private static final MetadataRoot ROOT = new MetadataRoot() {};

    private final Function<IObject, String> lsids;

    protected MetadataBase(Function<IObject, String> lsids) {
        this.lsids = lsids;
    }

    protected String getLsid(IObject object) {
        return lsids.apply(object);
    }

    protected Boolean fromRType(RBool v) {
        return v == null ? null : v.getValue();
    }

    protected String fromRType(RString v) {
        return v == null ? null : v.getValue();
    }

    protected Double fromRType(RDouble v) {
        return v == null ? null : v.getValue();
    }

    protected Time fromRType(omero.model.Time v) {
        if (v == null) {
            return null;
        }
        return UnitsFactory.convertTime(v);
    }

    protected Integer fromRType(RInt v) {
        return v == null ? null : v.getValue();
    }

    protected Long fromRType(RLong v) {
        return v == null ? null : v.getValue();
    }

    protected PositiveInteger toPositiveInteger(RInt v) {
        Integer asInt = fromRType(v);
        return asInt != null ? new PositiveInteger(asInt) : null;
    }

    protected NonNegativeInteger toNonNegativeInteger(RInt v) {
        Integer asInt = fromRType(v);
        return asInt != null ? new NonNegativeInteger(asInt) : null;
    }

    @Override
    public MetadataRoot getRoot() {
        return ROOT;
    }
}
