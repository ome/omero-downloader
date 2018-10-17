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

import omero.model.Annotation;
import omero.model.Dataset;
import omero.model.Experiment;
import omero.model.Fileset;
import omero.model.Folder;
import omero.model.IObject;
import omero.model.Image;
import omero.model.Instrument;
import omero.model.Plate;
import omero.model.Project;
import omero.model.Roi;
import omero.model.Screen;

/**
 * The principal OMERO model types that must be managed for download.
 * @author m.t.b.carroll@dundee.ac.uk
 */
public enum ModelType {

    /* OME Model top-level entities */
    EXPERIMENT(Experiment.class), PROJECT(Project.class), DATASET(Dataset.class), FOLDER(Folder.class),
    SCREEN(Screen.class), PLATE(Plate.class), IMAGE(Image.class), INSTRUMENT(Instrument.class), ANNOTATION(Annotation.class),
    ROI(Roi.class),
    /* additional OMERO top-level entities */
    FILESET(Fileset.class);

    private final Class<? extends IObject> omeroType;

    private ModelType(Class<? extends IObject> omeroType) {
        this.omeroType = omeroType;
    }

    @Override
    public String toString() {
        return omeroType.getSimpleName();
    }

    /**
     * Get the enumeration value for the given OMERO model object class.
     * @param modelClass an OMERO model object class
     * @return the corresponding enumeration value
     */
    public static ModelType getEnumValueFor(Class<? extends IObject> modelClass) {
        for (final ModelType object : ModelType.values()) {
            if (object.omeroType.isAssignableFrom(modelClass)) {
                return object;
            }
        }
        throw new IllegalArgumentException("unknown class: " + modelClass);
    }
}
