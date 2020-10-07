/*
 * Copyright (C) 2020 University of Dundee & Open Microscopy Environment.
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

package org.openmicroscopy.client.downloader.util;

import java.time.Duration;

/**
 * Utility methods related to chronology.
 * @author m.t.b.carroll@dundee.ac.uk
 */
public class TimeUtil {
    /**
     * End a progress line with a <q>done</q> message that may include a duration.
     * @param startTime the start time of the just-completed operation, as previously from {@link System#nanoTime()}
     */
    public static void printDone(long startTime) {
        final long endTime = System.nanoTime();
        final Duration duration = Duration.ofNanos(endTime - startTime);
        final long seconds = duration.getSeconds();
        System.out.print(" done");
        if (seconds > 0) {
            System.out.print(String.format(" (%,ds)", seconds));
        }
        System.out.println();
    }
}
