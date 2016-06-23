/*
 * Copyright (C) 2016 University of Dundee & Open Microscopy Environment.
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

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterate over an image's tiles in the <em>x</em>, <em>y</em> plane respecting the dimension ordering.
 * @author m.t.b.carroll@dundee.ac.uk
 */
class TileIterator implements Iterable<TileIterator.Tile> {

    /**
     * A fully-specified tile.
     */
    class Tile {

        /**
         * The <em>x</em> coordinate of this tile's left side.
         */
        public final int x;

        /**
         * The <em>y</em> coordinate of this tile's top side.
         */
        public final int y;

        /**
         * The <em>z</em> coordinate of this tile.
         */
        public final int z;

        /**
         * The channel of this tile.
         */
        public final int c;

        /**
         * The timepoint of this tile.
         */
        public final int t;

        /**
         * The width of this tile, spanning {@link #x}.
         */
        public final int w;

        /**
         * The height of this tile, spanning {@link #y}.
         */
        public final int h;

        /**
         * Only the {@link TileIterator} may create tiles.
         * @param x the <em>x</em> coordinate of the tile's left side
         * @param y the <em>y</em> coordinate of the tile's top side
         * @param z the <em>z</em> coordinate of the tile
         * @param c the channel of the tile
         * @param t the timepoint of the tile
         * @param w the width of the tile, spanning <em>x</em>
         * @param h the height of the tile, spanning <em>y</em>
         */
        private Tile (int x, int y, int z, int c, int t, int w, int h) {
            this.x = x;
            this.y = y;
            this.z = z;
            this.c = c;
            this.t = t;
            this.w = w;
            this.h = h;
        }

        /**
         * Check if a given tile is on the same <em>x</em>, <em>y</em> plane as this tile.
         * @param other a tile, may be {@code null}
         * @return if the given tile is on this tile's plane
         */
        public boolean isSamePlane(Tile other) {
            return other != null && z == other.z && c == other.c && t == other.t;
        }

        @Override
        public String toString() {
            return "Tile(x=" + x + ", y=" + y + ", z=" + z + ", c=" + c + ", t=" + t + "; " + w + "×" + h + ")";
        }
    }

    private final int sizeX, sizeY, sizeZ, sizeT, sizeC, tileX, tileY;
    private final char[] ordering;

    /**
     * Construct a new iterator factory. Each iterator covers the given dimensions in the given order.
     * @param sizeX the width of each <em>x</em>, <em>y</em> plane
     * @param sizeY the height of each <em>x</em>, <em>y</em> plane
     * @param sizeZ the number of planes in the <em>z</em>-stack
     * @param sizeC the number of channels in the image
     * @param sizeT the number of timepoints
     * @param tileX the maximum width of the tiles
     * @param tileY the maximum height of the tiles
     * @param ordering the ordering of the dimensions, may include {@code X}, {@code Y}, {@code Z}, {@code C}, {@code T}
     */
    TileIterator(int sizeX, int sizeY, int sizeZ, int sizeC, int sizeT, int tileX, int tileY, String ordering) {
        this.sizeX = sizeX;
        this.sizeY = sizeY;
        this.sizeZ = sizeZ;
        this.sizeC = sizeC;
        this.sizeT = sizeT;
        this.tileX = tileX;
        this.tileY = tileY;
        this.ordering = ordering.toCharArray();
    }

    /**
     * Given a tile, return the next tile in the iteration.
     * @param from the previous tile
     * @return the next tile
     */
    private Tile getNextTile(Tile from) {
        int x = from.x;
        int y = from.y;
        int z = from.z;
        int c = from.c;
        int t = from.t;
        int dimension = 0;
        INCREMENT: while (true) {
            if (dimension == ordering.length) {
                return null;
            }
            switch (ordering[dimension]) {
                case 'X':
                    x += from.w;
                    if (x < sizeX) {
                        break INCREMENT;
                    } else {
                        x = 0;
                        break;
                    }
                case 'Y':
                    y += from.h;
                    if (y < sizeY) {
                        break INCREMENT;
                    } else {
                        y = 0;
                        break;
                    }
                case 'Z':
                    z++;
                    if (z < sizeZ) {
                        break INCREMENT;
                    } else {
                        z = 0;
                        break;
                    }
                case 'C':
                    c++;
                    if (c < sizeC) {
                        break INCREMENT;
                    } else {
                        c = 0;
                        break;
                    }
                case 'T':
                    t++;
                    if (t < sizeT) {
                        break INCREMENT;
                    } else {
                        t = 0;
                        break;
                    }
                default:
                    throw new IllegalStateException("unknown dimension: " + ordering[dimension]);
            }
            dimension++;
        }
        return new Tile(x, y, z, c, t, Math.min(sizeX - x, tileX), Math.min(sizeY - y, tileY));
    }

    @Override
    public Iterator<Tile> iterator() {
        return new Iterator<Tile>() {
            Tile prevTile = null;
            Tile nextTile = new Tile(0, 0, 0, 0, 0, Math.min(sizeX, tileX), Math.min(sizeY, tileY));

            @Override
            public boolean hasNext() {
                if (nextTile == null) {
                    nextTile = getNextTile(prevTile);
                }
                return nextTile != null;
            }

            @Override
            public Tile next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                prevTile = nextTile;
                nextTile = null;
                return prevTile;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
