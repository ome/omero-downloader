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

import com.google.common.primitives.Ints;

import java.awt.Dimension;
import java.io.File;
import java.io.IOException;

import loci.formats.FormatException;
import loci.formats.FormatWriter;
import loci.formats.out.TiffWriter;
import loci.formats.tiff.IFD;

import omero.ServerError;
import omero.api.RawPixelsStorePrx;
import omero.log.Logger;
import omero.log.SimpleLogger;
import omero.model.Pixels;

/**
 * Download a remote image's tiles into a local file and assemble them into a TIFF file.
 * @author m.t.b.carroll@dundee.ac.uk
 */
public class LocalPixels {

    private static final Logger LOGGER = new SimpleLogger();
    private static final int TILES_PER_DOT = 64;  // affects write flush interval

    private final RawPixelsStorePrx rps;
    private final File tileFile;
    private final Dimension tileSize;
    private final TileIterator tiles;

    /**
     * Construct a new image download and assembly job.
     * @param pixels the pixels instance for the image whose pixel data is to be downloaded
     * @param tileFile the local file in which to store downloaded tiles from the remote image's pixel data
     * @param pixelsStore the pixels store from which to obtain pixel data from the server
     * @throws ServerError if the pixel data could not be initialized on the server
     */
    public LocalPixels(Pixels pixels, File tileFile, RawPixelsStorePrx pixelsStore) throws ServerError {
        this.tileFile = tileFile;

        this.rps = pixelsStore;
        this.rps.setPixelsId(pixels.getId().getValue(), false);

        final int[] tileSizeArray = rps.getTileSize();
        this.tileSize = new Dimension(tileSizeArray[0], tileSizeArray[1]);

        tiles = new TileIterator(pixels.getSizeX().getValue(), pixels.getSizeY().getValue(), pixels.getSizeZ().getValue(),
                pixels.getSizeC().getValue(), pixels.getSizeT().getValue(),
                tileSize.width, tileSize.height,
                pixels.getDimensionOrder().getValue().getValue());
    }

    /**
     * Download the tiles for the image from the OMERO server.
     * @throws IOException if the writing of the tiles failed
     * @throws ServerError if the pixel data could not be retrieved
     */
    public void downloadTiles() throws IOException, ServerError {
        int tileCount = 0;
        for (final TileIterator.Tile tile : tiles) {
            tileCount++;
        }
        final int[] tileSizes = new int[tileCount];
        final int headerSize = (tileSizes.length + 1) * FileIO.INT_BYTES;
        final FileIO tileIO = new FileIO(tileFile, true);
        if (tileFile.length() < headerSize) {
            System.out.print("commencing");
            tileIO.writeInt(tileSizes.length);
            for (int tileNumber = 0; tileNumber < tileSizes.length; tileNumber++) {
                tileIO.writeInt(tileSizes[tileNumber]);
            }
        } else {
            final int recordedCount = tileIO.readInt();
            if (recordedCount != tileSizes.length) {
                System.out.println("tile count in file is " + recordedCount);
                LOGGER.warn(tileFile, "ignoring stale tiles file");
                tileIO.close();
                tileFile.delete();
                downloadTiles();
            }
            System.out.print("resuming");
            for (int tileNumber = 0; tileNumber < tileSizes.length; tileNumber++) {
                tileSizes[tileNumber] = tileIO.readInt();
            }
        }
        System.out.print(" download of pixels " + rps.getPixelsId() + "...");
        System.out.flush();
        int tileNumber = 0;
        for (final TileIterator.Tile tile : tiles) {
            final long from = tileIO.tell();
            final long to;
            if (tileSizes[tileNumber] > 0) {
                to = from + tileSizes[tileNumber];
            } else {
                tileIO.writeBytes(rps.getTile(tile.z, tile.c, tile.t, tile.x, tile.y, tile.w, tile.h));
                to = tileIO.tell();
                tileSizes[tileNumber] = Ints.checkedCast(to - from);
                tileIO.seek((tileNumber + 1) * FileIO.INT_BYTES);
                tileIO.writeInt(tileSizes[tileNumber]);
                if ((tileNumber + 1) % TILES_PER_DOT == 0) {
                    tileIO.flush();
                    System.out.print('.');
                    System.out.flush();
                }
            }
            tileIO.seek(to);
            tileNumber++;
        }
        tileIO.close();
        System.out.println(" done");
    }

    /**
     * Assemble the downloaded tiles into an image file. Closes the writer when it is finished.
     * @param writer the Bio-Formats writer into which to save the tiles
     * @throws FormatException if the tile writing failed
     * @throws IOException if the tile reading or writing failed
     * @throws ServerError if the pixels ID could not be obtained
     */
    public void writeTiles(FormatWriter writer) throws FormatException, IOException, ServerError {
        System.out.print("assembling pixels " + rps.getPixelsId() + "..");
        System.out.flush();
        final FileIO tileIO = new FileIO(tileFile, false);
        final int[] tileSizes = new int[tileIO.readInt()];
        for (int tileNumber = 0; tileNumber < tileSizes.length; tileNumber++) {
            tileSizes[tileNumber] = tileIO.readInt();
        }
        int tileNumber = 0;
        int planeIndex = -1;
        TileIterator.Tile previousTile = null;
        for (final TileIterator.Tile tile : tiles) {
            if (!tile.isSamePlane(previousTile)) {
                previousTile = tile;
                planeIndex++;
            }
            final byte[] pixels = tileIO.readBytes(tileSizes[tileNumber]);
            if (writer instanceof TiffWriter) {
              /* TiffWriter requires IFD for tiled writing */
              final IFD ifd = new IFD();
              ifd.put(IFD.TILE_WIDTH, tileSize.width);
              ifd.put(IFD.TILE_LENGTH, tileSize.height);
              ((TiffWriter) writer).saveBytes(planeIndex, pixels, ifd, tile.x, tile.y, tile.w, tile.h);
            } else {
              writer.saveBytes(planeIndex, pixels, tile.x, tile.y, tile.w, tile.h);
            }
            if (tileNumber % TILES_PER_DOT == 0) {
                System.out.print('.');
                System.out.flush();
            }
            tileNumber++;
        }
        writer.close();
        tileIO.close();
        System.out.println(" done");
    }
}
