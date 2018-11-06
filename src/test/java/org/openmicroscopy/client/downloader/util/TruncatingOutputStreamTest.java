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

package org.openmicroscopy.client.downloader.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link TruncatingOutputStream}.
 * @author m.t.b.carroll@dundee.ac.uk
 */
public class TruncatingOutputStreamTest {

    private static final String TEXT = "Lorem ipsum dolor sit amet, " +
            "consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.";

    private static enum WriteMethod { ONE, SOME, ALL };

    /**
     * Return an array of the Cartesian product of the given arrays.
     * @param elements the arrays from which to choose one element each
     * @return the array of selections from each array
     */
    private static Object[][] getCartesianProduct(Object[]... elements) {
        int size = 1;
        for (final Object[] element : elements) {
            size *= element.length;
        }
        final Object[][] cases = new Object[size][];
        final int[] indices = new int[elements.length];
        for (int caseNumber = 0; caseNumber < cases.length; caseNumber++) {
            cases[caseNumber] = new Object[indices.length];
            for (int elementNumber = 0; elementNumber < elements.length; elementNumber++) {
                cases[caseNumber][elementNumber] = elements[elementNumber][indices[elementNumber]];
            }
            for (int elementNumber = 0; elementNumber < elements.length; elementNumber++) {
                if (++indices[elementNumber] == elements[elementNumber].length) {
                    indices[elementNumber] = 0;
                } else {
                    break;
                }
            }
        }
        return cases;
    }

    /**
     * Test that the truncating output stream objects to skipping a negative number of bytes but not otherwise.
     * @param initialSkip how many bytes to skip from the start
     * @param finalSkip how many bytes to skip from the end
     */
    @Test(dataProvider = "skips")
    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    public void testConstructorCheckSkips(int initialSkip, int finalSkip) {
        final boolean isExpectSuccess = initialSkip >= 0 && finalSkip >= 0;
        try {
            new TruncatingOutputStream(initialSkip, finalSkip, System.out);
            Assert.assertTrue(isExpectSuccess);
        } catch (IllegalArgumentException iae) {
            Assert.assertFalse(isExpectSuccess);
        }
    }

    /**
     * @return test cases for {@link #testConstructorCheckSkips(int, int)}
     */
    @DataProvider(name = "skips")
    public Object[][] getNumberPairsAroundZero() {
        final Object[] numbers = new Integer[] {-2, -1, 0, 1, 2};
        return getCartesianProduct(numbers, numbers);
    }

    /**
     * Test one of various combinations of where to write how much from the buffer against various sizes of truncation.
     * @param initialSkip how many bytes to skip from the start
     * @param finalSkip how many bytes to skip from the end
     * @param bufferSize how large a buffer to use
     * @param offset the start offset in the buffer
     * @param length the number of bytes to write
     * @throws IOException unexpected
     */
    @Test(dataProvider = "boundaries")
    public void testWriteBoundaries(int initialSkip, int finalSkip, int bufferSize, int offset, int length) throws IOException {
        final boolean isExpectSuccess = offset + length <= bufferSize;
        final String expectedString;
        if (isExpectSuccess) {
            final String written = TEXT.substring(offset, offset + length);
            if (initialSkip + finalSkip > written.length()) {
                expectedString = "";
            } else {
                expectedString = written.substring(initialSkip, written.length() - finalSkip);
            }
        } else {
            expectedString = null;
        }
        final byte[] buffer = TEXT.substring(0, bufferSize).getBytes();
        Assert.assertEquals(buffer.length, bufferSize);
        final OutputStream destination = new ByteArrayOutputStream();
        final OutputStream truncator = new TruncatingOutputStream(initialSkip, finalSkip, destination);
        try {
            truncator.write(buffer, offset, length);
            Assert.assertTrue(isExpectSuccess);
            Assert.assertEquals(destination.toString(), expectedString);
        } catch (IllegalArgumentException iae) {
            Assert.assertFalse(isExpectSuccess);
            Assert.assertTrue(destination.toString().isEmpty());
        }
    }

    /**
     * @return test cases for {@link #testWriteBoundaries(int, int, int, int, int)}
     */
    @DataProvider(name = "boundaries")
    public Object[][] getBuffersAndBoundaries() {
        return getCartesianProduct(
                new Integer[] {0, 1, 3, 8},
                new Integer[] {0, 1, 3, 8},
                new Integer[] {0, 11, 16, 21},
                new Integer[] {0, 1, 5, 12},
                new Integer[] {0, 1, 5, 12});
    }

    /**
     * Test one of various sequences of writes using various {@code write} methods.
     * @param writes the sequence of write methods to use
     * @throws IOException unexpected
     */
    @Test(dataProvider = "writes")
    @SuppressWarnings("NonPublicExported")
    public void testVariousWriteMethods(WriteMethod... writes) throws IOException {
        final StringBuilder expectedString = new StringBuilder();
        final int initialSkip = 4;
        final int finalSkip = 7;
        final byte[] buffer = TEXT.getBytes();
        final OutputStream destination = new ByteArrayOutputStream();
        final OutputStream truncator = new TruncatingOutputStream(initialSkip, finalSkip, destination);
        int position = 0;
        for (final WriteMethod write : writes) {
            switch (write) {
                case ONE:
                    truncator.write(buffer[position]);
                    expectedString.append(TEXT.charAt(position));
                    position++;
                    break;
                case SOME:
                    truncator.write(buffer, position, 5);
                    expectedString.append(TEXT.substring(position, position + 5));
                    position += 5;
                    break;
                case ALL:
                    final byte[] smallBuffer = new byte[6];
                    System.arraycopy(buffer, position, smallBuffer, 0, smallBuffer.length);
                    truncator.write(smallBuffer);
                    expectedString.append(TEXT.substring(position, position + smallBuffer.length));
                    position += smallBuffer.length;
                    break;
            }
        }
        if (initialSkip + finalSkip > expectedString.length()) {
            Assert.assertTrue(destination.toString().isEmpty());
        } else {
            expectedString.delete(0, initialSkip);
            expectedString.setLength(expectedString.length() - finalSkip);
            Assert.assertEquals(destination.toString(), expectedString.toString());
        }
    }

    /**
     * @return test cases for
     * {@link #testVariousWriteMethods(org.openmicroscopy.client.downloader.util.TruncatingOutputStreamTest.WriteMethod[])}
     */
    @DataProvider(name = "writes")
    public Object[][] getWriteMethods() {
        return getCartesianProduct(WriteMethod.values(), WriteMethod.values(), WriteMethod.values());
    }
}
