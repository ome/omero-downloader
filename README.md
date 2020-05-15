# Summary

An OMERO client for downloading data in bulk from an OMERO.server.
The `-h` option prints a brief summary of command-line options.


# Install

Binaries can be downloaded from the
[releases](https://github.com/ome/omero-downloader/releases) page.

NB: Use OMERO.downloader `0.1.5` to work with OMERO.server `5.4.x`.
Use OMERO.downloader `0.2.x` to work with OMERO.server `5.5.x` and later `5.x`.

For development, from the source repository `mvn` builds and packages the downloader.


# User Guide

In addition to the instructions below, OMERO downloader is described in the
[download section](https://omero-guides.readthedocs.io/en/latest/download/docs/download.html)
of the OMERO user guide.


# Caveat

OMERO.downloader has not yet seen much use. One should therefore expect
both bugs and breaking changes. However, it is hoped that even in its
early state there are use cases for which it offers significant help.


# Storing downloads locally

Choose or create a target directory for download. This is used in
the `-b` option below. In general one should use a separate download
directory for each OMERO server from which one fetches data.


# Downloading imported image files

```
./download.sh -b /tmp/repo -s <server host> -u <user name> -w <password> -f binary Image:<image ID>
```

downloads an image's binary files into the scratch directory. To include
companion files use `-f binary,companion`.

Repeating a download resumes any interrupted files and skips files that
are already present.

Within `/tmp/repo/Image/` it may seem inconvenient to have each image's
downloaded files sorted into separate `Binary` and `Companion`
directories. However, these are simply symbolic links that can be
followed to find the files together within the `Repository` directory.
When binary and companion files should be used together the `realpath`
utility from GNU coreutils can be helpful, e.g.,
```
showinf -autoscale `realpath /tmp/repo/Image/123/Binary/myimage`
```


# Exporting images with metadata

The `-f` option supports `ome-tiff`, `ome-xml` and `tiff` to
convert the data from original files into different formats.

These files are generated from binary files and other
metadata stored in OMERO, so this process may be slower than
downloading.

OME-TIFF includes pixel data, acquisition metadata and annotations.
OME-XML does not include pixel data and TIFF is images only
(does not include metadata).

Big images can be exported. Repeating an export resumes any interrupted
image tile downloads and skips images that were already exported.

The metadata included in OME-TIFF export currently includes that of the
Images, ROIs, and some of the simple kinds of annotation on either of
those. This can be limited with the `-x` option if less metadata is
desired.


# Targeting multiple images

Instead of using `Image` as a target, containers such as `Project`, `Dataset` or
`Screen` may be specified to target all their Images.

Note that the [default server configuration](https://docs.openmicroscopy.org/latest/omero/sysadmins/config.html#omero-policy-binary-access) disables download of original files
for Screen/Plate data. In this case the `-f tiff` option can be used as a workaround
to allow export of Images as TIFFs.

Additionally, specifying `-a` extends the targeted images to include all
that are in the same fileset as any targeted image.


# Fetching metadata only

Image metadata is available as OME-XML without pixel data. As for
OME-TIFF export the `-x` option also limits download of this.

`ome-xml-parts` downloads metadata for images, ROIs and some simple
annotations on them as many standalone XML files. For example, with,

```
./download.sh -b /tmp/repo -s <server host> -u <user name> -w <password> -f ome-xml-parts Image:123
```

the ROIs on image 123 can then be found as
`/tmp/repo/Image/123/Roi/*/Metadata/roi-*.ome.xml`. Note that these
numbered `Roi/*` directories are themselves symbolic links into
`/tmp/repo/Roi/`.

`ome-xml-whole` assembles OME-XML export files from the standalone XML
files. `ome-xml` is shorthand for both the parts and whole. So, to
export image 123's metadata into one file, simply omit the `-parts` from
the above then check `/tmp/repo/Image/123/Export/image-123.ome.xml`.

It is possible to directly target annotations and ROIs for metadata
export regardless of images.


# Licensing

Available as Open Source: see [the license](LICENSE.txt) for details.

Copyright (C) 2016-2020 University of Dundee & Open Microscopy Environment.
All rights reserved.


# See also

- [Change Log](CHANGELOG.md)
- [Implementation Notes](IMPLEMENTATION.md)
