# Summary

An OMERO client for downloading data in bulk from the server. The `-h`
option prints a brief summary of command-line options.

`mvn` builds and packages the downloader.


# Caveat

OMERO.downloader has not yet seen much use. One should therefore expect
both bugs and breaking changes. However, it is hoped that even in its
early state there are use cases for which it offers significant help.


# Storing downloads locally

For testing, make a new scratch directory, say `/tmp/repo/`, to specify
to the `-b` option below. In general one should use a separate download
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


# Targeting multiple images

Instead of using `Image` as a target, containers such as dataset or
screens may be specified to target all their images. However, note that
for plates the default server configuration disables file download.

Additionally, specifying `-a` extends the targeted images to include all
that are in the same fileset as any targeted image.


# Fetching metadata

Image metadata is available as OME-XML without pixel data.
The included metadata can be limited with the `-x` option.

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

Copyright (C) 2016-2018 University of Dundee & Open Microscopy Environment.
All rights reserved.
