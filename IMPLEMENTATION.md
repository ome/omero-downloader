# Tour of the Java classes

The package `org.openmicroscopy.client.downloader` has the most classes
including `Download` which carries the `main` method and various other
methods relating to different setup and download options. Its
`ALL_GROUPS_CONTEXT` constant is used by various classes to keep OMERO
group context from limiting query results. The `main` method uses:

1. The `org.openmicroscopy.client.downloader.options` package's
   `OptionParser` which encodes the client application's command-line
   options. That class uses `OptionSet` as a helper for options whose
   value represents a subset of possible choices. An instance of
   `OptionParser.Chosen` bears the options parsed from the command line.

1. OMERO.blitz's `FindChildren` to determine which OMERO model objects
   are targeted. The `RequestManager` is a helper class for submitting
   requests to OMERO and ensuring that the response is of the expected
   type. A `FileMapper` instance stores the outcome of queries to
   understand how filesets on the server relate to their images and
   pixels instances.

1. `Download.downloadFiles` to implement the `-f` option's `binary` and
   `companion`. This is where `FileManager` is used: for downloading
   files from a raw file store provided by the server. (The class used
   to have more methods.) It uses `LinkMakerPaths` for linking to the
   repository files from the other directories.

1. `Download.exportImages` to implement the `-f` option's `ome-tiff` and
   `tiff`. It uses `LocalPixels` to fetch pixel data from a raw pixels
   store from the server, `XmlGenerator` for populating the metadata
   store and `LinkMakerMetadata` for linking model objects in that store
   to note their relationships. `LocalPixels` uses a couple of helpers
   from the package `org.openmicroscopy.client.downloader.util`:
   `TileIterator` covers all the tiles of an image and `FileIO` reads
   and writes integers and byte arrays. Tiles are recorded in an
   intermediate file before Bio-Formats performs the export.

1. `Download.writeXmlObjects` to implement the `-f` option's
   `ome-xml-parts` with the help of `XmlGenerator`. This also makes some
   use of `LinkMakerPaths` for linking objects from their containers.
   `MetadataBase` provides helpers for the other classes of the
   `org.openmicroscopy.client.downloader.metadata` package which
   `XmlGenerator` uses to fetch metadata about the various top-level
   model objects. Those classes largely copy from OMERO.blitz's
   `OmeroMetadata` except that to support image export `ImageMetadata`
   specifies *XYCZT* dimension ordering to correspond with the ImageJ
   convention implemented by Bio-Formats' `TiffReader`. (`TileIterator`
   adapts to the specified ordering.)

1. `Download.assembleReferencedXml` to implement the `-f` option's
   `ome-xml-whole` with the help of `XmlAssembler` which uses the
   package `org.openmicroscopy.client.downloader.util`'s
   `TruncatingOutputStream` to trim unwanted headers and footers from
   XML documents as they are concatenated. The cross-referenced XML
   documents are assembled from fragments previously written by
   `XmlGenerator`, relationships being inferred from the linking on the
   filesystem. Each top-level model object is handled separately for
   scalability: even though all the objects are included in the output,
   an image document can be written without having every one of its
   annotations and ROIs in the metadata store at the same time.

Addtionally, `ModelType` enumerates the top-level model types by which
metadata is organized in download. To the OME Model types that can be
children of the `<OME>` tag in XML this adds the OMERO type `Fileset`.
`LocalPaths` defines the organization of the local filesystem and
interprets it for determining links among model objects.


# To-do's

## Technical nitpicks

1. Resource files should provide at least default configuration
   rather than relying solely on the command line.

1. The code's focus on top-level OME Model units is too pervasive. For
   example, one may need to export annotations that are on channels.

## Progress and errors

1. Logging is currently minimal and not configurable. Underlying
   exceptions are not reported, nor are warnings from Bio-Formats.

1. Printed error messages are often too vague, making it difficult to
   diagnose login or other issues.

1. When errors do occur server-side resources may not be cleaned up. The
   code makes broad use of the `System.exit` of
   `Download.abortOnFatalError` which is the wrong approach.

1. The exit code should indicate the outcome of the download process.

## Broader metadata

1. The input and output of metadata has incomplete coverage. For
   example, try importing or exporting OME-XML datasets using OMERO
   code. The implications affect this client and resolving such gaps
   requires strategic decisions.

1. OME-XML is hardly an ideal bulk metadata format. We should provide
   documentation on how to use the metadata export files from, say, a
   MATLAB script, but OME-XML is probably not the format from which one
   wants to read a million ROIs into R.

1. Much of the code could be reused; not only for download but, for
   example, the `ome-xml-whole` code for reading the model objects and
   their containment relationships. Providing a library API could much
   help script writers hoping to analyze exported metadata.

## Broader usability

1. The code assumes that the user is comfortable at the command line.
   Friendly veneers could be provided such as a graphical client for
   setting the download options and a local filesystem view using
   container *names*.

1. The code assumes that the user is downloading a subset of data from
   an essentially static server. Presently there is no consideration of
   *updating* the local filesystem to match changes on the server.
