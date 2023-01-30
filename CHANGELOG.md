# 0.3.0 (January 2023)

- Update to use OMERO Java Gateway 5.7.0. (PR [\#41](https://github.com/ome/omero-downloader/pull/49))
- Update to use testNG 7.5. (PR [\#41](https://github.com/ome/omero-downloader/pull/49))
- Exclude edu.ucar:cdm-core. (PR [\#41](https://github.com/ome/omero-downloader/pull/49))


# 0.2.2 (October 2020)

- Update to use OMERO Java Gateway 5.6.5. (PR [\#41](https://github.com/ome/omero-downloader/pull/41))

# 0.2.1 (January 2020)

- Remove the Unidata NetCDF repository. (PR [\#28](https://github.com/ome/omero-downloader/pull/28))
- Work against OMERO 5.5 and 5.6 servers. (PR [\#31](https://github.com/ome/omero-downloader/pull/31))
- Set `writeSequentially` to improve performance. (PR [\#32](https://github.com/ome/omero-downloader/pull/32))

# 0.2.0 (July 2019)

Work against OMERO 5.5 servers.
(PR [\#22](https://github.com/ome/omero-downloader/pull/22))


# 0.1.5 (May 2019)

- Automatically adapt to inability to create symbolic links.
  (PR [\#16](https://github.com/ome/omero-downloader/pull/16))
- Unset `writeSequentially` to address pixel data corruption in TIFF
  export. (Issue
  [\#17](https://github.com/ome/omero-downloader/issues/17))
- Include map annotations in metadata export.
  (PR [\#18](https://github.com/ome/omero-downloader/pull/18))


# 0.1.4 (Feburary 2019)

TIFF export:

- Set `writeSequentially` to improve performance.
  (PR [\#10](https://github.com/ome/omero-downloader/pull/10))
- Fix export bugs affecting pixel data.
  (PR [\#11](https://github.com/ome/omero-downloader/pull/11))
    - Compress with zlib's Deflate instead of JPEG-2000.
    - Write planes in XYCZT order to match ImageJ.
- List TIFF export options in user help.
  (PR [\#11](https://github.com/ome/omero-downloader/pull/11))


# 0.1.3 (Feburary 2019)

Fix the `download.bat` launch script for Windows users.
(PRs [\#6](https://github.com/ome/omero-downloader/pull/6),
[\#7](https://github.com/ome/omero-downloader/pull/7))


# 0.1.{1,2} (January 2019)

Release when a tag is pushed.


# 0.1.0 (December 2018)

Initial release.
