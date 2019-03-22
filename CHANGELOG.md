# 2019-02-28 v0.1.4

TIFF export:

- Set `writeSequentially` to improve performance.
- Fix export bugs affecting pixel data.
    - Compress with zlib's Deflate instead of JPEG-2000.
    - Write planes in XYCZT order to match ImageJ.
- List TIFF export options in user help.


# 2019-02-21 v0.1.3

Fix the `download.bat` launch script for Windows users.


# 2019-01-25 v0.1.{1,2}

Release when a tag is pushed.


# 2018-12-20 v0.1.0

Initial release.
