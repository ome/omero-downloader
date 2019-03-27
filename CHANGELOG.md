# 0.1.4 (Feburary 2019)

TIFF export:

- Set `writeSequentially` to improve performance.
- Fix export bugs affecting pixel data.
    - Compress with zlib's Deflate instead of JPEG-2000.
    - Write planes in XYCZT order to match ImageJ.
- List TIFF export options in user help.


# 0.1.3 (Feburary 2019)

Fix the `download.bat` launch script for Windows users.


# 0.1.{1,2} (January 2019)

Release when a tag is pushed.


# 0.1.0 (December 2018)

Initial release.
