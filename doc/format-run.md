% LSM specification: LSM file-run format
% Duncan Coutts

# Scope

This document is intend to cover the format of the files used to represent an
individual LSM run, including:

* any header for metadata
* bloom filters
* indexes
* key/operation pages (but the page format is covered in `format-page.md`)
* blob files for large additional values (blobs can vary in size, but we can
  design performance for a range, e.g. 1-16kb, and can impose an upper bound)
* separate files for: k/ops, blobs, and index & filter
* checksums for all files

This document is part of a group of documents that cover the formats:
 * `format-directory.md`: covering the overall LSM directory format
 * `format-run.md`: covering the LSM file-run format
 * `format-page.md`: covering the LSM file-run _page_ format

