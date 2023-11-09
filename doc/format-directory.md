% LSM specification: directory and file layouts
% Duncan Coutts

# Scope

This document is intend to cover the overall LSM directory format, including:

 * persistent snapshots
 * live handles
 * sufficient checksum/consistency info to detect directory corruption
   (not just individual file content corruption)
 * high-level configuration options persisted to files (if necessary)

This document is part of a group of documents that cover the formats:
 * `format-directory.md`: covering the overall LSM directory format
 * `format-run.md`: covering the LSM file-run format
 * `format-page.md`: covering the LSM file-run _page_ format

