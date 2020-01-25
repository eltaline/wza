<img src="/images/logo.png" alt="wZA Logo"/>

Документация на русском: https://github.com/eltaline/wza/blob/master/README-RUS.md

wZA is a archiver written in Go language that uses a <a href=https://github.com/eltaline/bolt>modified</a> version of the BoltDB database,
for conversion regular files to Bolt аrchives and these unpacking. Archiver is used together with <a href=https://github.com/eltaline/wzd>wZD</a> server.

<img src="/images/wzd-scheme.png" alt="wZD Scheme"/>

Current stable version: 1.1.0
========

Added in version 1.1.0:

- Choice of free page algorithm in BoltDB (freelist parameter)

Fixed in version 1.1.0:

- Fix memory buffers
- Fix some regular expressions

Features
========

- Multithreading
- Archiving files up to 32MB in size
- Supports CRC data integrity when writing or reading
- Mixed mode support. Large files will be excluded from archiving in Bolt archives

Incompatibilities
========

- Recalculation and recording of the checksum without unpacking and packaging operations is not yet supported
- Data disks cannot simply be transferred from the Little Endian system to the Big Endian system, or vice versa

Requirements
========

- Operating Systems: Linux, BSD, Solaris, OSX
- Architectures: amd64, arm64, ppc64 and mips64, with only amd64 tested
- Supported Byte Order: Little or Big Endian
- Any POSIX compatible file system with full locking support (preferred clustered MooseFS)

Real application
========

Our cluster used has about 250,000,000 small pictures and 15,000,000 directories on separate SATA drives. It utilizes the MooseFS cluster file system. This works well with so many files, but at the same time, its Master servers consume 75 gigabytes of RAM, and since frequent dumps of a large amount of metadata occur, this is bad for SSD disks. Accordingly, there is also a limit of about 1 billion files in MooseFS itself with the one replica of each file.

With a fragmented directory structure, an average of 10 to 1000 files are stored in most directories. After installing wZD and archiving the files in Bolt archives, it turned out about 25 times less files, about 10,000,000. With proper planning of the structure, a smaller number of files could have been achieved, but this is not possible if the already existing structure remains unchanged. Proper planning would result in very large inodes savings, low memory consumption of the cluster FS, significant acceleration of the MooseFS operation itself, and a reduction in the actual space occupied on the MooseFS cluster FS. The fact is, MooseFS always allocates a block of 64KB for each file, that is, even if a file has a size of 3KB, will still be allocated 64KB.

The multithreaded wZA archiver has already been tested on real data.

Our cluster used (10 servers) is an Origin server installed behind a CDN network and served by only 2 wZD servers.

<p align="center">
<img align="center" src="/images/reduction-full.png"/>
</p>

Documentation
========

<b>A table that describes the best options for using the server. How many files can be uploaded in one Bolt archive.</b>

<p align="center">
<img align="center" src="/images/optimal.png"/>
</p>

Installation
------------

Install packages or binaries
------------

- <a href=https://github.com/eltaline/wza/releases>Download</a>

**Files are packed and unpacked in the same directory where they were located, and not in the current directory where wZA starts**

View Bolt archive content
------------

```bash
wza --show=/path/to/file.bolt
```

Packing a single file to the Bolt archive
--------

```bash
wza --pack --single=/path/to/file.ext
```

Unpacking all files from a single Bolt archive
--------

```bash
wza --unpack --single=/path/to/file.bolt
```

Packing all files to Bolt archives, according to the list of files
--------

```bash
find /var/storage -type f -not -name '*.bolt' > /tmp/pack.list
wza --pack --list=/tmp/pack.list
```

Unpacking all files from the list of Bolt archives
--------

```bash
find /var/storage -type f -name '*.bolt' > /tmp/unpack.list
wza --unpack --list=/tmp/unpack.list
```

Data migration in 3 steps without stopping the service
------------

This archiver was designed to be used in conjunction with the <a href=https://github.com/eltaline/wzd>wZD</a> server,
for converting files on current real production systems without stopping the service.

The archiver allows for converting current files to Bolt archives without deletion and with deletion, and also allows for unpacking them back. It supports overwrite and other functions.

**The archiver, as far as possible, is safe. It does not support recursive traversal, only works on a pre-prepared list of files or Bolt archives, and provides for repeated reading of the file after packing and CRC verification of the checksum on the fly.**

Data migration guide:

The path to be migrated: /var/storage. Here are jpg files in the 1 million subdirectories of various depths that need to be archived in Bolt archives, but only th
ose files that are no larger than 1 MB.

- Nginx or HAProxy and wZD server should be configured with a virtual host and with a root in the /var/storage directory

- Perform a recursive search:

   ```bash
   find /var/storage -type f -name '*.jpg' -not -name '*.bolt' > /tmp/migration.list
   ```

- Start archiving without deleting current files:

   ```bash
   wza --pack --fmaxsize=1048576 --list=/tmp/migration.list
   ```

- Start deleting old files with key checking in Bolt archives without deleting files that are larger than fmaxsize:

  ```bash
  wza --pack --delete --fmaxsize=1048576 --list=/tmp/migration.list
  ```

This can be combined into one operation with the removal of the old files.
The archiver skips non-regular files and will not allow archiving of the Bolt archive to the Bolt archive.
The archiver will not delete the file until the checksum of the file being read coincides, after archiving, with the newly read file from the Bolt archive, unless
 of course that is forced to disable.
While the wZD server is running, it returns data from regular files, if it finds them. This is a priority.

This guide provides an example of a single-threaded start, stopping at any error with any file, even if non-regular files were included in the prepared list or fi
les were suddenly deleted by another process but remained in the list. To ignore already existing files from the list, the --ignore-not option must be added. The
same is true when unpacking.

Restarting archiving without the --overwrite option will not overwrite files in Bolt archives. The same is true when unpacking.

The archiver also supports the multithreaded version --threads= and also other options.
In the multithreaded version, the --ignore option is automatically applied so as not to stop running threads when any errors occur. In case of an error with the -
-delete option turned on, the source file will not be deleted.

A full description of all product parameters is available here: <a href="/OPTIONS.md">Options</a>

Notes and Q&A
========

- Bolt archives are automatically named with the name of the directory in which they are created

- It is not recommended to upload 100,000+ files to one directory (one Bolt archive); this would be a large overhead. If possible, plan your directory structure correctly

- It is not recommended to upload files or values larger than 16MB to Bolt archives. By default, the parameter fmaxsize = 1048576 bytes

- If the fmaxsize parameter is exceeded, files are skipped. The maximum possible size of the parameter is fmaxsize = 33554432 bytes

- When using the --disablewriteintegrity and / or --disablereadintegrity parameters, the CRC checksum is not written or verified. It is highly recommended not to use these options.

ToDo
========

- Recalculate and record the checksum without unpacking and packing
- Make CRC check for the contents of one archive
- Add Compression (gzip, zstd, snappy)
- Add Encryption

Parameters
========

A full description of all product parameters is available here: <a href="/OPTIONS-RUS.md">Options</a>

Guarantees
========

No warranty is provided for this software. Please test first

Donations
========

<a href="https://www.paypal.me/xwzd"><img src="/images/paypal.png"><a/>

Contacts
========

- E-mail: dev@wzd.dev
- wZD website: https://wzd.dev
- Company website: <a href="https://elta.ee">Eltaline</a>

```
Copyright © 2020 Andrey Kuvshinov. Contacts: <syslinux@protonmail.com>
Copyright © 2020 Eltaline OU. Contacts: <eltaline.ou@gmail.com>
All rights reserved.
```
