<img src="/images/logo.png" alt="wZD Logo"/>

wZA Parameters:
========

Section [command line]
------------

- -bfilemode string
        --bfilemode=0640 - (0600-0666) permissions on Bolt archives when packing with UID and GID from the current user and group (default 0640)
- -delete
        --delete - enables the deletion of the original regular files (with --pack) after packing, or the original Bolt archives (with --unpack) after unpacking
- -disablecompaction
        --disablecompaction - disables the delayed compaction/defragmentation of Bolt archives after packing (with --overwrite)
- -disablereadintegrity
        --disablereadintegrity - disables reading CRC checksums from the binary header for regular files in Bolt archives
- -disablewriteintegrity
        --disablewriteintegrity - disables writing CRC checksums to the binary header for regular files in Bolt archives
- -fmaxsize int
        --fmaxsize=1048576 - the maximum allowed regular file size for writing to Bolt archives, otherwise it skips the file. Maximum value: 33554432 bytes (1048576 by default)
- -freelist string
        --freelist=hashmap - set free page list algorithm for Bolt archives, values: hashmap or array (default hashmap)
- -help
        --help - displays help
- -ignore
        --ignore - enables the mode for ignoring all errors when executing the mass packing or unpacking mode from the list. (If the threads are > 1, ignore mode is always enabled to continue execution of threads)
- -ignore-not
        --ignore-not - enables the mode for ignoring only those files that do not exist or to which access is denied. Valid when working in the mode of mass packing or unpacking according to the list
- -list string
        --list=/path/to/list.txt - list of files or Bolt archives for pack or unpack
- -locktimeout int
        locktimeout=5 - the maximum timeout for opening the Bolt archive in 1 attempt. Maximum value: 3600 (5 by default)
- -overwrite
        --overwrite - turns on the mode for overwriting regular files or files in Bolt archives when packing or unpacking
- -pack
        --pack - enables the mode for packing regular files from a list (with --list=) or a single regular file (with --single=) into Bolt archives
- -progress
        --progress - enables the progress bar mode (incompatible with --verbose)
- -single string
        --single=/path/to/file - pack or unpack one regular file (with --pack) or Bolt archive (with --unpack)
- -show string
        --show=/path/to/file.bolt - shows regular files and/or values in single Bolt archive
- -threads int
        --threads=1 - parallel mass packing or unpacking (compaction/defragmentation (with --overwrite), is single-threaded for security) Maximum value: 256 (default 1)
- -tmpdir string
        --tmpdir=/tmp/wza - temporary directory for splitting a list of files between threads
- -trytimes int
        trytimes=5 - the maximum number of attempts to obtain a virtual lock on the Bolt archive (default sleep = 1 between attempts), (with --pack && --list=). Maximum value: 1000 (default 5)
- -opentries int
        opentries=5 - the maximum number of attempts to open Bolt archive (efault sleep = 1 between attempts), Maximum value: 1000 (default 5)
- -unpack
        --unpack - enables the mode for unpacking Bolt archives from the list (--list=) or a single Bolt archive (with --single=) into regular files
- -verbose
        --verbose - enables verbose mode (incompatible with --progress)
- -version
        --version - print version
