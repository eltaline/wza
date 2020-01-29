package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"sync"
	"syscall"
	"time"
)

// Global Configuration

// Header : type contains binary header fields
type Header struct {
	Size uint64
	Date uint32
	Mode uint16
	Uuid uint16
	Guid uint16
	Comp uint8
	Encr uint8
	Crcs uint32
	Rsvr uint64
}

// KeysIter : type contains results of iteration through key and value pairs
type KeysIter struct {
	key string
	val string
}

// Global Variables

var (
	// Endian global variable
	Endian binary.ByteOrder

	// Uid : System user UID
	Uid int64
	// Gid : System user GID
	Gid int64

	wg       sync.WaitGroup
	wgthread sync.WaitGroup

	shutdown bool = false

	progress bool = false
	verbose  bool = false

	defsleep time.Duration = 1 * time.Second

	trytimes    int = 5
	opentries   int = 5
	locktimeout int = 5

	list                  string
	single                string
	pack                  bool = false
	unpack                bool = false
	show                  string
	ifilemode             string
	bfilemode             os.FileMode
	fmaxsize              int64 = 1048576
	perbucket             int   = 1024
	overwrite             bool  = false
	ignore                bool  = false
	ignorenot             bool  = false
	fdelete               bool  = false
	disablereadintegrity  bool  = false
	disablewriteintegrity bool  = false
	disablecompaction     bool  = false
	tmpdir                string
	threads               int64 = 1
	freelist              string

	upgrade bool = false
)

// Interrupt : custom interrupt handler
func Interrupt() {
	channel := make(chan os.Signal, 2)
	signal.Notify(channel, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-channel
		fmt.Printf("Ctrl+C Interrupt Terminal\n")
		shutdown = true
		wgthread.Wait()
		wg.Wait()
		os.Exit(0)
	}()
}

// Init Function

func init() {

	var version string = "1.1.2"
	var vprint bool = false
	var help bool = false

	// Command Line Options

	flag.StringVar(&list, "list", list, "--list=/path/to/list.txt - list of files or Bolt archives for packing or unpacking")
	flag.StringVar(&single, "single", single, "--single=/path/to/file - pack or unpack one regular file (with --pack) or Bolt archive (with --unpack)")
	flag.BoolVar(&pack, "pack", pack, "--pack - enables the mode for packing regular files from a list (with --list=), or a single regular file (with --single=) into Bolt archives")
	flag.BoolVar(&unpack, "unpack", unpack, "--unpack - enables the mode for unpacking Bolt archives from the list (--list=), or a single Bolt archive (with --single=) into regular files")
	flag.StringVar(&show, "show", show, "--show=/path/to/file.bolt - shows regular files and/or values in single Bolt archive")
	flag.StringVar(&ifilemode, "bfilemode", ifilemode, "--bfilemode=0640 - (0600-0666) permissions on Bolt archives when packing with UID and GID from the current user and group (default 0640)")
	flag.Int64Var(&fmaxsize, "fmaxsize", fmaxsize, "--fmaxsize=1048576 -  the maximum allowed regular file size for writing to Bolt archives, otherwise it skips the file. Maximum value: 33554432 bytes")
	flag.BoolVar(&overwrite, "overwrite", overwrite, "--overwrite - turns on the mode for overwriting regular files or files in Bolt archives when packing or unpacking")
	flag.BoolVar(&ignore, "ignore", ignore, "--ignore - enables the mode for ignoring all errors when executing the mass packing or unpacking mode from the list. (If the threads are > 1, ignore mode is always enabled to continue execution of threads)")
	flag.BoolVar(&ignorenot, "ignore-not", ignorenot, "--ignore-not - enables the mode for ignoring only those files that do not exist or to which access is denied. Valid when working in the mode of mass packing or unpacking according to the list")
	flag.BoolVar(&fdelete, "delete", fdelete, "--delete - enables the deletion of the original regular files (with --pack) after packing, or the original Bolt archives (with --unpack) after unpacking")
	flag.BoolVar(&disablereadintegrity, "disablereadintegrity", disablereadintegrity, "--disablereadintegrity - disables reading CRC checksums from the binary header for regular files in Bolt archives")
	flag.BoolVar(&disablewriteintegrity, "disablewriteintegrity", disablewriteintegrity, "--disablewriteintegrity - disables writing CRC checksums to the binary header for regular files in Bolt archives")
	flag.BoolVar(&disablecompaction, "disablecompaction", disablecompaction, "--disablecompaction - disables the delayed compaction/defragmentation of Bolt archives after packing (with --overwrite)")
	flag.StringVar(&tmpdir, "tmpdir", tmpdir, "--tmpdir=/tmp/wza - temporary directory for splitting a list of files between threads")
	flag.Int64Var(&threads, "threads", threads, "--threads=1 - parallel mass packing or unpacking (compaction/defragmentation (with --overwrite), is single-threaded for security) Maximum value: 256")
	flag.StringVar(&freelist, "freelist", freelist, "--freelist=hashmap - set free page list algorithm for Bolt archives, Values: hashmap or array (default hashmap)")
	flag.IntVar(&trytimes, "trytimes", trytimes, "trytimes=5 - the maximum number of attempts to obtain a virtual lock on the Bolt archive (default sleep = 1 between attempts), (with --pack && --list=). Maximum value: 1000")
	flag.IntVar(&opentries, "opentries", opentries, "opentries=5 - the maximum number of attempts to open Bolt archive (efault sleep = 1 between attempts), Maximum value: 1000")
	flag.IntVar(&locktimeout, "locktimeout", locktimeout, "locktimeout=60 - the maximum timeout for opening the Bolt archive in 1 attempt. Maximum value: 3600")
	flag.BoolVar(&progress, "progress", progress, "--progress - enables the progress bar mode (incompatible with --verbose)")
	flag.BoolVar(&verbose, "verbose", verbose, "--verbose - enables verbose mode (incompatible with --progress)")
	flag.BoolVar(&vprint, "version", vprint, "--version - print version")
	flag.BoolVar(&help, "help", help, "--help - displays help")
	flag.BoolVar(&upgrade, "upgrade", upgrade, "--upgrade - upgrade bolt archives to latest version from a list (with --list=)")

	flag.Parse()

	switch {
	case vprint:
		fmt.Printf("wZA Version: %s\n", version)
		os.Exit(0)
	case help:
		flag.PrintDefaults()
		os.Exit(0)
	}

	// Basic Checks

	if pack && unpack {
		fmt.Printf("Can`t continue work with --pack and --unpack together, use only one option | Pack [%t] | Unpack [%t]\n", pack, unpack)
		os.Exit(1)
	}

	if !pack && !unpack && show == "" && !upgrade {
		fmt.Printf("Can`t continue work without --pack or --unpack -or --show, use only one option | Pack [%t] | Unpack [%t] | Show [%s]\n", pack, unpack, show)
		os.Exit(1)
	}

	if list != "" && single != "" {
		fmt.Printf("Can`t continue work with --list and --single together, use only one option | List [%s] | Single [%s]\n", list, single)
		os.Exit(1)
	}

	if list == "" && single == "" && show == "" {
		fmt.Printf("Can`t continue work without --list or --single or --show, use only one option | List [%s] | Single [%s] | Show [%s]\n", list, single, show)
		os.Exit(1)
	}

	if show != "" && (pack || unpack || single != "" || list != "" || upgrade) {
		fmt.Printf("Can`t continue work with --show and other options, for show use only show option | Show [%s]\n", show)
		os.Exit(1)
	}

	if disablereadintegrity && pack {
		fmt.Printf("Can`t continue work with --pack and --disablereadintegrity together\n")
		os.Exit(1)
	}

	if disablewriteintegrity && unpack {
		fmt.Printf("Can`t continue work with --unpack and --disablewriteintegrity together\n")
		os.Exit(1)
	}

	if disablecompaction && !overwrite {
		fmt.Printf("Can`t continue work with --disablecompaction and without --overwrite\n")
		os.Exit(1)
	}

	if progress && verbose {
		fmt.Printf("Can`t continue work with --progress and --overwrite together\n")
		os.Exit(1)
	}

	if list != "" {

		rgxpath := regexp.MustCompile("^(/?[^/\x00]*)+/?$")
		mchrgxpath := rgxpath.MatchString(list)
		Check(mchrgxpath, list, DoExit)

	}

	if single != "" {

		rgxpath := regexp.MustCompile("^(/?[^/\x00]*)+/?$")
		mchrgxpath := rgxpath.MatchString(single)
		Check(mchrgxpath, single, DoExit)

	}

	if show != "" {

		rgxpath := regexp.MustCompile("^(/?[^/\x00]*)+/?$")
		mchrgxpath := rgxpath.MatchString(show)
		Check(mchrgxpath, show, DoExit)

	}

	if list != "" && !FileExists(list) {
		fmt.Printf("Can`t continue work with list of files not exists error | List [%s]\n", list)
		os.Exit(1)
	}

	if single != "" && !FileExists(single) {
		fmt.Printf("Can`t continue work with single file not exists error | Single [%s]\n", single)
		os.Exit(1)
	}

	if show != "" && !FileExists(show) {
		fmt.Printf("Can`t continue work with show file not exists error | Show [%s]\n", show)
		os.Exit(1)
	}

	if ifilemode != "" {

		rgxfilemode := regexp.MustCompile("^([0-7]{4})")
		mchfilemode := rgxfilemode.MatchString(ifilemode)
		Check(mchfilemode, ifilemode, DoExit)

	}

	mchfmaxsize := RBInt64(fmaxsize, 1, 33554432)
	Check(mchfmaxsize, fmt.Sprintf("%d", fmaxsize), DoExit)

	if tmpdir != "" {

		rgxpath := regexp.MustCompile("^(/[^/\x00]*)+/?$")
		mchrgxpath := rgxpath.MatchString(tmpdir)
		Check(mchrgxpath, tmpdir, DoExit)

	} else {
		tmpdir = "/tmp/wza"
	}

	mchthreads := RBInt64(threads, 1, 256)
	Check(mchthreads, fmt.Sprintf("%d", threads), DoExit)

	if freelist != "" {

		rgxfreelist := regexp.MustCompile("^(?i)(hashmap|array)$")
		mchrgxfreelist := rgxfreelist.MatchString(freelist)
		Check(mchrgxfreelist, list, DoExit)

	} else {
		freelist = "hashmap"
	}

	mchtrytimes := RBInt(trytimes, 1, 1000)
	Check(mchtrytimes, fmt.Sprintf("%d", trytimes), DoExit)

	mchopentries := RBInt(opentries, 1, 1000)
	Check(mchopentries, fmt.Sprintf("%d", opentries), DoExit)

	mchlocktimeout := RBInt(locktimeout, 1, 3600)
	Check(mchlocktimeout, fmt.Sprintf("%d", locktimeout), DoExit)

	if upgrade && (pack || unpack || show != "") {
		fmt.Printf("Can`t continue work with --pack and --unpack or --show together, use --upgrade option only with --list= option | List [%s]\n", list)
		os.Exit(1)
	}

	if upgrade && list == "" {
		fmt.Printf("Can`t continue work with --upgrade option and empty --list= option, use --upgrade option only with --list= option | List [%s]\n", list)
		os.Exit(1)
	}

	if show == "" && !upgrade {

		switch {
		case fdelete:
			fmt.Printf("Info | Delete Mode [ENABLED]\n")
		default:
			fmt.Printf("Info | Delete Mode [DISABLED]\n")
		}

		switch {
		case ignore:
			fmt.Printf("Info | Ignore Mode [ENABLED]\n")
		default:
			fmt.Printf("Info | Ignore Mode [DISABLED]\n")
		}

		switch {
		case overwrite:
			fmt.Printf("Info | Overwrite Mode [ENABLED]\n")
		default:
			fmt.Printf("Info | Overwrite Mode [DISABLED]\n")
		}

		switch {
		case disablewriteintegrity && pack:
			fmt.Printf("Info | Write Integrity Mode [DISABLED]\n")
		case pack:
			fmt.Printf("Info | Write integrity Mode [ENABLED]\n")
		}

		switch {
		case disablereadintegrity && unpack:
			fmt.Printf("Info | Read Integrity Mode [DISABLED]\n")
		case unpack:
			fmt.Printf("Info | Read integrity Mode [ENABLED]\n")
		}

		if pack {
			fmt.Printf("Info | Max Allowed File Size [%d]\n", fmaxsize)
		}

		fmt.Printf("\n")

	}

	if upgrade {

		threads = 1
		verbose = true
		progress = false

		fmt.Printf("Info | Upgrade Mode [ENABLED]\n")

	}

}

// Main Function

func main() {

	// System Handling

	DetectEndian()
	DetectUser()
	Interrupt()

	err := os.MkdirAll(tmpdir, 0777)
	if err != nil {
		fmt.Printf("Can`t create temporary directory error | Directory [%s] | %v\n", tmpdir, err)
		os.Exit(1)
	}

	err = os.Chmod(tmpdir, 0777)
	if err != nil {
		fmt.Printf("Can`t chmod temporary directory error | Directory [%s] | %v\n", tmpdir, err)
		os.Exit(1)
	}

	// Threads Errors Handle

	if threads > 1 {
		ignore = true
	}

	// Bolt File Mode

	cfilemode, err := strconv.ParseUint(ifilemode, 8, 32)

	switch {
	case err != nil || cfilemode == 0:
		bfilemode = os.FileMode(0640)
	default:
		bfilemode = os.FileMode(cfilemode)
	}

	switch {
	case list != "" && pack:
		ZAPackList()
	case list != "" && unpack:
		ZAUnpackList()
	case single != "" && pack:
		ZAPackSingle()
	case single != "" && unpack:
		ZAUnpackSingle()
	case show != "":
		ZAShowSingle()
	case list != "" && upgrade:
		ZAUpgrade()
	}

	wg.Wait()
	os.Exit(0)

}
