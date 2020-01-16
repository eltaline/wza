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

type KeysIter struct {
	key string
}

// Global Variables

var (
	Endian binary.ByteOrder

	Uid int64
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
	ifilemode             string
	bfilemode             os.FileMode
	fmaxsize              int64 = 1048576
	overwrite             bool  = false
	ignore                bool  = false
	ignorenot             bool  = false
	delete                bool  = false
	disablereadintegrity        = false
	disablewriteintegrity       = false
	disablecompaction           = false
	tmpdir                string
	threads               int64 = 1
)

// Interrupt Handler

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

	var version string = "1.0.0"
	var vprint bool = false
	var help bool = false

	// Command Line Options

	flag.StringVar(&list, "list", list, "--list=/path/to/list.txt - pack/unpack regular files from list (with --pack) or bolt archives from list (with --unpack)")
	flag.StringVar(&single, "single", single, "--single=/path/to/file - pack/unpack single regular file (with --pack) or bolt archive (with --unpack)")
	flag.BoolVar(&pack, "pack", pack, "--pack - enables pack mode for regular files from list (with --list) or single regular file (with --single=) to bolt archives")
	flag.BoolVar(&unpack, "unpack", unpack, "--unpack - enables unpack mode for bolt archives from list (--list=) or single bolt archive (with --single=) to regular files")
	flag.StringVar(&ifilemode, "bfilemode", ifilemode, "--bfilemode=0640 - (0600-0666) new bolt archive mode with uid/gid from current user/group (default 0640)")
	flag.Int64Var(&fmaxsize, "fmaxsize", fmaxsize, "--fmaxsize=1048576 - max allowed size of regular file to write in bolt archives, otherwise skip, max value: 536870912 bytes")
	flag.BoolVar(&overwrite, "overwrite", overwrite, "--overwrite - enables overwrite regular files or files in bolt archives when do pack/unpack")
	flag.BoolVar(&ignore, "ignore", ignore, "--ignore - enables skip all errors mode when do mass pack/unpack mode from list (if threads > 1, ignore always enabled for continue working threads)")
	flag.BoolVar(&ignorenot, "ignore-not", ignorenot, "--ignore-not - enables skip only file not exists/permission denied errors mode when do mass pack/unpack mode from list")
	flag.BoolVar(&delete, "delete", delete, "--delete - enables delete mode original regular files (with --pack) after pack or original bolt archives (with --unpack) after unpack")
	flag.BoolVar(&disablereadintegrity, "disablereadintegrity", disablereadintegrity, "--disablereadintegrity - disables read crc checksums from binary header for regular files in bolt archives")
	flag.BoolVar(&disablewriteintegrity, "disablewriteintegrity", disablewriteintegrity, "--disablewriteintegrity - disables write crc checksums to binary header for regular files in bolt archives")
	flag.BoolVar(&disablecompaction, "disablecompaction", disablecompaction, "--disablecompaction - disables delayed compaction of bolt archives when do pack (with --overwrite)")
	flag.StringVar(&tmpdir, "tmpdir", tmpdir, "--tmpdir=/tmp/wza - temporary directory for split file list between threads")
	flag.Int64Var(&threads, "threads", threads, "--threads=1 - for parallel mass pack/unpack (compaction (with --overwrite) is single threaded, for safety), max value: 256")
	flag.IntVar(&trytimes, "trytimes", trytimes, "trytimes=5 - max tries to take a virtual lock for bolt archive (default sleep = 1 between tries), (with --pack && --list=), max value: 1000")
	flag.IntVar(&opentries, "opentries", opentries, "opentries=5 - max tries to open bolt archive (default sleep = 1 between tries), (with --pack && --list=), max value: 1000")
	flag.IntVar(&locktimeout, "locktimeout", locktimeout, "locktimeout=5 - max timeout for open bolt archive, per try, max value: 3600")
	flag.BoolVar(&progress, "progress", progress, "--progress - enables progress bar mode (incompatible with --verbose)")
	flag.BoolVar(&verbose, "verbose", verbose, "--verbose - enables verbose mode (incompatible with --progress)")
	flag.BoolVar(&vprint, "version", vprint, "--version - prints version")
	flag.BoolVar(&help, "help", help, "--help - prints help")

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

	if !pack && !unpack {
		fmt.Printf("Can`t continue work without --pack or --unpack, use only one option | Pack [%t] | Unpack [%t]\n", pack, unpack)
		os.Exit(1)
	}

	if list != "" && single != "" {
		fmt.Printf("Can`t continue work with --list and --single together, use only one option | List [%s] | Single [%s]\n", list, single)
		os.Exit(1)
	}

	if list == "" && single == "" {
		fmt.Printf("Can`t continue work without --list or --single, use only one option | List [%s] | Single [%s]\n", list, single)
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

		rgxpath := regexp.MustCompile("^(/[^/\x00]*)+/?$")
		mchrgxpath := rgxpath.MatchString(list)
		Check(mchrgxpath, list, DoExit)

	}

	if single != "" {

		rgxpath := regexp.MustCompile("^(/[^/\x00]*)+/?$")
		mchrgxpath := rgxpath.MatchString(single)
		Check(mchrgxpath, single, DoExit)

	}

	if list != "" && !FileExists(list) {
		fmt.Printf("Can`t continue work with list of files not exists error | List [%s]\n", list)
		os.Exit(1)
	}

	if single != "" && !FileExists(single) {
		fmt.Printf("Can`t continue work with single file not exists error | Single [%s]\n", single)
		os.Exit(1)
	}

	if ifilemode != "" {

		rgxfilemode := regexp.MustCompile("^([0-7]{4})")
		mchfilemode := rgxfilemode.MatchString(ifilemode)
		Check(mchfilemode, ifilemode, DoExit)

	}

	mchfmaxsize := RBInt64(fmaxsize, 1, 536870912)
	Check(mchfmaxsize, fmt.Sprintf("%d", fmaxsize), DoExit)

	if tmpdir != "" {

		rgxpath := regexp.MustCompile("^(/[^/\x00]*)+/?$")
		mchrgxpath := rgxpath.MatchString(list)
		Check(mchrgxpath, list, DoExit)

	} else {
		tmpdir = "/tmp/wza"
	}

	mchthreads := RBInt64(threads, 1, 256)
	Check(mchthreads, fmt.Sprintf("%d", threads), DoExit)

	mchtrytimes := RBInt(trytimes, 1, 1000)
	Check(mchtrytimes, fmt.Sprintf("%d", trytimes), DoExit)

	mchopentries := RBInt(opentries, 1, 1000)
	Check(mchopentries, fmt.Sprintf("%d", opentries), DoExit)

	mchlocktimeout := RBInt(locktimeout, 1, 3600)
	Check(mchlocktimeout, fmt.Sprintf("%d", locktimeout), DoExit)

	switch {
	case delete:
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
	}

	wg.Wait()
	os.Exit(0)

}
