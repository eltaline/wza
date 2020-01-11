package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/eltaline/bolt"
	"github.com/eltaline/mmutex"
	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

// Global Configuration

type header struct {
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

type keysIter struct {
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

	defsleep    time.Duration = 1 * time.Second
	opentries   int           = 30
	trytimes    int           = 45
	locktimeout int           = 60

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

func wzPackList() {
	defer wg.Done()

	// Wait Group

	wg.Add(1)

	// Map Mutex

	keymutex := mmutex.NewMMutex()

	// Compaction Map

	mcmp := make(map[string]bool)
	timeout := time.Duration(locktimeout) * time.Second

	lfile, err := os.OpenFile(list, os.O_RDONLY, os.ModePerm)
	if err != nil {
		fmt.Printf("Can`t open list file error | List [%s] | %v\n", list, err)
		os.Exit(1)
	}
	defer lfile.Close()

	var count int64 = 0

	scaninit := bufio.NewScanner(lfile)
	for scaninit.Scan() {
		count++
	}

	err = scaninit.Err()
	if err != nil {
		fmt.Printf("Count lines from list file error | List [%s] | %v\n", list, err)
		os.Exit(1)
	}

	_, err = lfile.Seek(0, 0)
	if err != nil {

		fmt.Printf("Can`t seek in list file to position 0 error\n")

		err = lfile.Close()
		if err != nil {
			fmt.Printf("Close list file error | List [%s] | %v\n", list, err)
			os.Exit(1)
		}

		os.Exit(0)

	}

	lfname := filepath.Base(list)

	var partlines int64 = count / threads
	var lastlines int64 = count - (partlines * threads)

	scanline := bufio.NewScanner(lfile)

	p := mpb.New(mpb.WithWaitGroup(&wgthread))

	var t int64

	for t = 1; t <= threads; t++ {

		if shutdown {
			return
		}

		listname := fmt.Sprintf("%s/%s_%d", tmpdir, lfname, t)
		fdlist, err := os.Create(listname)
		if err != nil {
			fmt.Printf("Can`t create part list file error | List [%s] | File [%s] | %v\n", list, listname, err)
			os.Exit(1)
		}
		defer fdlist.Close()

		err = os.Chmod(listname, 0666)
		if err != nil {
			fmt.Printf("Can`t chmod part list file error | List [%s] | File [%s] | %v\n", list, listname, err)
			os.Exit(1)
		}

		wline := bufio.NewWriter(fdlist)

		var lc int64 = 0
		var ls int64 = 0

		for scanline.Scan() {

			if shutdown {
				return
			}

			fline := fmt.Sprintf("%s\n", scanline.Text())

			_, err = wline.WriteString(fline)
			if err != nil {

				fmt.Printf("Can`t write string to part list file error | List [%s] | File [%s] | %v\n", list, listname, err)
				err = fdlist.Close()

				if err != nil {
					fmt.Printf("Close temporary part file list error | List [%s] | File [%s] | %v\n", list, listname, err)
					os.Exit(1)
				}

				os.Exit(1)

			}

			if t == threads && lc == partlines {

				if ls == lastlines {
					break
				}

				ls++

				continue

			}

			if lc == partlines {

				lc = 0
				break

			}

			lc++

		}

		wline.Flush()

		err = fdlist.Close()
		if err != nil {
			fmt.Printf("Close temporary part file list error | List [%s] | File [%s] | %v\n", list, listname, err)
			os.Exit(1)
		}

		err = scanline.Err()
		if err != nil {
			fmt.Printf("Read lines from list file err | List [%s] | %v\n", list, err)
			os.Exit(1)
		}

	}

	err = lfile.Close()
	if err != nil {
		fmt.Printf("Close list file error | List [%s] | %v\n", list, err)
		os.Exit(1)
	}

	for t = 1; t <= threads; t++ {

		if shutdown {
			return
		}

		name := fmt.Sprintf("Thread: %d ", t)

		listname := fmt.Sprintf("%s/%s_%d", tmpdir, lfname, t)

		wgthread.Add(1)

		go wzPackListThread(keymutex, mcmp, listname, t, p, name)

		time.Sleep(time.Duration(100) * time.Millisecond)

	}

	wgthread.Wait()

	time.Sleep(time.Duration(100) * time.Millisecond)

	// Delayed Compaction

	if !disablecompaction {

		for ckey, cval := range mcmp {

			if shutdown {
				return
			}

			if cval {

				dbf := ckey

				key := false

				for i := 0; i < trytimes; i++ {

					if key = keymutex.TryLock(dbf); key {
						break
					}

					time.Sleep(defsleep)

				}

				if key {

					db, err := bolt.Open(dbf, bfilemode, &bolt.Options{Timeout: timeout})
					if err != nil {

						if !fileExists(dbf) {

							fmt.Printf("Can`t open db for delayed compaction error | DB [%s] | %v\n", dbf, err)
							keymutex.Unlock(dbf)

							if ignore {
								continue
							}

							return

						}

						tries := 0

						for itry := 0; itry <= opentries; itry++ {

							tries++

							db, err = bolt.Open(dbf, bfilemode, &bolt.Options{Timeout: timeout})
							if err == nil {
								break
							}

							time.Sleep(defsleep)

						}

						if tries == opentries {

							fmt.Printf("Can`t open db for delayed compaction error | DB [%s] | %v\n", dbf, err)
							keymutex.Unlock(dbf)

							if ignore {
								continue
							}

							return

						}

					}
					defer db.Close()

					err = db.CompactQuietly()
					if err != nil {

						fmt.Printf("On the fly delayed compaction error | DB [%s] | %v\n", dbf, err)
						db.Close()
						keymutex.Unlock(dbf)

						if ignore {
							continue
						}

						return

					}

					if verbose {
						fmt.Printf("Delayed compaction db | DB [%s]\n", dbf)
					}

					err = os.Chmod(dbf, bfilemode)
					if err != nil {
						fmt.Printf("Can`t chmod db error | DB [%s] | %v\n", dbf, err)
						db.Close()
						keymutex.Unlock(dbf)

						if ignore {
							continue
						}

						return

					}

					db.Close()
					keymutex.Unlock(dbf)

				} else {

					fmt.Printf("| Timeout mmutex lock error | DB [%s]\n", dbf)

					if ignore {
						continue
					}

					return

				}

			}

		}

	}

	for t = 1; t <= threads; t++ {

		listname := fmt.Sprintf("%s/%s_%d", tmpdir, lfname, t)
		err = removeFile(listname)
		if err != nil {

			fmt.Printf("Can`t remove temporary part file list error | List [%s] | File [%s] | %v\n", list, listname, err)

			if ignore {
				continue
			}

			os.Exit(1)

		}

	}

}

func wzPackListThread(keymutex *mmutex.Mutex, mcmp map[string]bool, listname string, t int64, p *mpb.Progress, name string) {
	defer wgthread.Done()

	var vfilemode uint64

	bucket := "default"
	timeout := time.Duration(locktimeout) * time.Second

	rgxbolt := regexp.MustCompile(`(\.bolt$)`)

	lfile, err := os.OpenFile(listname, os.O_RDONLY, os.ModePerm)
	if err != nil {

		fmt.Printf("Can`t open part list file error | File [%s] | %v\n", listname, err)
		wgthread.Done()
		return

	}
	defer lfile.Close()

	var count int64 = 0

	scaninit := bufio.NewScanner(lfile)
	for scaninit.Scan() {
		count++
	}

	err = scaninit.Err()
	if err != nil {
		fmt.Printf("Count lines from part list file error | File [%s] | %v\n", listname, err)
		os.Exit(1)
	}

	bar := p.AddBar(count, mpb.PrependDecorators(decor.Name(name), decor.CountersNoUnit("%d / %d", decor.WCSyncWidth)),
		mpb.AppendDecorators(decor.Percentage(decor.WCSyncSpace)))

	_, err = lfile.Seek(0, 0)
	if err != nil {

		fmt.Printf("Can`t seek in part list file to position 0 error | File [%s] | %v\n", listname, err)

		err = lfile.Close()
		if err != nil {
			fmt.Printf("Close part list file error | File [%s] | %v\n", listname, err)
			return
		}

		return

	}

	if !progress {
		bar.Abort(true)
	} else {
		verbose = false
	}

	scanner := bufio.NewScanner(lfile)
	for scanner.Scan() {

		if shutdown {
			return
		}

		uri := scanner.Text()

		dir := filepath.Dir(uri)
		file := filepath.Base(uri)

		abs := fmt.Sprintf("%s/%s", dir, file)

		dbn := filepath.Base(dir)
		dbf := fmt.Sprintf("%s/%s.bolt", dir, dbn)

		if dirExists(abs) {

			if !progress {
				fmt.Printf("Skipping directory add to bolt | Directory [%s]\n", abs)
			}

			continue

		}

		mchregbolt := rgxbolt.MatchString(file)

		if mchregbolt {

			if !progress {
				fmt.Printf("Skipping bolt add to bolt | File [%s] | Path [%s]\n", file, abs)
			}

			continue

		}

		lnfile, err := os.Lstat(abs)
		if err != nil {

			if !progress {
				fmt.Printf("Can`t stat file error | File [%s] | Path [%s] | %v\n", file, abs, err)
			}

			if ignore || ignorenot {
				continue
			}

			return

		}

		if lnfile.Mode()&os.ModeType != 0 {

			if !progress {
				fmt.Printf("Skipping non regular file add to bolt | File [%s] | Path [%s]\n", file, abs)
			}

			if ignore || ignorenot {
				continue
			}

			return

		}

		infile, err := os.Stat(abs)
		if err != nil {

			if !progress {
				fmt.Printf("Can`t stat file error | File [%s] | Path [%s] | %v\n", file, abs, err)
			}

			if ignore || ignorenot {
				continue
			}

			return

		}

		size := infile.Size()
		modt := infile.ModTime()
		tmst := modt.Unix()
		filemode := infile.Mode()

		cfilemode, err := strconv.ParseUint(fmt.Sprintf("%o", filemode), 8, 32)
		switch {
		case err != nil || cfilemode == 0:
			filemode = os.FileMode(0640)
			vfilemode, _ = strconv.ParseUint(fmt.Sprintf("%o", filemode), 8, 32)
		default:
			filemode = os.FileMode(cfilemode)
			vfilemode, _ = strconv.ParseUint(fmt.Sprintf("%o", filemode), 8, 32)
		}

		if size == 0 {

			if !progress {
				fmt.Printf("Skipping file add to bolt | File [%s] | Path [%s] | Size [%d] | Zero File Size\n", file, abs, size)
			}

			continue

		}

		if size > fmaxsize {

			if !progress {
				fmt.Printf("Skipping file add to bolt | File [%s] | Path [%s] | Size [%d] | Max Allowed File Size [%d]\n", file, abs, size, fmaxsize)
			}

			continue

		}

		key := false

		for i := 0; i < trytimes; i++ {

			if key = keymutex.TryLock(dbf); key {
				break
			}

			time.Sleep(defsleep)

		}

		if key {

			pfile, err := os.OpenFile(abs, os.O_RDONLY, os.ModePerm)
			if err != nil {

				fmt.Printf("Can`t open file error | File [%s] | Path [%s] | %v\n", file, abs, err)
				keymutex.Unlock(dbf)

				if ignore || ignorenot {
					continue
				}

				return

			}
			defer pfile.Close()

			rcrc := uint32(0)
			wcrc := uint32(0)

			db, err := bolt.Open(dbf, bfilemode, &bolt.Options{Timeout: timeout})
			if err != nil {

				if !fileExists(dbf) {

					fmt.Printf("Can`t open/create db file error | File [%s] | DB [%s] | %v\n", file, dbf, err)
					keymutex.Unlock(dbf)

					err = pfile.Close()
					if err != nil {

						fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)

						if ignore {
							continue
						}

						return

					}

					if ignore {
						continue
					}

					return

				}

				tries := 0

				for itry := 0; itry <= opentries; itry++ {

					tries++

					db, err = bolt.Open(dbf, bfilemode, &bolt.Options{Timeout: timeout})
					if err == nil {
						break
					}

					time.Sleep(defsleep)

				}

				if tries == opentries {

					fmt.Printf("Can`t open/create db file error | File [%s] | DB [%s] | %v\n", file, dbf, err)
					keymutex.Unlock(dbf)

					err = pfile.Close()
					if err != nil {

						fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)

						if ignore {
							continue
						}

						return

					}

					if ignore {
						continue
					}

					return

				}

			}
			defer db.Close()

			err = os.Chmod(dbf, bfilemode)
			if err != nil {
				fmt.Printf("Can`t chmod db error | DB [%s] | %v\n", dbf, err)
				db.Close()
				keymutex.Unlock(dbf)

				err = pfile.Close()
				if err != nil {

					fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)

					if ignore {
						continue
					}

					return

				}

				if ignore {
					continue
				}

				return

			}

			err = db.Update(func(tx *bolt.Tx) error {
				_, err := tx.CreateBucketIfNotExists([]byte(bucket))
				if err != nil {
					return err
				}
				return nil

			})
			if err != nil {

				fmt.Printf("Can`t write file to db bucket error | File [%s] | DB [%s] | %v\n", file, dbf, err)
				db.Close()
				keymutex.Unlock(dbf)

				err = pfile.Close()
				if err != nil {

					fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)

					if ignore {
						continue
					}

					return

				}

				if ignore {
					continue
				}

				return

			}

			keyexists, err := keyExists(db, bucket, file)
			if err != nil {

				fmt.Printf("Can`t check key of file in db bucket error | File [%s] | DB [%s] | %v\n", file, dbf, err)
				db.Close()
				keymutex.Unlock(dbf)

				err = pfile.Close()
				if err != nil {

					fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)

					if ignore {
						continue
					}

					return

				}

				if ignore {
					continue
				}

				return

			}

			if keyexists && !overwrite {

				if delete {

					db.Close()
					keymutex.Unlock(dbf)

					err = pfile.Close()
					if err != nil {

						fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)

						if ignore {
							continue
						}

						return

					}

					err = removeFile(abs)
					if err != nil {

						fmt.Printf("Can`t remove file error | File [%s] | Path [%s] | %v\n", file, abs, err)

						if ignore {
							continue
						}

						return

					}

					if verbose {
						fmt.Printf("Deleting file | File [%s] | Path [%s]\n", file, abs)
					}

					bar.IncrBy(1)

					continue

				}

				db.Close()
				keymutex.Unlock(dbf)

				err = pfile.Close()
				if err != nil {

					fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)

					if ignore {
						continue
					}

					return

				}

				continue

			}

			rawbuffer := new(bytes.Buffer)

			_, err = rawbuffer.ReadFrom(pfile)
			if err != nil && err != io.EOF {

				fmt.Printf("Can`t read file to rawbuffer data error | File [%s] | Path [%s] | %v\n", file, abs, err)
				db.Close()
				keymutex.Unlock(dbf)

				err = pfile.Close()
				if err != nil {

					fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)

					if ignore {
						continue
					}

					return

				}

				if ignore {
					continue
				}

				return

			}

			endbuffer := new(bytes.Buffer)

			if !disablewriteintegrity {

				var readbuffer bytes.Buffer
				tee := io.TeeReader(rawbuffer, &readbuffer)

				tbl := crc32.MakeTable(0xEDB88320)

				crcdata := new(bytes.Buffer)

				_, err = crcdata.ReadFrom(tee)
				if err != nil && err != io.EOF {

					fmt.Printf("Can`t read tee crc data error | File [%s] | Path [%s] | %v\n", file, abs, err)
					db.Close()
					keymutex.Unlock(dbf)

					err = pfile.Close()
					if err != nil {

						fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)

						if ignore {
							continue
						}

						return

					}

					if ignore {
						continue
					}

					return

				}

				wcrc = crc32.Checksum(crcdata.Bytes(), tbl)

				head := header{
					Size: uint64(size), Date: uint32(tmst), Mode: uint16(vfilemode), Uuid: uint16(Uid), Guid: uint16(Gid), Comp: uint8(0), Encr: uint8(0), Crcs: wcrc, Rsvr: uint64(0),
				}

				err = binary.Write(endbuffer, Endian, head)
				if err != nil {

					fmt.Printf("Write header data to db error | Header [%v] | File [%s] | DB [%s] | %v\n", head, file, dbf, err)
					db.Close()
					keymutex.Unlock(dbf)

					err = pfile.Close()
					if err != nil {

						fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)

						if ignore {
							continue
						}

						return

					}

					if ignore {
						continue
					}

					return

				}

				_, err = endbuffer.ReadFrom(&readbuffer)
				if err != nil && err != io.EOF {

					fmt.Printf("Can`t read readbuffer data error | File [%s] | DB [%s] | %v\n", file, dbf, err)
					db.Close()
					keymutex.Unlock(dbf)

					err = pfile.Close()
					if err != nil {

						fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)

						if ignore {
							continue
						}

						return

					}

					if ignore {
						continue
					}

					return

				}

			} else {

				head := header{
					Size: uint64(size), Date: uint32(tmst), Mode: uint16(vfilemode), Uuid: uint16(Uid), Guid: uint16(Gid), Comp: uint8(0), Encr: uint8(0), Crcs: wcrc, Rsvr: uint64(0),
				}

				err = binary.Write(endbuffer, Endian, head)
				if err != nil {

					fmt.Printf("Write header data to db error | Header [%v] | File [%s] | DB [%s] | %v\n", head, file, dbf, err)
					db.Close()
					keymutex.Unlock(dbf)

					err = pfile.Close()
					if err != nil {

						fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)

						if ignore {
							continue
						}

						return

					}

					if ignore {
						continue
					}

					return

				}

				_, err = endbuffer.ReadFrom(rawbuffer)
				if err != nil && err != io.EOF {

					fmt.Printf("Can`t read rawbuffer data error | File [%s] | DB [%s] | %v\n", file, dbf, err)
					db.Close()
					keymutex.Unlock(dbf)

					err = pfile.Close()
					if err != nil {

						fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)

						if ignore {
							continue
						}

						return

					}

					if ignore {
						continue
					}

					return

				}

			}

			err = db.Update(func(tx *bolt.Tx) error {
				nb := tx.Bucket([]byte(bucket))
				err = nb.Put([]byte(file), []byte(endbuffer.Bytes()))
				if err != nil {
					return err
				}

				return nil

			})
			if err != nil {

				fmt.Printf("Can`t write file to db bucket error | File [%s] | DB [%s] | %v\n", file, dbf, err)
				db.Close()
				keymutex.Unlock(dbf)

				err = pfile.Close()
				if err != nil {

					fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)

					if ignore {
						continue
					}

					return

				}

				if ignore {
					continue
				}

				return

			}

			err = pfile.Close()
			if err != nil {

				fmt.Printf("Close after read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
				keymutex.Unlock(dbf)

				if ignore {
					continue
				}

				return

			}

			if !disablewriteintegrity {

				var pdata []byte

				err = db.View(func(tx *bolt.Tx) error {
					nb := tx.Bucket([]byte(bucket))
					pdata = nb.Get([]byte(file))
					return nil
				})
				if err != nil {

					fmt.Printf("Can`t get data by key from db error | File [%s] | DB [%s] | %v\n", file, dbf, err)
					db.Close()
					keymutex.Unlock(dbf)

					if ignore {
						continue
					}

					return

				}

				pread := bytes.NewReader(pdata)

				var readhead header

				headbuffer := make([]byte, 32)

				hsizebuffer, err := pread.Read(headbuffer)
				if err != nil {

					fmt.Printf("Read header data from db error | Header Buffer [%p] | File [%s] | DB [%s] | %v\n", headbuffer, file, dbf, err)
					db.Close()
					keymutex.Unlock(dbf)

					if ignore {
						continue
					}

					return

				}

				hread := bytes.NewReader(headbuffer[:hsizebuffer])

				err = binary.Read(hread, Endian, &readhead)
				if err != nil {

					fmt.Printf("Read binary header data from db error | Header Buffer [%p] | File [%s] | DB [%s] | %v\n", hread, file, dbf, err)
					db.Close()
					keymutex.Unlock(dbf)

					if ignore {
						continue
					}

					return

				}

				rtbl := crc32.MakeTable(0xEDB88320)

				rcrcdata := new(bytes.Buffer)

				_, err = rcrcdata.ReadFrom(pread)
				if err != nil && err != io.EOF {

					fmt.Printf("Can`t read pread data error | File [%s] | DB [%s] | %v\n", file, dbf, err)
					db.Close()
					keymutex.Unlock(dbf)

					if ignore {
						continue
					}

					return

				}

				rcrc = crc32.Checksum(rcrcdata.Bytes(), rtbl)

				rcrcdata.Reset()

				if wcrc != rcrc {

					fmt.Printf("CRC read file error | File [%s] | DB [%s] | Have CRC [%v] | Awaiting CRC [%v]\n", file, dbf, rcrc, wcrc)
					db.Close()
					keymutex.Unlock(dbf)

					if ignore {
						continue
					}

					return

				}

			}

			db.Close()
			keymutex.Unlock(dbf)

			if keyexists {

				_, found := mcmp[dbf]
				if !found {
					mcmp[dbf] = true
				}

			}

		} else {

			fmt.Printf("| Timeout mmutex lock error | File [%s] | Path [%s] | DB [%s]\n", file, abs, dbf)

			if ignore {
				continue
			}

			return

		}

		if verbose {
			fmt.Printf("Packing file | File [%s] | Path [%s] | DB [%s]\n", file, abs, dbf)
		}

		if delete {

			err = removeFile(abs)
			if err != nil {

				fmt.Printf("Can`t remove file error | File [%s] | Path [%s] | %v\n", file, abs, err)

				if ignore {
					continue
				}

				return

			}

			if verbose {
				fmt.Printf("Deleting file | File [%s] | Path [%s]\n", file, abs)
			}
		}

		bar.IncrBy(1)

	}

	err = lfile.Close()
	if err != nil {
		fmt.Printf("Close list file error | List [%s] | %v\n", list, err)
		return
	}

	err = scanner.Err()
	if err != nil {
		fmt.Printf("Read lines from list file err | List [%s] | %v\n", list, err)
		return
	}

}

func wzPackSingle() {
	defer wg.Done()

	// Wait Group

	wg.Add(1)

	var vfilemode uint64

	uri := single

	dir := filepath.Dir(uri)
	file := filepath.Base(uri)

	abs := fmt.Sprintf("%s/%s", dir, file)

	dbn := filepath.Base(dir)
	dbf := fmt.Sprintf("%s/%s.bolt", dir, dbn)

	bucket := "default"
	timeout := time.Duration(locktimeout) * time.Second

	rgxbolt := regexp.MustCompile(`(\.bolt$)`)

	if dirExists(abs) {
		fmt.Printf("Skipping directory add to bolt | Directory [%s]\n", abs)
		os.Exit(0)
	}

	mchregbolt := rgxbolt.MatchString(file)

	if mchregbolt {
		fmt.Printf("Skipping bolt add to bolt | File [%s] | Path [%s]\n", file, abs)
		os.Exit(0)
	}

	lnfile, err := os.Lstat(abs)
	if err != nil {
		fmt.Printf("Can`t stat file error | File [%s] | Path [%s] | %v\n", file, abs, err)
		os.Exit(0)
	}

	if lnfile.Mode()&os.ModeType != 0 {
		fmt.Printf("Skipping non regular file add to bolt | File [%s] | Path [%s]\n", file, abs)
		os.Exit(0)
	}

	infile, err := os.Stat(abs)
	if err != nil {
		fmt.Printf("Can`t stat file error | File [%s] | Path [%s] | %v\n", file, abs, err)
		os.Exit(1)
	}

	size := infile.Size()
	modt := infile.ModTime()
	tmst := modt.Unix()
	filemode := infile.Mode()

	cfilemode, err := strconv.ParseUint(fmt.Sprintf("%o", filemode), 8, 32)
	switch {
	case err != nil || cfilemode == 0:
		filemode = os.FileMode(0640)
		vfilemode, _ = strconv.ParseUint(fmt.Sprintf("%o", filemode), 8, 32)
	default:
		filemode = os.FileMode(cfilemode)
		vfilemode, _ = strconv.ParseUint(fmt.Sprintf("%o", filemode), 8, 32)
	}

	if size == 0 {
		fmt.Printf("Skipping file add to bolt | File [%s] | Path [%s] | Size [%d] | Zero File Size\n", file, abs, size)
		os.Exit(0)
	}

	if size > fmaxsize {
		fmt.Printf("Skipping file add to bolt | File [%s] | Path [%s] | Size [%d] | Max Allowed File Size [%d]\n", file, abs, size, fmaxsize)
		os.Exit(0)
	}

	pfile, err := os.OpenFile(abs, os.O_RDONLY, os.ModePerm)
	if err != nil {
		fmt.Printf("Can`t open file error | File [%s] | Path [%s] | %v\n", file, abs, err)
		os.Exit(1)

	}
	defer pfile.Close()

	rcrc := uint32(0)
	wcrc := uint32(0)

	db, err := bolt.Open(dbf, bfilemode, &bolt.Options{Timeout: timeout})
	if err != nil {

		if !fileExists(dbf) {

			fmt.Printf("Can`t open db file error | File [%s] | DB [%s] | %v\n", file, dbf, err)

			err = pfile.Close()
			if err != nil {
				fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
				os.Exit(1)
			}

			os.Exit(1)

		}

		tries := 0

		for itry := 0; itry <= opentries; itry++ {

			tries++

			db, err = bolt.Open(dbf, bfilemode, &bolt.Options{Timeout: timeout})
			if err == nil {
				break
			}

			time.Sleep(defsleep)

		}

		if tries == opentries {

			fmt.Printf("Can`t open/create db file error | File [%s] | DB [%s] | %v\n", file, dbf, err)

			err = pfile.Close()
			if err != nil {
				fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
				os.Exit(1)
			}

			os.Exit(1)

		}

	}
	defer db.Close()

	err = os.Chmod(dbf, bfilemode)
	if err != nil {
		fmt.Printf("Can`t chmod db error | DB [%s] | %v\n", dbf, err)
		db.Close()

		err = pfile.Close()
		if err != nil {
			fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
			os.Exit(1)
		}

		os.Exit(1)

	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}
		return nil

	})
	if err != nil {

		fmt.Printf("Can`t write file to db bucket error | File [%s] | DB [%s] | %v\n", file, dbf, err)
		db.Close()

		err = pfile.Close()
		if err != nil {
			fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
			os.Exit(1)
		}

		os.Exit(1)

	}

	keyexists, err := keyExists(db, bucket, file)
	if err != nil {

		fmt.Printf("Can`t check key of file in db bucket error | File [%s] | DB [%s] | %v\n", file, dbf, err)
		db.Close()

		err = pfile.Close()
		if err != nil {
			fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
			os.Exit(1)
		}

		os.Exit(1)

	}

	if keyexists && !overwrite {

		if delete {

			db.Close()
			err = pfile.Close()
			if err != nil {
				fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
				os.Exit(1)
			}

			err = removeFile(abs)
			if err != nil {
				fmt.Printf("Can`t remove file error | File [%s] | Path [%s] | %v\n", file, abs, err)
				os.Exit(1)
			}

			if verbose {
				fmt.Printf("Deleting file | File [%s] | Path [%s]\n", file, abs)
			}

			os.Exit(0)

		}

		db.Close()
		err = pfile.Close()
		if err != nil {
			fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
			os.Exit(1)
		}

		os.Exit(0)

	}

	rawbuffer := new(bytes.Buffer)

	_, err = rawbuffer.ReadFrom(pfile)
	if err != nil && err != io.EOF {

		fmt.Printf("Can`t read file to rawbuffer data error | File [%s] | Path [%s] | %v\n", file, abs, err)
		db.Close()

		err = pfile.Close()
		if err != nil {
			fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
			os.Exit(1)
		}

		os.Exit(1)

	}

	endbuffer := new(bytes.Buffer)

	if !disablewriteintegrity {

		var readbuffer bytes.Buffer
		tee := io.TeeReader(rawbuffer, &readbuffer)

		tbl := crc32.MakeTable(0xEDB88320)

		crcdata := new(bytes.Buffer)

		_, err = crcdata.ReadFrom(tee)
		if err != nil && err != io.EOF {

			fmt.Printf("Can`t read tee crc data error | File [%s] | Path [%s] | %v\n", file, abs, err)
			db.Close()

			err = pfile.Close()
			if err != nil {
				fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
				os.Exit(1)
			}

			os.Exit(1)

		}

		wcrc = crc32.Checksum(crcdata.Bytes(), tbl)

		head := header{
			Size: uint64(size), Date: uint32(tmst), Mode: uint16(vfilemode), Uuid: uint16(Uid), Guid: uint16(Gid), Comp: uint8(0), Encr: uint8(0), Crcs: wcrc, Rsvr: uint64(0),
		}

		err = binary.Write(endbuffer, Endian, head)
		if err != nil {

			fmt.Printf("Write header data to db error | Header [%v] | File [%s] | DB [%s] | %v\n", head, file, dbf, err)
			db.Close()

			err = pfile.Close()
			if err != nil {
				fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
				os.Exit(1)
			}

			os.Exit(1)

		}

		_, err = endbuffer.ReadFrom(&readbuffer)
		if err != nil && err != io.EOF {

			fmt.Printf("Can`t read readbuffer data error | File [%s] | DB [%s] | %v\n", file, dbf, err)
			db.Close()

			err = pfile.Close()
			if err != nil {
				fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
				os.Exit(1)
			}

			os.Exit(1)

		}

	} else {

		head := header{
			Size: uint64(size), Date: uint32(tmst), Mode: uint16(vfilemode), Uuid: uint16(Uid), Guid: uint16(Gid), Comp: uint8(0), Encr: uint8(0), Crcs: wcrc, Rsvr: uint64(0),
		}

		err = binary.Write(endbuffer, Endian, head)
		if err != nil {

			fmt.Printf("Write header data to db error | Header [%v] | File [%s] | DB [%s] | %v\n", head, file, dbf, err)
			db.Close()

			err = pfile.Close()
			if err != nil {
				fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
				os.Exit(1)
			}

			os.Exit(1)

		}

		_, err = endbuffer.ReadFrom(rawbuffer)
		if err != nil && err != io.EOF {

			fmt.Printf("Can`t read rawbuffer data error | File [%s] | DB [%s] | %v\n", file, dbf, err)
			db.Close()

			err = pfile.Close()
			if err != nil {
				fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
				os.Exit(1)
			}

			os.Exit(1)

		}

	}

	err = db.Update(func(tx *bolt.Tx) error {
		nb := tx.Bucket([]byte(bucket))
		err = nb.Put([]byte(file), []byte(endbuffer.Bytes()))
		if err != nil {
			return err
		}

		return nil

	})
	if err != nil {

		fmt.Printf("Can`t write file to db bucket error | File [%s] | DB [%s] | %v\n", file, dbf, err)
		db.Close()

		err = pfile.Close()
		if err != nil {
			fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
			os.Exit(1)
		}

		os.Exit(1)

	}

	err = pfile.Close()
	if err != nil {
		fmt.Printf("Close after read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
		os.Exit(1)
	}

	if !disablewriteintegrity {

		var pdata []byte

		err = db.View(func(tx *bolt.Tx) error {
			nb := tx.Bucket([]byte(bucket))
			pdata = nb.Get([]byte(file))
			return nil
		})
		if err != nil {

			fmt.Printf("Can`t get data by key from db error | File [%s] | DB [%s] | %v\n", file, dbf, err)
			db.Close()
			os.Exit(1)

		}

		pread := bytes.NewReader(pdata)

		var readhead header

		headbuffer := make([]byte, 32)

		hsizebuffer, err := pread.Read(headbuffer)
		if err != nil {

			fmt.Printf("Read header data from db error | Header Buffer [%p] | File [%s] | DB [%s] | %v\n", headbuffer, file, dbf, err)
			db.Close()
			os.Exit(1)

		}

		hread := bytes.NewReader(headbuffer[:hsizebuffer])

		err = binary.Read(hread, Endian, &readhead)
		if err != nil {

			fmt.Printf("Read binary header data from db error | Header Buffer [%p] | File [%s] | DB [%s] | %v\n", hread, file, dbf, err)
			db.Close()
			os.Exit(1)

		}

		rtbl := crc32.MakeTable(0xEDB88320)

		rcrcdata := new(bytes.Buffer)

		_, err = rcrcdata.ReadFrom(pread)
		if err != nil && err != io.EOF {

			fmt.Printf("Can`t read pread data error | File [%s] | DB [%s] | %v\n", file, dbf, err)
			db.Close()
			os.Exit(1)

		}

		rcrc = crc32.Checksum(rcrcdata.Bytes(), rtbl)

		rcrcdata.Reset()

		if wcrc != rcrc {

			fmt.Printf("CRC read file error | File [%s] | DB [%s] | Have CRC [%v] | Awaiting CRC [%v]\n", file, dbf, rcrc, wcrc)
			db.Close()
			os.Exit(1)

		}

		if keyexists {

			err = db.CompactQuietly()
			if err != nil {

				fmt.Printf("On the fly compaction error | DB [%s] | %v\n", dbf, err)
				db.Close()
				os.Exit(1)

			}

			if verbose {
				fmt.Printf("Compaction db | DB [%s]\n", dbf)
			}

		}

	}

	if keyexists {

		err = db.CompactQuietly()
		if err != nil {

			fmt.Printf("On the fly compaction error | DB [%s] | %v\n", dbf, err)
			db.Close()
			os.Exit(1)

		}

		if verbose {
			fmt.Printf("Compaction db | DB [%s]\n", dbf)
		}

	}

	db.Close()

	if verbose {
		fmt.Printf("Packing file | File [%s] | Path [%s] | DB [%s]\n", file, abs, dbf)
	}

	if delete {

		err = removeFile(abs)
		if err != nil {
			fmt.Printf("Can`t remove file error | File [%s] | Path [%s] | %v\n", file, abs, err)
			os.Exit(1)
		}

		if verbose {
			fmt.Printf("Deleting file | File [%s] | Path [%s]\n", file, abs)
		}

	}

}

func wzUnpackList() {
	defer wg.Done()

	// Wait Group

	wg.Add(1)

	lfile, err := os.OpenFile(list, os.O_RDONLY, os.ModePerm)
	if err != nil {
		fmt.Printf("Can`t open list file error | List [%s] | %v\n", list, err)
		os.Exit(1)
	}
	defer lfile.Close()

	var count int64 = 0

	scaninit := bufio.NewScanner(lfile)
	for scaninit.Scan() {
		count++
	}

	err = scaninit.Err()
	if err != nil {
		fmt.Printf("Count lines from list file error | List [%s] | %v\n", list, err)
		os.Exit(1)
	}

	_, err = lfile.Seek(0, 0)
	if err != nil {

		err = lfile.Close()
		if err != nil {
			fmt.Printf("Close list file error | List [%s] | %v\n", list, err)
			os.Exit(1)
		}

		fmt.Printf("Can`t seek in list file to position 0 error\n")
		os.Exit(0)

	}

	lfname := filepath.Base(list)

	var partlines int64 = count / threads
	var lastlines int64 = count - (partlines * threads)

	scanline := bufio.NewScanner(lfile)

	p := mpb.New(mpb.WithWaitGroup(&wgthread))

	var t int64

	for t = 1; t <= threads; t++ {

		if shutdown {
			return
		}

		listname := fmt.Sprintf("%s/%s_%d", tmpdir, lfname, t)
		fdlist, err := os.Create(listname)
		if err != nil {
			fmt.Printf("Can`t create part list file error | List [%s] | File [%s] | %v\n", list, listname, err)
			os.Exit(1)
		}
		defer fdlist.Close()

		err = os.Chmod(listname, 0666)
		if err != nil {
			fmt.Printf("Can`t chmod part list file error | List [%s] | File [%s] | %v\n", list, listname, err)
			os.Exit(1)
		}

		wline := bufio.NewWriter(fdlist)

		var lc int64 = 0
		var ls int64 = 0

		for scanline.Scan() {

			if shutdown {
				return
			}

			fline := fmt.Sprintf("%s\n", scanline.Text())

			_, err = wline.WriteString(fline)
			if err != nil {

				fmt.Printf("Can`t write string to part list file error | List [%s] | File [%s] | %v\n", list, listname, err)

				err = fdlist.Close()
				if err != nil {
					fmt.Printf("Close temporary part file list error | List [%s] | File [%s] | %v\n", list, listname, err)
					os.Exit(1)
				}

				os.Exit(1)

			}

			if t == threads && lc == partlines {

				if ls == lastlines {
					break
				}

				ls++

				continue

			}

			if lc == partlines {

				lc = 0
				break

			}

			lc++

		}

		wline.Flush()

		err = fdlist.Close()
		if err != nil {
			fmt.Printf("Close temporary part file list error | List [%s] | File [%s] | %v\n", list, listname, err)
			os.Exit(1)
		}

		err = scanline.Err()
		if err != nil {
			fmt.Printf("Read lines from list file err | List [%s] | %v\n", list, err)
			os.Exit(1)
		}

	}

	err = lfile.Close()
	if err != nil {
		fmt.Printf("Close list file error | List [%s] | %v\n", list, err)
		os.Exit(1)
	}

	for t = 1; t <= threads; t++ {

		if shutdown {
			return
		}

		name := fmt.Sprintf("Thread: %d ", t)

		listname := fmt.Sprintf("%s/%s_%d", tmpdir, lfname, t)

		wgthread.Add(1)

		go wzUnpackListThread(listname, t, p, name)

		time.Sleep(time.Duration(10) * time.Millisecond)

	}

	wgthread.Wait()

	time.Sleep(time.Duration(10) * time.Millisecond)

	for t = 1; t <= threads; t++ {

		listname := fmt.Sprintf("%s/%s_%d", tmpdir, lfname, t)
		err = removeFile(listname)
		if err != nil {

			fmt.Printf("Can`t remove temporary part file list error | List [%s] | File [%s] | %v\n", list, listname, err)

			if ignore {
				continue
			}

			os.Exit(1)

		}

	}

}

func wzUnpackListThread(listname string, t int64, p *mpb.Progress, name string) {
	defer wgthread.Done()

	// Wait Group

	var vfilemode os.FileMode

	bucket := "default"
	timeout := time.Duration(locktimeout) * time.Second

	rgxbolt := regexp.MustCompile(`(\.bolt$)`)

	lfile, err := os.OpenFile(listname, os.O_RDONLY, os.ModePerm)
	if err != nil {
		fmt.Printf("Can`t open list file error | List [%s] | %v\n", list, err)
		return
	}
	defer lfile.Close()

	var count int64 = 0

	scaninit := bufio.NewScanner(lfile)
	for scaninit.Scan() {
		count++
	}

	err = scaninit.Err()
	if err != nil {
		fmt.Printf("Count lines from part list file error | File [%s] | %v\n", listname, err)
		os.Exit(1)
	}

	bar := p.AddBar(count, mpb.PrependDecorators(decor.Name(name), decor.CountersNoUnit("%d / %d", decor.WCSyncWidth)),
		mpb.AppendDecorators(decor.Percentage(decor.WCSyncSpace)))

	_, err = lfile.Seek(0, 0)
	if err != nil {

		fmt.Printf("Can`t seek in part list file to position 0 error | File [%s] | %v\n", listname, err)

		err = lfile.Close()
		if err != nil {
			fmt.Printf("Close part list file error | File [%s] | %v\n", listname, err)
			return
		}

		return

	}

	if !progress {
		bar.Abort(true)
	} else {
		verbose = false
	}

	scanner := bufio.NewScanner(lfile)
	for scanner.Scan() {

		if shutdown {
			return
		}

		uri := scanner.Text()

		dir := filepath.Dir(uri)
		file := filepath.Base(uri)

		abs := fmt.Sprintf("%s/%s", dir, file)

		dbn := filepath.Base(dir)
		dbf := fmt.Sprintf("%s/%s.bolt", dir, dbn)

		if dirExists(abs) {

			if !progress {
				fmt.Printf("Skipping directory extract from list | Directory [%s]\n", abs)
			}

			continue

		}

		mchregbolt := rgxbolt.MatchString(file)

		if !mchregbolt {

			if !progress {
				fmt.Printf("Skipping extract files from non-bolt file | File [%s] | Path [%s]\n", file, abs)
			}

			continue

		}

		dbfile, err := os.Stat(dbf)
		if err != nil {

			if !progress {
				fmt.Printf("Can`t stat db file error | DB [%s] | %v\n", dbf, err)
			}

			if ignore || ignorenot {
				continue
			}

			return

		}

		size := dbfile.Size()

		if size == 0 {

			if !progress {
				fmt.Printf("Skipping extract files from zero bolt file | DB [%s] | Size [%d] | Zero File Size\n", dbf, size)
			}

			continue

		}

		rcrc := uint32(0)

		db, err := bolt.Open(dbf, bfilemode, &bolt.Options{Timeout: timeout, ReadOnly: true})
		if err != nil {

			if !fileExists(dbf) {

				if !progress {
					fmt.Printf("Can`t open db file error | DB [%s] | %v\n", dbf, err)
				}

				if ignore || ignorenot {
					continue
				}

				return

			}

			tries := 0

			for itry := 0; itry <= opentries; itry++ {

				tries++

				db, err = bolt.Open(dbf, bfilemode, &bolt.Options{Timeout: timeout})
				if err == nil {
					break
				}

				time.Sleep(defsleep)

			}

			if tries == opentries {

				if !progress {
					fmt.Printf("Can`t open db file error | DB [%s] | %v\n", dbf, err)
				}

				if ignore {
					continue
				}

				return

			}

		}
		defer db.Close()

		var keys []keysIter
		var k keysIter

		err = db.View(func(tx *bolt.Tx) error {

			nb := tx.Bucket([]byte(bucket))
			pos := nb.Cursor()

			for inkey, _ := pos.First(); inkey != nil; inkey, _ = pos.Next() {
				k.key = fmt.Sprintf("%s", inkey)
				keys = append(keys, k)
			}

			return nil

		})
		if err != nil {

			fmt.Printf("Can`t iterate all keys of files in db bucket error | DB [%s] | %v\n", dbf, err)
			db.Close()

			if ignore {
				continue
			}

			return

		}

		var rkey string

		var loopcount int = 0

		for _, hkey := range keys {

			rkey = hkey.key
			dabs := fmt.Sprintf("%s/%s", dir, rkey)

			if fileExists(dabs) && !overwrite {
				loopcount++
				db.Close()
				bar.IncrBy(1)
				continue
			}

			var pdata []byte

			err = db.View(func(tx *bolt.Tx) error {
				nb := tx.Bucket([]byte(bucket))
				pdata = nb.Get([]byte(rkey))
				return nil
			})
			if err != nil {

				fmt.Printf("Can`t get data by key from db error | File [%s] | DB [%s] | %v\n", rkey, dbf, err)
				db.Close()

				if ignore {
					continue
				}

				return

			}

			pread := bytes.NewReader(pdata)

			var readhead header

			headbuffer := make([]byte, 32)

			hsizebuffer, err := pread.Read(headbuffer)
			if err != nil {

				fmt.Printf("Read header data from db error | Header Buffer [%p] | File [%s] | DB [%s] | %v\n", headbuffer, rkey, dbf, err)
				db.Close()

				if ignore {
					continue
				}

				return

			}

			hread := bytes.NewReader(headbuffer[:hsizebuffer])

			err = binary.Read(hread, Endian, &readhead)
			if err != nil {

				fmt.Printf("Read binary header data from db error | Header Buffer [%p] | File [%s] | DB [%s] | %v\n", hread, rkey, dbf, err)
				db.Close()

				if ignore {
					continue
				}

				return

			}

			tmst := int64(readhead.Date)
			modt := time.Unix(tmst, 0)

			crc := readhead.Crcs

			switch {
			case err != nil || readhead.Mode == 0:
				vfilemode = os.FileMode(0640)
			default:
				vfilemode = os.FileMode(readhead.Mode)
			}

			endbuffer := new(bytes.Buffer)

			if !disablereadintegrity && crc != 0 {

				var readbuffer bytes.Buffer

				tee := io.TeeReader(pread, &readbuffer)

				rtbl := crc32.MakeTable(0xEDB88320)

				rcrcdata := new(bytes.Buffer)

				_, err = rcrcdata.ReadFrom(tee)
				if err != nil && err != io.EOF {

					fmt.Printf("Can`t read tee crc data error | File [%s] | DB [%s] | %v\n", rkey, dbf, err)
					db.Close()

					if ignore {
						continue
					}

					return

				}

				rcrc = crc32.Checksum(rcrcdata.Bytes(), rtbl)

				rcrcdata.Reset()

				if crc != rcrc {

					fmt.Printf("CRC read file error | File [%s] | DB [%s] | Have CRC [%v] | Awaiting CRC [%v]\n", rkey, dbf, rcrc, crc)
					db.Close()

					if ignore {
						continue
					}

					return

				}

				_, err = endbuffer.ReadFrom(&readbuffer)
				if err != nil && err != io.EOF {

					fmt.Printf("Can`t read readbuffer data error | File [%s] | DB [%s] | %v\n", file, dbf, err)
					db.Close()

					if ignore {
						continue
					}

					return

				}

			} else {

				_, err = endbuffer.ReadFrom(pread)
				if err != nil && err != io.EOF {

					fmt.Printf("Can`t read readbuffer data error | File [%s] | DB [%s] | %v\n", file, dbf, err)
					db.Close()

					if ignore {
						continue
					}

					return

				}

			}

			err = ioutil.WriteFile(dabs, endbuffer.Bytes(), vfilemode)
			if err != nil {

				fmt.Printf("Write full buffer write to file error | File [%s] | Path [%s] | %v\n", rkey, dabs, err)
				db.Close()

				if ignore {
					continue
				}

				return
			}

			err = os.Chtimes(dabs, modt, modt)
			if err != nil {

				fmt.Printf("Can`t change time on file error | File [%s] | Path [%s] | %v\n", rkey, dabs, err)
				db.Close()

				if ignore {
					continue
				}

				return
			}

			loopcount++

			bar.IncrBy(1)

			if verbose {
				fmt.Printf("Unpacking file | File [%s] | Path [%s] | DB [%s]\n", rkey, dabs, dbf)
			}

		}

		if delete {

			keycount, err := keyCount(db, bucket)
			if err != nil {

				fmt.Printf("Can`t count keys of files in db bucket error | DB [%s] | %v\n", dbf, err)
				db.Close()

				if ignore {
					continue
				}

				return

			}

			if keycount == loopcount {

				err = removeFileDB(dbf)
				if err != nil {

					fmt.Printf("Can`t remove db file error | DB [%s] | %v\n", dbf, err)
					db.Close()

					if ignore {
						continue
					}

					return

				}

				if verbose {
					fmt.Printf("Deleting db file | DB [%s]\n", dbf)
				}

			} else {

				fmt.Printf("Keys count in db != extracted file, | DB [%s] | DB Keys [%d] | Extracted Files [%d] | %v\n", dbf, keycount, loopcount, err)
				db.Close()

				if ignore {
					continue
				}

				return

			}

			db.Close()
			continue

		}

		db.Close()

	}

	err = lfile.Close()
	if err != nil {
		fmt.Printf("Close list file error | List [%s] | %v\n", list, err)
		return
	}

	err = scanner.Err()
	if err != nil {
		fmt.Printf("Read lines from list file err | List [%s] | %v\n", list, err)
		return
	}

}

func wzUnpackSingle() {

	// Wait Group

	wg.Add(1)

	var vfilemode os.FileMode

	bucket := "default"
	timeout := time.Duration(locktimeout) * time.Second

	rgxbolt := regexp.MustCompile(`(\.bolt$)`)

	uri := single

	dir := filepath.Dir(uri)
	file := filepath.Base(uri)

	abs := fmt.Sprintf("%s/%s", dir, file)

	dbn := filepath.Base(dir)
	dbf := fmt.Sprintf("%s/%s.bolt", dir, dbn)

	if dirExists(abs) {
		fmt.Printf("Skipping directory extract from list | Directory [%s]\n", abs)
		os.Exit(0)
	}

	mchregbolt := rgxbolt.MatchString(file)

	if !mchregbolt {
		fmt.Printf("Skipping extract files from non-bolt file | File [%s] | Path [%s]\n", file, abs)
		os.Exit(0)
	}

	dbfile, err := os.Stat(dbf)
	if err != nil {
		fmt.Printf("Can`t stat db file error | DB [%s] | %v\n", dbf, err)
		os.Exit(1)
	}

	size := dbfile.Size()

	if size == 0 {
		fmt.Printf("Skipping extract files from zero bolt file | DB [%s] | Size [%d] | Zero File Size\n", dbf, size)
		os.Exit(0)
	}

	rcrc := uint32(0)

	db, err := bolt.Open(dbf, bfilemode, &bolt.Options{Timeout: timeout, ReadOnly: true})
	if err != nil {

		if !fileExists(dbf) {
			fmt.Printf("Can`t open db file error | DB [%s] | %v\n", dbf, err)
			os.Exit(1)
		}

		tries := 0

		for itry := 0; itry <= opentries; itry++ {

			tries++

			db, err = bolt.Open(dbf, bfilemode, &bolt.Options{Timeout: timeout})
			if err == nil {
				break
			}

			time.Sleep(defsleep)

		}

		if tries == opentries {

			fmt.Printf("Can`t open db file error | DB [%s] | %v\n", dbf, err)
			os.Exit(1)

		}

	}
	defer db.Close()

	var keys []keysIter
	var k keysIter

	err = db.View(func(tx *bolt.Tx) error {

		nb := tx.Bucket([]byte(bucket))
		pos := nb.Cursor()

		for inkey, _ := pos.First(); inkey != nil; inkey, _ = pos.Next() {
			k.key = fmt.Sprintf("%s", inkey)
			keys = append(keys, k)
		}

		return nil

	})
	if err != nil {

		fmt.Printf("Can`t iterate all keys of files in db bucket error | DB [%s] | %v\n", dbf, err)
		db.Close()
		os.Exit(1)

	}

	var rkey string

	var loopcount int = 0

	for _, hkey := range keys {

		rkey = hkey.key
		dabs := fmt.Sprintf("%s/%s", dir, rkey)

		if fileExists(dabs) && !overwrite {
			loopcount++
			db.Close()
			continue
		}

		var pdata []byte

		err = db.View(func(tx *bolt.Tx) error {
			nb := tx.Bucket([]byte(bucket))
			pdata = nb.Get([]byte(rkey))
			return nil
		})
		if err != nil {

			fmt.Printf("Can`t get data by key from db error | File [%s] | DB [%s] | %v\n", rkey, dbf, err)
			db.Close()
			continue

		}

		pread := bytes.NewReader(pdata)

		var readhead header

		headbuffer := make([]byte, 32)

		hsizebuffer, err := pread.Read(headbuffer)
		if err != nil {

			fmt.Printf("Read header data from db error | Header Buffer [%p] | File [%s] | DB [%s] | %v\n", headbuffer, rkey, dbf, err)
			db.Close()
			continue

		}

		hread := bytes.NewReader(headbuffer[:hsizebuffer])

		err = binary.Read(hread, Endian, &readhead)
		if err != nil {

			fmt.Printf("Read binary header data from db error | Header Buffer [%p] | File [%s] | DB [%s] | %v\n", hread, rkey, dbf, err)
			db.Close()
			continue

		}

		tmst := int64(readhead.Date)
		modt := time.Unix(tmst, 0)

		crc := readhead.Crcs

		switch {
		case err != nil || readhead.Mode == 0:
			vfilemode = os.FileMode(0640)
		default:
			vfilemode = os.FileMode(readhead.Mode)
		}

		endbuffer := new(bytes.Buffer)

		if !disablereadintegrity {

			var readbuffer bytes.Buffer

			tee := io.TeeReader(pread, &readbuffer)

			rtbl := crc32.MakeTable(0xEDB88320)

			rcrcdata := new(bytes.Buffer)

			_, err = rcrcdata.ReadFrom(tee)
			if err != nil && err != io.EOF {

				fmt.Printf("Can`t read tee crc data error | File [%s] | DB [%s] | %v\n", rkey, dbf, err)
				db.Close()
				continue

			}

			rcrc = crc32.Checksum(rcrcdata.Bytes(), rtbl)

			rcrcdata.Reset()

			if crc != rcrc {

				fmt.Printf("CRC read file error | File [%s] | DB [%s] | Have CRC [%v] | Awaiting CRC [%v]\n", rkey, dbf, rcrc, crc)
				db.Close()
				continue

			}

			_, err = endbuffer.ReadFrom(&readbuffer)
			if err != nil && err != io.EOF {

				fmt.Printf("Can`t read readbuffer data error | File [%s] | DB [%s] | %v\n", file, dbf, err)
				db.Close()
				continue

			}

		} else {

			_, err = endbuffer.ReadFrom(pread)
			if err != nil && err != io.EOF {

				fmt.Printf("Can`t read readbuffer data error | File [%s] | DB [%s] | %v\n", file, dbf, err)
				db.Close()
				continue

			}

		}

		err = ioutil.WriteFile(dabs, endbuffer.Bytes(), vfilemode)
		if err != nil {

			fmt.Printf("Write full buffer write to file error | File [%s] | Path [%s] | %v\n", rkey, dabs, err)
			db.Close()
			continue

		}

		err = os.Chtimes(dabs, modt, modt)
		if err != nil {

			fmt.Printf("Can`t change time on file error | File [%s] | Path [%s] | %v\n", rkey, dabs, err)
			db.Close()
			continue

		}

		loopcount++

		if verbose {
			fmt.Printf("Unpacking file | File [%s] | Path [%s] | DB [%s]\n", rkey, dabs, dbf)
		}

	}

	if delete {

		keycount, err := keyCount(db, bucket)
		if err != nil {

			fmt.Printf("Can`t count keys of files in db bucket error | DB [%s] | %v\n", dbf, err)
			db.Close()
			os.Exit(1)

		}

		if keycount == loopcount {

			err = removeFileDB(dbf)
			if err != nil {

				fmt.Printf("Can`t remove db file error | DB [%s] | %v\n", dbf, err)
				db.Close()
				os.Exit(1)

			}

			if verbose {
				fmt.Printf("Deleting db file | DB [%s]\n", dbf)
			}

		} else {

			fmt.Printf("Keys count in db != extracted file, | DB [%s] | DB Keys [%d] | Extracted Files [%d] | %v\n", dbf, keycount, loopcount, err)
			db.Close()
			os.Exit(1)

		}

		db.Close()
		os.Exit(0)

	}

	db.Close()

}

// Determine Endianess Handler

func DetectEndian() {

	buf := [2]byte{}
	*(*uint16)(unsafe.Pointer(&buf[0])) = uint16(0xABCD)

	switch buf {
	case [2]byte{0xCD, 0xAB}:
		Endian = binary.LittleEndian
	case [2]byte{0xAB, 0xCD}:
		Endian = binary.BigEndian
	default:
		fmt.Printf("Can`t determine native endianness error\n")
		os.Exit(1)
	}

}

// Detect Daemon User/Group Handler

func DetectUser() {

	user, err := user.Current()
	if err != nil {
		fmt.Printf("Can`t determine current user error | %v\n", err)
		os.Exit(1)
	}

	Uid, err = strconv.ParseInt(user.Uid, 10, 16)
	if err != nil {
		fmt.Printf("Can`t int convert current user uid error | %v\n", err)
		os.Exit(1)
	}

	Gid, err = strconv.ParseInt(user.Gid, 10, 16)
	if err != nil {
		fmt.Printf("Can`t int convert current user gid error | %v\n", err)
		os.Exit(1)
	}

}

// File Exists Handler

func fileExists(filename string) bool {

	if fi, err := os.Stat(filename); err == nil {

		if fi.Mode().IsRegular() {
			return true
		}

	}

	return false

}

// Dir Exists Handler

func dirExists(filename string) bool {

	if fi, err := os.Stat(filename); err == nil {

		if fi.Mode().IsDir() {
			return true
		}

	}

	return false

}

// Remove File Handler

func removeFile(file string) error {

	err := os.Remove(file)
	if err != nil {
		return err
	}

	return err

}

// DB Key Exists Handler

func keyExists(db *bolt.DB, bucket string, file string) (exkey bool, err error) {

	exkey = false

	err = db.View(func(tx *bolt.Tx) error {

		nb := tx.Bucket([]byte(bucket))
		pos := nb.Cursor()

		skey := ""

		for inkey, _ := pos.First(); inkey != nil; inkey, _ = pos.Next() {

			skey = fmt.Sprintf("%s", inkey)

			if skey == file {
				exkey = true
				break
			}

		}

		return nil

	})

	return exkey, err

}

// DB Keys Count Handler

func keyCount(db *bolt.DB, bucket string) (cnt int, err error) {

	cnt = 0

	var sts bolt.BucketStats

	err = db.View(func(tx *bolt.Tx) error {

		nb := tx.Bucket([]byte(bucket))
		sts = nb.Stats()
		cnt = sts.KeyN
		return nil

	})

	return cnt, err

}

// DB Remove File Handler

func removeFileDB(file string) error {

	err := os.Remove(file)
	if err != nil {
		return err
	}

	return err

}

// Check Options With Boolean Functions

func check(bvar bool, val string, ferr func(string)) {

	if !bvar {
		ferr(val)
	}

}

func doexit(val string) {

	fmt.Printf("Bad option value error | Value [%v]\n", val)
	os.Exit(1)

}

func RBInt(i int, min int, max int) bool {

	switch {
	case i >= min && i <= max:
		return true
	default:
		return false
	}

}

func RBInt64(i int64, min int64, max int64) bool {

	switch {
	case i >= min && i <= max:
		return true
	default:
		return false
	}

}

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
	flag.Int64Var(&fmaxsize, "fmaxsize", fmaxsize, "--fmaxsize=1048576 - max allowed size of regular file to write in bolt archives, otherwise skip, max value: 536338432 bytes")
	flag.BoolVar(&overwrite, "overwrite", overwrite, "--overwrite - enables overwrite regular files or files in bolt archives when do pack/unpack")
	flag.BoolVar(&ignore, "ignore", ignore, "--ignore - enables skip all errors mode when do mass pack/unpack mode from list (if threads > 1, ignore always enabled for continue working threads)")
	flag.BoolVar(&ignorenot, "ignore-not", ignorenot, "--ignore-not - enables skip only file not exists/permission denied errors mode when do mass pack/unpack mode from list")
	flag.BoolVar(&delete, "delete", delete, "--delete - enables delete mode original regular files (with --pack) after pack or original bolt archives (with --unpack) after unpack")
	flag.BoolVar(&disablereadintegrity, "disablereadintegrity", disablereadintegrity, "--disablereadintegrity - disables read crc checksums from binary header for regular files in bolt archives")
	flag.BoolVar(&disablewriteintegrity, "disablewriteintegrity", disablewriteintegrity, "--disablewriteintegrity - disables write crc checksums to binary header for regular files in bolt archives")
	flag.BoolVar(&disablecompaction, "disablecompaction", disablecompaction, "--disablecompaction - disables delayed compaction of bolt archives when do pack (with --overwrite)")
	flag.StringVar(&tmpdir, "tmpdir", tmpdir, "--tmpdir=/tmp/wza - temporary directory for split file list between threads")
	flag.Int64Var(&threads, "threads", threads, "--threads=1 - for parallel mass pack/unpack (compaction (with --overwrite) is single threaded, for safety), max value: 256")
	flag.IntVar(&locktimeout, "locktimeout", locktimeout, "locktimeout=60 - max timeout for open bolt archive, per try, max value: 3600")
	flag.IntVar(&opentries, "opentries", opentries, "opentries=30 - max tries to open bolt archive (default sleep = 1 between tries), max value: 1000")
	flag.IntVar(&trytimes, "trytimes", trytimes, "trytimes=45 - max tries to take a virtual lock for bolt archive (default sleep = 1 between tries), (with --pack && --list=), max value: 1000")
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
		check(mchrgxpath, list, doexit)

	}

	if single != "" {

		rgxpath := regexp.MustCompile("^(/[^/\x00]*)+/?$")
		mchrgxpath := rgxpath.MatchString(single)
		check(mchrgxpath, single, doexit)

	}

	if list != "" && !fileExists(list) {
		fmt.Printf("Can`t continue work with list of files not exists error | List [%s]\n", list)
		os.Exit(1)
	}

	if single != "" && !fileExists(single) {
		fmt.Printf("Can`t continue work with single file not exists error | Single [%s]\n", single)
		os.Exit(1)
	}

	if ifilemode != "" {

		rgxfilemode := regexp.MustCompile("^([0-7]{4})")
		mchfilemode := rgxfilemode.MatchString(ifilemode)
		check(mchfilemode, ifilemode, doexit)

	}

	mchfmaxsize := RBInt64(fmaxsize, 1, 536338432)
	check(mchfmaxsize, fmt.Sprintf("%d", fmaxsize), doexit)

	if tmpdir != "" {

		rgxpath := regexp.MustCompile("^(/[^/\x00]*)+/?$")
		mchrgxpath := rgxpath.MatchString(list)
		check(mchrgxpath, list, doexit)

	} else {
		tmpdir = "/tmp/wza"
	}

	mchthreads := RBInt64(threads, 1, 256)
	check(mchthreads, fmt.Sprintf("%d", threads), doexit)

	mchlocktimeout := RBInt(locktimeout, 1, 3600)
	check(mchlocktimeout, fmt.Sprintf("%d", locktimeout), doexit)

	mchopentries := RBInt(opentries, 1, 1000)
	check(mchopentries, fmt.Sprintf("%d", opentries), doexit)

	mchtrytimes := RBInt(trytimes, 1, 1000)
	check(mchtrytimes, fmt.Sprintf("%d", trytimes), doexit)

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
		wzPackList()
	case list != "" && unpack:
		wzUnpackList()
	case single != "" && pack:
		wzPackSingle()
	case single != "" && unpack:
		wzUnpackSingle()
	}

	wg.Wait()
	os.Exit(0)

}
