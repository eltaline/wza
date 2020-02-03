package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/eltaline/bolt"
	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"time"
)

// ZAUnpackList : unpacking all files from the list of bolt archives
func ZAUnpackList() {
	defer wg.Done()

	var err error

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
		// No need to defer in loop

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
			fmt.Printf("Read lines from list file error | List [%s] | %v\n", list, err)
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

		go ZAUnpackListThread(listname, p, name)

		time.Sleep(time.Duration(250) * time.Millisecond)

	}

	wgthread.Wait()

	time.Sleep(time.Duration(100) * time.Millisecond)

	for t = 1; t <= threads; t++ {

		listname := fmt.Sprintf("%s/%s_%d", tmpdir, lfname, t)
		err = RemoveFile(listname)
		if err != nil {

			fmt.Printf("Can`t remove temporary part file list error | List [%s] | File [%s] | %v\n", list, listname, err)

			if ignore {
				continue
			}

			os.Exit(1)

		}

	}

}

// ZAUnpackListThread : from ZAUnpackList: unpacking all files from the threaded lists of bolt archives
func ZAUnpackListThread(listname string, p *mpb.Progress, name string) {
	defer wgthread.Done()

	// Wait Group

	var vfilemode os.FileMode

	bucket := ""
	ibucket := "index"

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

		start := time.Now()

		uri := scanner.Text()

		dir := filepath.Dir(uri)
		file := filepath.Base(uri)

		abs := filepath.Clean(dir + "/" + file)

		dbn := filepath.Base(dir)
		dbf := filepath.Clean(dir + "/" + dbn + ".bolt")

		if DirExists(abs) {

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

		db, err := BoltOpenRead(dbf, bfilemode, timeout, opentries, freelist)
		if err != nil {

			if !progress {
				fmt.Printf("Can`t open db file error | DB [%s] | %v\n", dbf, err)
			}

			if ignore || ignorenot {
				continue
			}

			return

		}
		// No need to defer in loop

		var keys []KeysIter
		var k KeysIter

		err = db.View(func(tx *bolt.Tx) error {

			verr := errors.New("index bucket not exists")

			b := tx.Bucket([]byte(ibucket))
			if b != nil {
				pos := b.Cursor()

				for inkey, inval := pos.First(); inkey != nil; inkey, inval = pos.Next() {
					k.key = string(inkey)
					k.val = string(inval)
					keys = append(keys, k)
				}

			} else {
				return verr
			}

			return nil

		})
		if err != nil {

			fmt.Printf("Can`t iterate all keys of files in index db bucket error | DB [%s] | %v\n", dbf, err)
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
			bucket = hkey.val
			dabs := filepath.Clean(dir + "/" + rkey)

			if FileExists(dabs) && !overwrite {
				loopcount++
				bar.IncrBy(1)
				continue
			}

			var pdata []byte

			err = db.View(func(tx *bolt.Tx) error {

				verr := errors.New("bucket not exists")

				b := tx.Bucket([]byte(bucket))
				if b != nil {
					pdata = b.Get([]byte(rkey))
					return nil
				} else {
					return verr
				}

			})
			if err != nil {

				fmt.Printf("Can`t get data by key from db error | File [%s] | DB [%s] | %v\n", rkey, dbf, err)
				pdata = nil

				if ignore {
					continue
				}

				db.Close()
				return

			}

			pread := bytes.NewReader(pdata)

			var readhead Header

			headbuffer := make([]byte, 36)

			hsizebuffer, err := pread.Read(headbuffer)
			if err != nil {

				fmt.Printf("Read binary header data from db error | File [%s] | DB [%s] | Header Buffer [%p] | %v\n", rkey, dbf, headbuffer, err)
				pdata = nil
				headbuffer = nil

				if ignore {
					continue
				}

				db.Close()
				return

			}

			hread := bytes.NewReader(headbuffer[:hsizebuffer])

			err = binary.Read(hread, Endian, &readhead)
			if err != nil {

				fmt.Printf("Read binary header data from db error | File [%s] | DB [%s] | Header Buffer [%p] | %v\n", rkey, dbf, hread, err)
				pdata = nil
				headbuffer = nil
				readhead = Header{}

				if ignore {
					continue
				}

				db.Close()
				return

			}

			headbuffer = nil

			tmst := int64(readhead.Date)
			modt := time.Unix(tmst, 0)

			crc := readhead.Crcs

			switch {
			case readhead.Mode == 0:
				vfilemode = os.FileMode(0640)
			default:
				vfilemode = os.FileMode(readhead.Mode)
			}

			readhead = Header{}

			endbuffer := new(bytes.Buffer)

			if !disablereadintegrity && crc != 0 {

				var readbuffer bytes.Buffer

				tee := io.TeeReader(pread, &readbuffer)

				rtbl := crc32.MakeTable(0xEDB88320)

				rcrcdata := new(bytes.Buffer)

				_, err = rcrcdata.ReadFrom(tee)
				if err != nil && err != io.EOF {

					fmt.Printf("Can`t read tee crc data error | File [%s] | DB [%s] | %v\n", rkey, dbf, err)
					pdata = nil
					readbuffer.Reset()
					rcrcdata.Reset()

					if ignore {
						continue
					}

					db.Close()
					return

				}

				rcrc = crc32.Checksum(rcrcdata.Bytes(), rtbl)

				rcrcdata.Reset()

				if crc != rcrc {

					fmt.Printf("CRC read file error | File [%s] | DB [%s] | Have CRC [%v] | Awaiting CRC [%v]\n", rkey, dbf, rcrc, crc)
					pdata = nil
					readbuffer.Reset()

					if ignore {
						continue
					}

					db.Close()
					return

				}

				_, err = endbuffer.ReadFrom(&readbuffer)
				if err != nil && err != io.EOF {

					fmt.Printf("Can`t read readbuffer data error | File [%s] | DB [%s] | %v\n", file, dbf, err)
					pdata = nil
					readbuffer.Reset()
					endbuffer.Reset()

					if ignore {
						continue
					}

					db.Close()
					return

				}

				pdata = nil
				readbuffer.Reset()

			} else {

				_, err = endbuffer.ReadFrom(pread)
				if err != nil && err != io.EOF {

					fmt.Printf("Can`t read readbuffer data error | File [%s] | DB [%s] | %v\n", file, dbf, err)
					pdata = nil
					endbuffer.Reset()

					if ignore {
						continue
					}

					db.Close()
					return

				}

			}

			err = ioutil.WriteFile(dabs, endbuffer.Bytes(), vfilemode)
			if err != nil {

				fmt.Printf("Write full buffer write to file error | File [%s] | Path [%s] | %v\n", rkey, dabs, err)
				pdata = nil
				endbuffer.Reset()

				if ignore {
					continue
				}

				db.Close()
				return

			}

			err = os.Chtimes(dabs, modt, modt)
			if err != nil {

				fmt.Printf("Can`t change time on file error | File [%s] | Path [%s] | %v\n", rkey, dabs, err)
				pdata = nil
				endbuffer.Reset()

				if ignore {
					continue
				}

				db.Close()
				return

			}

			loopcount++

			pdata = nil
			endbuffer.Reset()

			elapsed := float64(time.Since(start)) / float64(time.Millisecond)

			bar.IncrBy(1)

			if verbose {
				fmt.Printf("Unpacking file | File [%s] | Path [%s] | Elapsed [%f] | DB [%s]\n", rkey, dabs, elapsed, dbf)
			}

		}

		if fdelete {

			keyscount, err := KeysCount(db, ibucket)
			if err != nil {

				fmt.Printf("Can`t count keys of files in index db bucket error | DB [%s] | %v\n", dbf, err)
				db.Close()

				if ignore {
					continue
				}

				return

			}

			db.Close()

			if keyscount == loopcount {

				err = RemoveFileDB(dbf)
				if err != nil {

					fmt.Printf("Can`t remove db file error | DB [%s] | %v\n", dbf, err)

					if ignore {
						continue
					}

					return

				}

				if verbose {
					fmt.Printf("Deleting db file | DB [%s]\n", dbf)
				}

			} else {

				fmt.Printf("Keys count in db != extracted file, | DB [%s] | DB Keys [%d] | Extracted Files [%d] | %v\n", dbf, keyscount, loopcount, err)

				if ignore {
					continue
				}

				return

			}

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
		fmt.Printf("Read lines from list file error | List [%s] | %v\n", list, err)
		return
	}

}

// ZAUnpackSingle : unpacking all files from single bolt archive
func ZAUnpackSingle() {
	defer wg.Done()

	// Wait Group

	wg.Add(1)

	start := time.Now()

	var vfilemode os.FileMode

	bucket := ""
	ibucket := "index"

	timeout := time.Duration(locktimeout) * time.Second

	rgxbolt := regexp.MustCompile(`(\.bolt$)`)

	uri := single

	dir := filepath.Dir(uri)
	file := filepath.Base(uri)

	abs := filepath.Clean(dir + "/" + file)

	dbn := filepath.Base(dir)
	dbf := filepath.Clean(dir + "/" + dbn + ".bolt")

	if DirExists(abs) {
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

	db, err := BoltOpenRead(dbf, bfilemode, timeout, opentries, freelist)
	if err != nil {
		fmt.Printf("Can`t open db file error | DB [%s] | %v\n", dbf, err)
		os.Exit(1)
	}
	defer db.Close()

	var keys []KeysIter
	var k KeysIter

	err = db.View(func(tx *bolt.Tx) error {

		verr := errors.New("index bucket not exists")

		b := tx.Bucket([]byte(ibucket))
		if b != nil {
			pos := b.Cursor()

			for inkey, inval := pos.First(); inkey != nil; inkey, inval = pos.Next() {
				k.key = string(inkey)
				k.val = string(inval)
				keys = append(keys, k)
			}

		} else {
			return verr
		}

		return nil

	})
	if err != nil {

		fmt.Printf("Can`t iterate all keys of files in index db bucket error | DB [%s] | %v\n", dbf, err)
		db.Close()
		os.Exit(1)

	}

	var rkey string

	var loopcount int = 0

	for _, hkey := range keys {

		rkey = hkey.key
		bucket = hkey.val
		dabs := filepath.Clean(dir + "/" + rkey)

		if FileExists(dabs) && !overwrite {
			loopcount++
			continue
		}

		var pdata []byte

		err = db.View(func(tx *bolt.Tx) error {

			verr := errors.New("bucket not exists")

			b := tx.Bucket([]byte(bucket))
			if b != nil {
				pdata = b.Get([]byte(rkey))
				return nil
			} else {
				return verr
			}

		})
		if err != nil {

			fmt.Printf("Can`t get data by key from db error | File [%s] | DB [%s] | %v\n", rkey, dbf, err)
			pdata = nil

			continue

		}

		pread := bytes.NewReader(pdata)

		var readhead Header

		headbuffer := make([]byte, 36)

		hsizebuffer, err := pread.Read(headbuffer)
		if err != nil {

			fmt.Printf("Read binary header data from db error | File [%s] | DB [%s] | Header Buffer [%p] | %v\n", rkey, dbf, headbuffer, err)
			pdata = nil
			headbuffer = nil

			continue

		}

		hread := bytes.NewReader(headbuffer[:hsizebuffer])

		err = binary.Read(hread, Endian, &readhead)
		if err != nil {

			fmt.Printf("Read binary header data from db error | File [%s] | DB [%s] | Header Buffer [%p] | %v\n", rkey, dbf, hread, err)
			pdata = nil
			headbuffer = nil
			readhead = Header{}

			continue

		}

		headbuffer = nil

		tmst := int64(readhead.Date)
		modt := time.Unix(tmst, 0)

		crc := readhead.Crcs

		switch {
		case readhead.Mode == 0:
			vfilemode = os.FileMode(0640)
		default:
			vfilemode = os.FileMode(readhead.Mode)
		}

		readhead = Header{}

		endbuffer := new(bytes.Buffer)

		if !disablereadintegrity {

			var readbuffer bytes.Buffer

			tee := io.TeeReader(pread, &readbuffer)

			rtbl := crc32.MakeTable(0xEDB88320)

			rcrcdata := new(bytes.Buffer)

			_, err = rcrcdata.ReadFrom(tee)
			if err != nil && err != io.EOF {

				fmt.Printf("Can`t read tee crc data error | File [%s] | DB [%s] | %v\n", rkey, dbf, err)
				pdata = nil
				readbuffer.Reset()
				rcrcdata.Reset()

				continue

			}

			rcrc = crc32.Checksum(rcrcdata.Bytes(), rtbl)

			rcrcdata.Reset()

			if crc != rcrc {

				fmt.Printf("CRC read file error | File [%s] | DB [%s] | Have CRC [%v] | Awaiting CRC [%v]\n", rkey, dbf, rcrc, crc)
				pdata = nil
				readbuffer.Reset()
				continue

			}

			_, err = endbuffer.ReadFrom(&readbuffer)
			if err != nil && err != io.EOF {

				fmt.Printf("Can`t read readbuffer data error | File [%s] | DB [%s] | %v\n", file, dbf, err)
				pdata = nil
				readbuffer.Reset()
				endbuffer.Reset()

				continue

			}

			pdata = nil
			readbuffer.Reset()

		} else {

			_, err = endbuffer.ReadFrom(pread)
			if err != nil && err != io.EOF {

				fmt.Printf("Can`t read readbuffer data error | File [%s] | DB [%s] | %v\n", file, dbf, err)
				pdata = nil
				endbuffer.Reset()

				continue

			}

		}

		err = ioutil.WriteFile(dabs, endbuffer.Bytes(), vfilemode)
		if err != nil {

			fmt.Printf("Write full buffer write to file error | File [%s] | Path [%s] | %v\n", rkey, dabs, err)
			pdata = nil
			endbuffer.Reset()

			continue

		}

		err = os.Chtimes(dabs, modt, modt)
		if err != nil {

			fmt.Printf("Can`t change time on file error | File [%s] | Path [%s] | %v\n", rkey, dabs, err)
			pdata = nil
			endbuffer.Reset()

			continue

		}

		loopcount++

		pdata = nil
		endbuffer.Reset()

		elapsed := float64(time.Since(start)) / float64(time.Millisecond)

		if verbose {
			fmt.Printf("Unpacking file | File [%s] | Path [%s] | Elapsed [%f] | DB [%s]\n", rkey, dabs, elapsed, dbf)
		}

	}

	if fdelete {

		keyscount, err := KeysCount(db, ibucket)
		if err != nil {

			fmt.Printf("Can`t count keys of files in index db bucket error | DB [%s] | %v\n", dbf, err)
			db.Close()
			os.Exit(1)

		}

		db.Close()

		if keyscount == loopcount {

			err = RemoveFileDB(dbf)
			if err != nil {
				fmt.Printf("Can`t remove db file error | DB [%s] | %v\n", dbf, err)
				os.Exit(1)
			}

			if verbose {
				fmt.Printf("Deleting db file | DB [%s]\n", dbf)
			}

		} else {

			fmt.Printf("Keys count in db != extracted file, | DB [%s] | DB Keys [%d] | Extracted Files [%d] | %v\n", dbf, keyscount, loopcount, err)
			os.Exit(1)

		}

		os.Exit(0)

	}

	db.Close()

}
