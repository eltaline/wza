/*

Copyright © 2020 Andrey Kuvshinov. Contacts: <syslinux@protonmail.com>
Copyright © 2020 Eltaline OU. Contacts: <eltaline.ou@gmail.com>
All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

The wZA project contains unmodified/modified libraries imports too with
separate copyright notices and license terms. Your use of the source code
this libraries is subject to the terms and conditions of licenses these libraries.

*/

package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/eltaline/bolt"
	"github.com/eltaline/mmutex"
	"github.com/vbauerster/mpb/v4"
	"github.com/vbauerster/mpb/v4/decor"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"
)

// ZAPackList : packing all files to bolt archives, according to the list of files
func ZAPackList() {
	defer wg.Done()

	var err error

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

		go ZAPackListThread(keymutex, mcmp, listname, p, name, &wgthread)

		time.Sleep(time.Duration(250) * time.Millisecond)

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

					db, err := BoltOpenWrite(dbf, bfilemode, timeout, opentries, freelist)
					if err != nil {

						fmt.Printf("Can`t open db for delayed compaction error | DB [%s] | %v\n", dbf, err)
						keymutex.UnLock(dbf)

						if ignore {
							continue
						}

						return

					}
					// No need to defer in loop

					err = db.CompactQuietly()
					if err != nil {

						fmt.Printf("On the fly delayed compaction error | DB [%s] | %v\n", dbf, err)
						db.Close()
						keymutex.UnLock(dbf)

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
						keymutex.UnLock(dbf)

						if ignore {
							continue
						}

						return

					}

					db.Close()
					keymutex.UnLock(dbf)

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

// ZAPackListThread : from ZAPackList: packing all files to bolt archives, according to the threaded lists of files
func ZAPackListThread(keymutex *mmutex.Mutex, mcmp map[string]bool, listname string, p *mpb.Progress, name string, wgthread *sync.WaitGroup) {
	defer wgthread.Done()

	var err error

	var vfilemode uint64

	bucket := "wzd1"
	ibucket := "index"
	sbucket := "size"
	tbucket := "time"
	cbucket := "count"

	timeout := time.Duration(locktimeout) * time.Second

	rgxbolt := regexp.MustCompile(`(\.bolt$)`)
	rgxcrcbolt := regexp.MustCompile(`(\.crcbolt$)`)

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

		start := time.Now()

		keyscount := 0
		keysbytes := 0

		uri := scanner.Text()

		dir := filepath.Dir(uri)
		file := filepath.Base(uri)

		if dir == "." || dir == "/" {
			pwd, err := os.Getwd()
			if err != nil {

				if !progress {
					fmt.Printf("Can`t get current directory error | File [%s] | Path [%s] | %v\n", file, pwd, err)
				}

				if ignore {
					continue
				}

				return

			}

			if pwd == "/" {

				if !progress {
					fmt.Printf("Can`t add a file to bolt archive in root/chroot filesystem directory restricted error | File [%s] | Path [%s]\n", file, pwd)
				}

				if ignore {
					continue
				}

				return

			}

			dir = pwd

		}

		abs := filepath.Clean(dir + "/" + file)

		dbn := filepath.Base(dir)
		dbf := filepath.Clean(dir + "/" + dbn + ".bolt")

		if DirExists(abs) {

			if !progress {
				fmt.Printf("Skipping directory add to bolt | Directory [%s]\n", abs)
			}

			continue

		}

		mchregbolt := rgxbolt.MatchString(file)
		mchregcrcbolt := rgxcrcbolt.MatchString(file)

		if mchregbolt || mchregcrcbolt {

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

		sb := make([]byte, 8)
		Endian.PutUint64(sb, uint64(size))

		modt := infile.ModTime()
		tmst := modt.Unix()

		tb := make([]byte, 8)
		Endian.PutUint64(tb, uint64(tmst))

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

		switch {
		case size >= 262144 && size < 1048576:
			perbucket = 512
		case size >= 1048576 && size < 4194304:
			perbucket = 256
		case size >= 4194304 && size < 8388608:
			perbucket = 128
		case size >= 8388608 && size < 16777216:
			perbucket = 64
		case size >= 16777216:
			perbucket = 32
		}

		var bf BoltFiles
		var bfiles []BoltFiles

		var bcount int64 = 0
		var ncount int64 = 0
		var dcount int64 = 0

		if FileExists(dbf) {

			bf.Name = dbf
			bfiles = append(bfiles, bf)

			bcount++

		}

		for {

			dcount++
			ndbf := fmt.Sprintf("%s/%s_%08d.bolt", dir, dbn, dcount)

			if FileExists(ndbf) {

				bcount++

				bf.Name = ndbf
				bfiles = append(bfiles, bf)

			} else {
				break
			}

		}

		if bcount == 0 {

			db, err := BoltOpenWrite(dbf, bfilemode, timeout, opentries, freelist)
			if err != nil {

				fmt.Printf("Can`t open/create db file error | File [%s] | DB [%s] | %v\n", file, dbf, err)

				if ignore {
					continue
				}

				return

			}

			err = os.Chmod(dbf, filemode)
			if err != nil {

				fmt.Printf("Can`t chmod db error | DB [%s] | %v\n", dbf, err)
				db.Close()

				if ignore {
					continue
				}

				return

			}

			// Keys Index Bucket

			err = db.Update(func(tx *bolt.Tx) error {
				_, err = tx.CreateBucketIfNotExists([]byte(ibucket))
				if err != nil {
					return err
				}
				return nil

			})
			if err != nil {

				fmt.Printf("Can`t create index db bucket error | File [%s] | DB [%s] | %v\n", file, dbf, err)
				db.Close()

				if ignore {
					continue
				}

				return

			}

			db.Close()

			bf.Name = dbf
			bfiles = append(bfiles, bf)

			bcount++

		}

		for _, bfile := range bfiles {

			ncount++

			dbf = bfile.Name

			lndb, err := os.Lstat(dbf)
			if err != nil {

				fmt.Printf("Can`t stat db file error | File [%s] | DB [%s] | %v\n", file, dbf, err)

				if ignore {
					continue
				}

				return

			}

			if lndb.Mode()&os.ModeType != 0 {

				fmt.Printf("Non-regular db file error | File [%s] | DB [%s] | %v\n", file, dbf, err)

				if ignore {
					continue
				}

				return

			}

			indb, err := os.Stat(dbf)
			if err != nil {

				fmt.Printf("Can`t stat db file error | File [%s] | DB [%s] | %v\n", file, dbf, err)

				if ignore {
					continue
				}

				return

			}

			bsize := indb.Size()

			db, err := BoltOpenRead(dbf, bfilemode, timeout, opentries, freelist)
			if err != nil {

				fmt.Printf("Can`t open db file error | File [%s] | DB [%s] | %v\n", file, dbf, err)

				if ignore {
					continue
				}

				return

			}

			keyexists, err := KeyExists(db, ibucket, file)
			if err != nil {

				fmt.Printf("Can`t check key of file in index db bucket error | File [%s] | DB [%s] | %v\n", file, dbf, err)
				db.Close()

				if ignore {
					continue
				}

				return

			}

			if keyexists != "" {
				db.Close()
				break
			}

			if ncount == bcount {

				keyscnt, err := KeysCount(db, ibucket)
				if err != nil {

					fmt.Printf("Can`t count keys of all files in index db bucket error | File [%s] | DB [%s] | %v", file, dbf, err)
					db.Close()

					if ignore {
						continue
					}

					return

				}

				if bsize >= smaxsize || keyscnt >= skeyscnt {

					db.Close()
					dbf = fmt.Sprintf("%s/%s_%08d.bolt", dir, dbn, ncount)
					bucket = "wzd1"
					break

				}

			}

			db.Close()

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
				keymutex.UnLock(dbf)

				if ignore || ignorenot {
					continue
				}

				return

			}

			wcrc := uint32(0)
			rcrc := uint32(0)

			db, err := BoltOpenWrite(dbf, bfilemode, timeout, opentries, freelist)
			if err != nil {

				fmt.Printf("Can`t open/create db file error | File [%s] | DB [%s] | %v\n", file, dbf, err)
				keymutex.UnLock(dbf)

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

			err = os.Chmod(dbf, bfilemode)
			if err != nil {

				fmt.Printf("Can`t chmod db error | DB [%s] | %v\n", dbf, err)
				db.Close()
				keymutex.UnLock(dbf)

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

			// Keys Index Bucket

			err = db.Update(func(tx *bolt.Tx) error {
				_, err = tx.CreateBucketIfNotExists([]byte(ibucket))
				if err != nil {
					return err
				}
				return nil

			})
			if err != nil {

				fmt.Printf("Can`t create index db bucket error | File [%s] | DB [%s] | %v\n", file, dbf, err)
				db.Close()
				keymutex.UnLock(dbf)

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

			// Keys Size Bucket

			err = db.Update(func(tx *bolt.Tx) error {
				_, err = tx.CreateBucketIfNotExists([]byte(sbucket))
				if err != nil {
					return err
				}
				return nil

			})
			if err != nil {

				fmt.Printf("Can`t create size db bucket error | File [%s] | DB [%s] | %v\n", file, dbf, err)
				db.Close()
				keymutex.UnLock(dbf)

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

			// Keys Time Bucket

			err = db.Update(func(tx *bolt.Tx) error {
				_, err = tx.CreateBucketIfNotExists([]byte(tbucket))
				if err != nil {
					return err
				}
				return nil

			})
			if err != nil {

				fmt.Printf("Can`t create time db bucket error | File [%s] | DB [%s] | %v\n", file, dbf, err)
				db.Close()
				keymutex.UnLock(dbf)

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

			// Buckets Internal Sharding Bucket

			err = db.Update(func(tx *bolt.Tx) error {
				_, err = tx.CreateBucketIfNotExists([]byte(cbucket))
				if err != nil {
					return err
				}
				return nil

			})
			if err != nil {

				fmt.Printf("Can`t create count db bucket error | File [%s] | DB [%s] | %v\n", file, dbf, err)
				db.Close()
				keymutex.UnLock(dbf)

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

			keyexists, err := KeyExists(db, ibucket, file)
			if err != nil {

				fmt.Printf("Can`t check key of file in index db bucket error | File [%s] | DB [%s] | %v\n", file, dbf, err)
				db.Close()
				keymutex.UnLock(dbf)

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

			if keyexists != "" && !overwrite {

				if fdelete {

					db.Close()
					keymutex.UnLock(dbf)

					err = pfile.Close()
					if err != nil {

						fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)

						if ignore {
							continue
						}

						return

					}

					err = RemoveFile(abs)
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
				keymutex.UnLock(dbf)

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

			keybucket, err := BucketCount(db, cbucket)
			if err != nil {

				fmt.Printf("Can`t count buckets in count db bucket error | DB [%s] | %v\n", dbf, err)
				db.Close()
				keymutex.UnLock(dbf)

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

			if keybucket > uint64(0) && keyexists == "" {

				lastbucket := fmt.Sprintf("wzd%d", keybucket)

				keyscount, err = KeysCountBucket(db, lastbucket)
				if err != nil {

					fmt.Printf("Can`t count keys of files in last db bucket error | DB [%s] | %v\n", dbf, err)
					db.Close()
					keymutex.UnLock(dbf)

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

				keysbytes, err = BucketStats(db, lastbucket)
				if err != nil {

					fmt.Printf("Can`t count bytes of files in last db bucket error | DB [%s] | %v\n", dbf, err)
					db.Close()
					keymutex.UnLock(dbf)

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

				if keyscount >= perbucket || keysbytes >= 536870912 {

					bucket = fmt.Sprintf("wzd%d", keybucket+1)

					nb := make([]byte, 8)
					Endian.PutUint64(nb, keybucket+1)

					err = db.Update(func(tx *bolt.Tx) error {

						verr := errors.New("count bucket not exists")

						b := tx.Bucket([]byte(cbucket))
						if b != nil {
							err = b.Put([]byte("counter"), nb)
							if err != nil {
								return err
							}

						} else {
							return verr
						}

						return nil

					})
					if err != nil {

						fmt.Printf("Can`t write bucket counter to count db bucket error | File [%s] | DB [%s] | %v\n", file, dbf, err)
						db.Close()
						keymutex.UnLock(dbf)

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
					bucket = lastbucket
				}

			} else if keyexists != "" && overwrite {

				bucket = keyexists

				keyscount, err = KeysCountBucket(db, bucket)
				if err != nil {

					fmt.Printf("Can`t count keys of files in last db bucket error | DB [%s] | %v\n", dbf, err)
					db.Close()
					keymutex.UnLock(dbf)

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

				keysbytes, err = BucketStats(db, bucket)
				if err != nil {

					fmt.Printf("Can`t count bytes of files in last db bucket error | DB [%s] | %v\n", dbf, err)
					db.Close()
					keymutex.UnLock(dbf)

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

				nb := make([]byte, 8)
				Endian.PutUint64(nb, uint64(1))

				err = db.Update(func(tx *bolt.Tx) error {

					verr := errors.New("count bucket not exists")

					b := tx.Bucket([]byte(cbucket))
					if b != nil {
						err = b.Put([]byte("counter"), nb)
						if err != nil {
							return err
						}

					} else {
						return verr
					}

					return nil

				})
				if err != nil {

					fmt.Printf("Can`t write bucket counter to count db bucket error | File [%s] | DB [%s] | %v\n", file, dbf, err)
					db.Close()
					keymutex.UnLock(dbf)

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
				_, err = tx.CreateBucketIfNotExists([]byte(bucket))
				if err != nil {
					return err
				}

				return nil

			})
			if err != nil {

				fmt.Printf("Can`t create db bucket error | Bucket [%s] | File [%s] | DB [%s] | %v\n", bucket, file, dbf, err)
				db.Close()
				keymutex.UnLock(dbf)

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

			rawbuffer := new(bytes.Buffer)

			_, err = rawbuffer.ReadFrom(pfile)
			if err != nil && err != io.EOF {

				fmt.Printf("Can`t read file to rawbuffer data error | File [%s] | Path [%s] | %v\n", file, abs, err)
				db.Close()
				keymutex.UnLock(dbf)
				rawbuffer.Reset()

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

				crcdata := new(bytes.Buffer)

				_, err = crcdata.ReadFrom(tee)
				if err != nil && err != io.EOF {

					fmt.Printf("Can`t read tee crc data error | File [%s] | Path [%s] | %v\n", file, abs, err)
					db.Close()
					keymutex.UnLock(dbf)
					rawbuffer.Reset()
					readbuffer.Reset()
					crcdata.Reset()

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

				wcrc = crc32.Checksum(crcdata.Bytes(), ctbl32)

				crcdata.Reset()

				head := Header{
					Size: uint64(size), Date: uint64(tmst), Mode: uint16(vfilemode), Uuid: uint16(Uid), Guid: uint16(Gid), Comp: uint8(0), Encr: uint8(0), Crcs: wcrc, Rsvr: uint64(0),
				}

				err = binary.Write(endbuffer, Endian, head)
				if err != nil {

					fmt.Printf("Write binary header data to db error | File [%s] | DB [%s] | Header [%v] | %v\n", file, dbf, head, err)
					db.Close()
					keymutex.UnLock(dbf)
					rawbuffer.Reset()
					readbuffer.Reset()
					endbuffer.Reset()
					//					head = Header{}

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

				//				head = Header{}

				_, err = endbuffer.ReadFrom(&readbuffer)
				if err != nil && err != io.EOF {

					fmt.Printf("Can`t read readbuffer data error | File [%s] | DB [%s] | %v\n", file, dbf, err)
					db.Close()
					keymutex.UnLock(dbf)
					rawbuffer.Reset()
					readbuffer.Reset()
					endbuffer.Reset()

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

				rawbuffer.Reset()
				readbuffer.Reset()

			} else {

				head := Header{
					Size: uint64(size), Date: uint64(tmst), Mode: uint16(vfilemode), Uuid: uint16(Uid), Guid: uint16(Gid), Comp: uint8(0), Encr: uint8(0), Crcs: wcrc, Rsvr: uint64(0),
				}

				err = binary.Write(endbuffer, Endian, head)
				if err != nil {

					fmt.Printf("Write binary header data to db error | File [%s] | DB [%s] | Header [%v] | %v\n", file, dbf, head, err)
					db.Close()
					keymutex.UnLock(dbf)
					rawbuffer.Reset()
					endbuffer.Reset()
					//					head = Header{}

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

				//				head = Header{}

				_, err = endbuffer.ReadFrom(rawbuffer)
				if err != nil && err != io.EOF {

					fmt.Printf("Can`t read rawbuffer data error | File [%s] | DB [%s] | %v\n", file, dbf, err)
					db.Close()
					keymutex.UnLock(dbf)
					rawbuffer.Reset()
					endbuffer.Reset()

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

				rawbuffer.Reset()

			}

			err = db.Update(func(tx *bolt.Tx) error {

				verr := errors.New("bucket not exists")

				b := tx.Bucket([]byte(bucket))
				if b != nil {
					err = b.Put([]byte(file), endbuffer.Bytes())
					if err != nil {
						return err
					}

				} else {
					return verr
				}

				return nil

			})
			if err != nil {

				fmt.Printf("Can`t write a file to db error | File [%s] | DB [%s] | %v\n", file, dbf, err)
				db.Close()
				keymutex.UnLock(dbf)
				endbuffer.Reset()

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

				verr := errors.New("index bucket not exists")

				b := tx.Bucket([]byte(ibucket))
				if b != nil {
					err = b.Put([]byte(file), []byte(bucket))
					if err != nil {
						return err
					}

				} else {
					return verr
				}

				return nil

			})
			if err != nil {

				fmt.Printf("Can`t write key to index db bucket error | File [%s] | DB [%s] | %v\n", file, dbf, err)
				db.Close()
				keymutex.UnLock(dbf)
				endbuffer.Reset()

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

				verr := errors.New("size bucket not exists")

				b := tx.Bucket([]byte(sbucket))
				if b != nil {
					err = b.Put([]byte(file), sb)
					if err != nil {
						return err
					}

				} else {
					return verr
				}

				return nil

			})
			if err != nil {

				fmt.Printf("Can`t write key to size db bucket error | File [%s] | DB [%s] | %v\n", file, dbf, err)
				db.Close()
				keymutex.UnLock(dbf)
				endbuffer.Reset()

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

				verr := errors.New("time bucket not exists")

				b := tx.Bucket([]byte(tbucket))
				if b != nil {
					err = b.Put([]byte(file), tb)
					if err != nil {
						return err
					}

				} else {
					return verr
				}

				return nil

			})
			if err != nil {

				fmt.Printf("Can`t write key to time db bucket error | File [%s] | DB [%s] | %v\n", file, dbf, err)
				db.Close()
				keymutex.UnLock(dbf)
				endbuffer.Reset()

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

			endbuffer.Reset()

			err = pfile.Close()
			if err != nil {

				fmt.Printf("Close after read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
				keymutex.UnLock(dbf)

				if ignore {
					continue
				}

				return

			}

			if !disablewriteintegrity {

				var pdata []byte

				err = db.View(func(tx *bolt.Tx) error {

					verr := errors.New("bucket not exists")

					b := tx.Bucket([]byte(bucket))
					if b != nil {
						pdata = b.Get([]byte(file))
						return nil
					} else {
						return verr
					}

				})
				if err != nil {

					fmt.Printf("Can`t get data by key from db error | File [%s] | DB [%s] | %v\n", file, dbf, err)
					db.Close()
					keymutex.UnLock(dbf)
					pdata = nil

					if ignore {
						continue
					}

					return

				}

				pread := bytes.NewReader(pdata)

				var readhead Header

				headbuffer := make([]byte, 36)

				hsizebuffer, err := pread.Read(headbuffer)
				if err != nil {

					fmt.Printf("Read binary header data from db error | File [%s] | DB [%s] | Header Buffer [%p] | %v\n", file, dbf, headbuffer, err)
					db.Close()
					keymutex.UnLock(dbf)
					pdata = nil
					headbuffer = nil

					if ignore {
						continue
					}

					return

				}

				hread := bytes.NewReader(headbuffer[:hsizebuffer])

				err = binary.Read(hread, Endian, &readhead)
				if err != nil {

					fmt.Printf("Read binary header data from db error | File [%s] | DB [%s] | Header Buffer [%p] | %v\n", file, dbf, hread, err)
					db.Close()
					keymutex.UnLock(dbf)
					pdata = nil
					headbuffer = nil
					readhead = Header{}

					if ignore {
						continue
					}

					return

				}

				headbuffer = nil
				readhead = Header{}

				rcrcdata := new(bytes.Buffer)

				_, err = rcrcdata.ReadFrom(pread)
				if err != nil && err != io.EOF {

					fmt.Printf("Can`t read pread data error | File [%s] | DB [%s] | %v\n", file, dbf, err)
					db.Close()
					keymutex.UnLock(dbf)
					pdata = nil
					rcrcdata.Reset()

					if ignore {
						continue
					}

					return

				}

				rcrc = crc32.Checksum(rcrcdata.Bytes(), ctbl32)

				pdata = nil
				rcrcdata.Reset()

				if wcrc != rcrc {

					fmt.Printf("CRC read file error | File [%s] | DB [%s] | Have CRC [%v] | Awaiting CRC [%v]\n", file, dbf, rcrc, wcrc)
					db.Close()
					keymutex.UnLock(dbf)

					if ignore {
						continue
					}

					return

				}

			}

			db.Close()
			keymutex.UnLock(dbf)

			if keyexists != "" {

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

		elapsed := float64(time.Since(start)) / float64(time.Millisecond)

		if verbose {
			fmt.Printf("Packing file | File [%s] | Path [%s] | Bucket [%s] | Past Count [%d] | Past Bytes [%d] | Elapsed [%f] | DB [%s]\n", file, abs, bucket, keyscount, keysbytes, elapsed, dbf)
		}

		if fdelete {

			err = RemoveFile(abs)
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
		fmt.Printf("Read lines from list file error | List [%s] | %v\n", list, err)
		return
	}

}

// ZAPackSingle : packing single file to bolt archive
func ZAPackSingle() {
	defer wg.Done()

	var err error

	// Wait Group

	wg.Add(1)

	start := time.Now()

	var vfilemode uint64

	keyscount := 0
	keysbytes := 0

	uri := single

	dir := filepath.Dir(uri)
	file := filepath.Base(uri)

	if dir == "." || dir == "/" {
		pwd, err := os.Getwd()
		if err != nil {
			fmt.Printf("Can`t get current directory error | File [%s] | Path [%s] | %v\n", file, pwd, err)
			os.Exit(1)
		}

		if pwd == "/" {
			fmt.Printf("Can`t add a file to bolt archive in root/chroot filesystem directory restricted error | File [%s] | Path [%s] | %v\n", file, pwd, err)
			os.Exit(1)
		}

		dir = pwd

	}

	abs := filepath.Clean(dir + "/" + file)

	dbn := filepath.Base(dir)
	dbf := filepath.Clean(dir + "/" + dbn + ".bolt")

	bucket := "wzd1"
	ibucket := "index"
	sbucket := "size"
	tbucket := "time"
	cbucket := "count"

	timeout := time.Duration(locktimeout) * time.Second

	rgxbolt := regexp.MustCompile(`(\.bolt$)`)
	rgxcrcbolt := regexp.MustCompile(`(\.crcbolt$)`)

	if DirExists(abs) {
		fmt.Printf("Skipping directory add to bolt | Directory [%s]\n", abs)
		os.Exit(0)
	}

	mchregbolt := rgxbolt.MatchString(file)
	mchregcrcbolt := rgxcrcbolt.MatchString(file)

	if mchregbolt || mchregcrcbolt {
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

	sb := make([]byte, 8)
	Endian.PutUint64(sb, uint64(size))

	modt := infile.ModTime()
	tmst := modt.Unix()

	tb := make([]byte, 8)
	Endian.PutUint64(tb, uint64(tmst))

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

	switch {
	case size >= 262144 && size < 1048576:
		perbucket = 512
	case size >= 1048576 && size < 4194304:
		perbucket = 256
	case size >= 4194304 && size < 8388608:
		perbucket = 128
	case size >= 8388608 && size < 16777216:
		perbucket = 64
	case size >= 16777216:
		perbucket = 32
	}

	var bf BoltFiles
	var bfiles []BoltFiles

	var bcount int64 = 0
	var ncount int64 = 0
	var dcount int64 = 0

	if FileExists(dbf) {

		bf.Name = dbf
		bfiles = append(bfiles, bf)

		bcount++

	}

	for {

		dcount++
		ndbf := fmt.Sprintf("%s/%s_%08d.bolt", dir, dbn, dcount)

		if FileExists(ndbf) {

			bcount++

			bf.Name = ndbf
			bfiles = append(bfiles, bf)

		} else {
			break
		}

	}

	if bcount == 0 {

		db, err := BoltOpenWrite(dbf, bfilemode, timeout, opentries, freelist)
		if err != nil {
			fmt.Printf("Can`t open/create db file error | File [%s] | DB [%s] | %v\n", file, dbf, err)
			os.Exit(1)
		}

		err = os.Chmod(dbf, filemode)
		if err != nil {
			fmt.Printf("Can`t chmod db error | DB [%s] | %v\n", dbf, err)
			db.Close()
			os.Exit(1)
		}

		// Keys Index Bucket

		err = db.Update(func(tx *bolt.Tx) error {
			_, err = tx.CreateBucketIfNotExists([]byte(ibucket))
			if err != nil {
				return err
			}
			return nil

		})
		if err != nil {
			fmt.Printf("Can`t create index db bucket error | File [%s] | DB [%s] | %v\n", file, dbf, err)
			db.Close()
			os.Exit(1)
		}

		db.Close()

		bf.Name = dbf
		bfiles = append(bfiles, bf)

		bcount++

	}

	for _, bfile := range bfiles {

		ncount++

		dbf = bfile.Name

		lndb, err := os.Lstat(dbf)
		if err != nil {
			fmt.Printf("Can`t stat db file error | File [%s] | DB [%s] | %v\n", file, dbf, err)
			os.Exit(1)
		}

		if lndb.Mode()&os.ModeType != 0 {
			fmt.Printf("Non-regular db file error | File [%s] | DB [%s] | %v\n", file, dbf, err)
			os.Exit(1)
		}

		indb, err := os.Stat(dbf)
		if err != nil {
			fmt.Printf("Can`t stat db file error | File [%s] | DB [%s] | %v\n", file, dbf, err)
			os.Exit(1)
		}

		bsize := indb.Size()

		db, err := BoltOpenRead(dbf, bfilemode, timeout, opentries, freelist)
		if err != nil {
			fmt.Printf("Can`t open db file error | File [%s] | DB [%s] | %v\n", file, dbf, err)
			os.Exit(1)
		}

		keyexists, err := KeyExists(db, ibucket, file)
		if err != nil {
			fmt.Printf("Can`t check key of file in index db bucket error | File [%s] | DB [%s] | %v\n", file, dbf, err)
			db.Close()
			os.Exit(1)
		}

		if keyexists != "" {
			db.Close()
			break
		}

		if ncount == bcount {

			keyscnt, err := KeysCount(db, ibucket)
			if err != nil {
				fmt.Printf("Can`t count keys of all files in index db bucket error | File [%s] | DB [%s] | %v", file, dbf, err)
				db.Close()
				os.Exit(1)
			}

			if bsize >= smaxsize || keyscnt >= skeyscnt {

				db.Close()
				dbf = fmt.Sprintf("%s/%s_%08d.bolt", dir, dbn, ncount)
				bucket = "wzd1"
				break

			}

		}

		db.Close()

	}

	pfile, err := os.OpenFile(abs, os.O_RDONLY, os.ModePerm)
	if err != nil {
		fmt.Printf("Can`t open file error | File [%s] | Path [%s] | %v\n", file, abs, err)
		os.Exit(1)

	}
	defer pfile.Close()

	rcrc := uint32(0)
	wcrc := uint32(0)

	db, err := BoltOpenWrite(dbf, bfilemode, timeout, opentries, freelist)
	if err != nil {

		fmt.Printf("Can`t open/create db file error | File [%s] | DB [%s] | %v\n", file, dbf, err)

		err = pfile.Close()
		if err != nil {
			fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
			os.Exit(1)
		}

		os.Exit(1)

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

	// Keys Index Bucket

	err = db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists([]byte(ibucket))
		if err != nil {
			return err
		}
		return nil

	})
	if err != nil {

		fmt.Printf("Can`t create index db bucket error | File [%s] | DB [%s] | %v\n", file, dbf, err)
		db.Close()

		err = pfile.Close()
		if err != nil {
			fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
			os.Exit(1)
		}

		os.Exit(1)

	}

	// Keys Size Bucket

	err = db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists([]byte(sbucket))
		if err != nil {
			return err
		}
		return nil

	})
	if err != nil {

		fmt.Printf("Can`t create size db bucket error | File [%s] | DB [%s] | %v\n", file, dbf, err)
		db.Close()

		err = pfile.Close()
		if err != nil {
			fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
			os.Exit(1)
		}

		os.Exit(1)

	}

	// Keys Time Bucket

	err = db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists([]byte(tbucket))
		if err != nil {
			return err
		}
		return nil

	})
	if err != nil {

		fmt.Printf("Can`t create time db bucket error | File [%s] | DB [%s] | %v\n", file, dbf, err)
		db.Close()

		err = pfile.Close()
		if err != nil {
			fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
			os.Exit(1)
		}

		os.Exit(1)

	}

	// Buckets Internal Sharding Bucket

	err = db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists([]byte(cbucket))
		if err != nil {
			return err
		}
		return nil

	})
	if err != nil {

		fmt.Printf("Can`t create count db bucket error | File [%s] | DB [%s] | %v\n", file, dbf, err)
		db.Close()

		err = pfile.Close()
		if err != nil {
			fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
			os.Exit(1)
		}

		os.Exit(1)

	}

	keyexists, err := KeyExists(db, ibucket, file)
	if err != nil {

		fmt.Printf("Can`t check index key of file in index db bucket error | File [%s] | DB [%s] | %v\n", file, dbf, err)
		db.Close()

		err = pfile.Close()
		if err != nil {
			fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
			os.Exit(1)
		}

		os.Exit(1)

	}

	if keyexists != "" && !overwrite {

		if fdelete {

			db.Close()
			err = pfile.Close()
			if err != nil {
				fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
				os.Exit(1)
			}

			err = RemoveFile(abs)
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

	keybucket, err := BucketCount(db, cbucket)
	if err != nil {

		fmt.Printf("Can`t count buckets in count db bucket error | DB [%s] | %v\n", dbf, err)
		db.Close()

		err = pfile.Close()
		if err != nil {
			fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
			os.Exit(1)
		}

		os.Exit(1)

	}

	if keybucket > uint64(0) && keyexists == "" {

		lastbucket := fmt.Sprintf("wzd%d", keybucket)

		keyscount, err := KeysCountBucket(db, lastbucket)
		if err != nil {

			fmt.Printf("Can`t count keys of files in last db bucket error | DB [%s] | %v\n", dbf, err)
			db.Close()

			err = pfile.Close()
			if err != nil {
				fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
				os.Exit(1)
			}

			os.Exit(1)

		}

		keysbytes, err = BucketStats(db, lastbucket)
		if err != nil {

			fmt.Printf("Can`t count bytes of files in last db bucket error | DB [%s] | %v\n", dbf, err)
			db.Close()

			err = pfile.Close()
			if err != nil {
				fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
				os.Exit(1)
			}

			os.Exit(1)

		}

		if keyscount >= perbucket || keysbytes >= 536870912 {

			bucket = fmt.Sprintf("wzd%d", keybucket+1)

			nb := make([]byte, 8)
			Endian.PutUint64(nb, keybucket+1)

			err = db.Update(func(tx *bolt.Tx) error {

				verr := errors.New("count bucket not exists")

				b := tx.Bucket([]byte(cbucket))
				if b != nil {
					err = b.Put([]byte("counter"), nb)
					if err != nil {
						return err
					}

				} else {
					return verr
				}

				return nil

			})
			if err != nil {

				fmt.Printf("Can`t write bucket counter to count db bucket error | File [%s] | DB [%s] | %v\n", file, dbf, err)
				db.Close()

				err = pfile.Close()
				if err != nil {
					fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
					os.Exit(1)
				}

				os.Exit(1)

			}

		} else {
			bucket = lastbucket
		}

	} else if keyexists != "" && overwrite {

		bucket = keyexists

	} else {

		nb := make([]byte, 8)
		Endian.PutUint64(nb, uint64(1))

		err = db.Update(func(tx *bolt.Tx) error {

			verr := errors.New("count bucket not exists")

			b := tx.Bucket([]byte(cbucket))
			if b != nil {
				err = b.Put([]byte("counter"), nb)
				if err != nil {
					return err
				}

			} else {
				return verr
			}

			return nil

		})
		if err != nil {

			fmt.Printf("Can`t write bucket counter to count db bucket error | File [%s] | DB [%s] | %v\n", file, dbf, err)
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
		_, err = tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}
		return nil

	})
	if err != nil {

		fmt.Printf("Can`t create db bucket error | Bucket [%s] | File [%s] | DB [%s] | %v\n", bucket, file, dbf, err)
		db.Close()

		err = pfile.Close()
		if err != nil {
			fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
			os.Exit(1)
		}

		os.Exit(1)

	}

	rawbuffer := new(bytes.Buffer)

	_, err = rawbuffer.ReadFrom(pfile)
	if err != nil && err != io.EOF {

		fmt.Printf("Can`t read file to rawbuffer data error | File [%s] | Path [%s] | %v\n", file, abs, err)
		db.Close()
		rawbuffer.Reset()

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

		crcdata := new(bytes.Buffer)

		_, err = crcdata.ReadFrom(tee)
		if err != nil && err != io.EOF {

			fmt.Printf("Can`t read tee crc data error | File [%s] | Path [%s] | %v\n", file, abs, err)
			db.Close()
			rawbuffer.Reset()
			readbuffer.Reset()
			crcdata.Reset()

			err = pfile.Close()
			if err != nil {
				fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
				os.Exit(1)
			}

			os.Exit(1)

		}

		wcrc = crc32.Checksum(crcdata.Bytes(), ctbl32)

		crcdata.Reset()

		head := Header{
			Size: uint64(size), Date: uint64(tmst), Mode: uint16(vfilemode), Uuid: uint16(Uid), Guid: uint16(Gid), Comp: uint8(0), Encr: uint8(0), Crcs: wcrc, Rsvr: uint64(0),
		}

		err = binary.Write(endbuffer, Endian, head)
		if err != nil {

			fmt.Printf("Write binary header data to db error | File [%s] | DB [%s] | Header [%v] | %v\n", file, dbf, head, err)
			db.Close()
			rawbuffer.Reset()
			readbuffer.Reset()
			endbuffer.Reset()
			//			head = Header{}

			err = pfile.Close()
			if err != nil {
				fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
				os.Exit(1)
			}

			os.Exit(1)

		}

		//		head = Header{}

		_, err = endbuffer.ReadFrom(&readbuffer)
		if err != nil && err != io.EOF {

			fmt.Printf("Can`t read readbuffer data error | File [%s] | DB [%s] | %v\n", file, dbf, err)
			db.Close()
			rawbuffer.Reset()
			readbuffer.Reset()
			endbuffer.Reset()

			err = pfile.Close()
			if err != nil {
				fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
				os.Exit(1)
			}

			os.Exit(1)

		}

		rawbuffer.Reset()
		readbuffer.Reset()

	} else {

		head := Header{
			Size: uint64(size), Date: uint64(tmst), Mode: uint16(vfilemode), Uuid: uint16(Uid), Guid: uint16(Gid), Comp: uint8(0), Encr: uint8(0), Crcs: wcrc, Rsvr: uint64(0),
		}

		err = binary.Write(endbuffer, Endian, head)
		if err != nil {

			fmt.Printf("Write binary header data to db error | File [%s] | DB [%s] | Header [%v] | %v\n", file, dbf, head, err)
			db.Close()
			rawbuffer.Reset()
			endbuffer.Reset()
			//			head = Header{}

			err = pfile.Close()
			if err != nil {
				fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
				os.Exit(1)
			}

			os.Exit(1)

		}

		//		head = Header{}

		_, err = endbuffer.ReadFrom(rawbuffer)
		if err != nil && err != io.EOF {

			fmt.Printf("Can`t read rawbuffer data error | File [%s] | DB [%s] | %v\n", file, dbf, err)
			db.Close()
			rawbuffer.Reset()
			endbuffer.Reset()

			err = pfile.Close()
			if err != nil {
				fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
				os.Exit(1)
			}

			os.Exit(1)

		}

		rawbuffer.Reset()

	}

	err = db.Update(func(tx *bolt.Tx) error {

		verr := errors.New("bucket not exists")

		b := tx.Bucket([]byte(bucket))
		if b != nil {
			err = b.Put([]byte(file), endbuffer.Bytes())
			if err != nil {
				return err
			}

		} else {
			return verr
		}

		return nil

	})
	if err != nil {

		fmt.Printf("Can`t write a file to db error | File [%s] | DB [%s] | %v\n", file, dbf, err)
		db.Close()
		endbuffer.Reset()

		err = pfile.Close()
		if err != nil {
			fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
			os.Exit(1)
		}

		os.Exit(1)

	}

	err = db.Update(func(tx *bolt.Tx) error {

		verr := errors.New("index bucket not exists")

		b := tx.Bucket([]byte(ibucket))
		if b != nil {
			err = b.Put([]byte(file), []byte(bucket))
			if err != nil {
				return err
			}

		} else {
			return verr
		}

		return nil

	})
	if err != nil {

		fmt.Printf("Can`t write key to index db bucket error | File [%s] | DB [%s] | %v\n", file, dbf, err)
		db.Close()
		endbuffer.Reset()

		err = pfile.Close()
		if err != nil {
			fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
			os.Exit(1)
		}

		os.Exit(1)

	}

	err = db.Update(func(tx *bolt.Tx) error {

		verr := errors.New("size bucket not exists")

		b := tx.Bucket([]byte(sbucket))
		if b != nil {
			err = b.Put([]byte(file), sb)
			if err != nil {
				return err
			}

		} else {
			return verr
		}

		return nil

	})
	if err != nil {

		fmt.Printf("Can`t write key to size db bucket error | File [%s] | DB [%s] | %v\n", file, dbf, err)
		db.Close()
		endbuffer.Reset()

		err = pfile.Close()
		if err != nil {
			fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
			os.Exit(1)
		}

		os.Exit(1)

	}

	err = db.Update(func(tx *bolt.Tx) error {

		verr := errors.New("time bucket not exists")

		b := tx.Bucket([]byte(tbucket))
		if b != nil {
			err = b.Put([]byte(file), tb)
			if err != nil {
				return err
			}

		} else {
			return verr
		}

		return nil

	})
	if err != nil {

		fmt.Printf("Can`t write key to time db bucket error | File [%s] | DB [%s] | %v\n", file, dbf, err)
		db.Close()
		endbuffer.Reset()

		err = pfile.Close()
		if err != nil {
			fmt.Printf("Close full read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
			os.Exit(1)
		}

		os.Exit(1)

	}

	endbuffer.Reset()

	err = pfile.Close()
	if err != nil {
		fmt.Printf("Close after read file error | File [%s] | Path [%s] | %v\n", file, abs, err)
		os.Exit(1)
	}

	if !disablewriteintegrity {

		var pdata []byte

		err = db.View(func(tx *bolt.Tx) error {

			verr := errors.New("bucket not exists")

			b := tx.Bucket([]byte(bucket))
			if b != nil {
				pdata = b.Get([]byte(file))
				return nil
			} else {
				return verr
			}

		})
		if err != nil {

			fmt.Printf("Can`t get data by key from db error | File [%s] | DB [%s] | %v\n", file, dbf, err)
			db.Close()
			pdata = nil

			os.Exit(1)

		}

		pread := bytes.NewReader(pdata)

		var readhead Header

		headbuffer := make([]byte, 36)

		hsizebuffer, err := pread.Read(headbuffer)
		if err != nil {

			fmt.Printf("Read binary header data from db error | File [%s] | DB [%s] | Header Buffer [%p] | %v\n", file, dbf, headbuffer, err)
			db.Close()
			pdata = nil
			headbuffer = nil

			os.Exit(1)

		}

		hread := bytes.NewReader(headbuffer[:hsizebuffer])

		err = binary.Read(hread, Endian, &readhead)
		if err != nil {

			fmt.Printf("Read binary header data from db error | File [%s] | DB [%s] | Header Buffer [%p] | %v\n", file, dbf, hread, err)
			db.Close()
			pdata = nil
			headbuffer = nil
			readhead = Header{}

			os.Exit(1)

		}

		headbuffer = nil
		readhead = Header{}

		rcrcdata := new(bytes.Buffer)

		_, err = rcrcdata.ReadFrom(pread)
		if err != nil && err != io.EOF {

			fmt.Printf("Can`t read pread data error | File [%s] | DB [%s] | %v\n", file, dbf, err)
			db.Close()
			pdata = nil
			rcrcdata.Reset()

			os.Exit(1)

		}

		rcrc = crc32.Checksum(rcrcdata.Bytes(), ctbl32)

		pdata = nil
		rcrcdata.Reset()

		if wcrc != rcrc {

			fmt.Printf("CRC read file error | File [%s] | DB [%s] | Have CRC [%v] | Awaiting CRC [%v]\n", file, dbf, rcrc, wcrc)
			db.Close()
			os.Exit(1)

		}

		if keyexists != "" {

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

	if keyexists != "" {

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

	elapsed := float64(time.Since(start)) / float64(time.Millisecond)

	if verbose {
		fmt.Printf("Packing file | File [%s] | Path [%s] | Bucket [%s] | Past Count [%d] | Past Bytes [%d] | Elapsed [%f] | DB [%s]\n", file, abs, bucket, keyscount, keysbytes, elapsed, dbf)
	}

	if fdelete {

		err = RemoveFile(abs)
		if err != nil {
			fmt.Printf("Can`t remove file error | File [%s] | Path [%s] | %v\n", file, abs, err)
			os.Exit(1)
		}

		if verbose {
			fmt.Printf("Deleting file | File [%s] | Path [%s]\n", file, abs)
		}

	}

}
