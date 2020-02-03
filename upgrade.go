package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/eltaline/bolt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"time"
)

// ZAUpgrade : upgrade bolt archives to new version
func ZAUpgrade() {
	defer wg.Done()

	// Wait Group

	wg.Add(1)

	lfile, err := os.OpenFile(list, os.O_RDONLY, os.ModePerm)
	if err != nil {
		fmt.Printf("Can`t open list file error | List [%s] | %v\n", list, err)
		os.Exit(1)
	}
	defer lfile.Close()

	bucket := ""
	ibucket := "index"
	sbucket := "size"
	tbucket := "time"

	timeout := time.Duration(locktimeout) * time.Second

	rgxbolt := regexp.MustCompile(`(\.bolt$)`)

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

		db, err := BoltOpenWrite(dbf, bfilemode, timeout, opentries, freelist)
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

		// Keys Size Bucket

		err = db.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists([]byte(sbucket))
			if err != nil {
				return err
			}
			return nil

		})
		if err != nil {

			fmt.Printf("Can`t create size db bucket error | File [%s] | DB [%s] | %v\n", file, dbf, err)
			db.Close()

			if ignore {
				continue
			}

			return

		}

		// Keys Time Bucket

		err = db.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists([]byte(tbucket))
			if err != nil {
				return err
			}
			return nil

		})
		if err != nil {

			fmt.Printf("Can`t create time db bucket error | File [%s] | DB [%s] | %v\n", file, dbf, err)
			db.Close()

			if ignore {
				continue
			}

			return

		}

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

			sdata := int64(-1)
			tdata := int64(-1)

			err = db.View(func(tx *bolt.Tx) error {

				verr := errors.New("size bucket not exists")

				b := tx.Bucket([]byte(sbucket))
				if b != nil {
					val := b.Get([]byte(rkey))
					if val != nil {
						sdata = int64(Endian.Uint64(val))
					}
					return nil
				} else {
					return verr
				}

			})
			if err != nil {

				fmt.Printf("Can`t get data by key from size db bucket error | File [%s] | DB [%s] | %v\n", rkey, dbf, err)

				if ignore {
					continue
				}

				db.Close()
				return

			}

			err = db.View(func(tx *bolt.Tx) error {

				verr := errors.New("time bucket not exists")

				b := tx.Bucket([]byte(tbucket))
				if b != nil {
					val := b.Get([]byte(rkey))
					if val != nil {
						tdata = int64(Endian.Uint64(val))
					}
					return nil
				} else {
					return verr
				}

			})
			if err != nil {

				fmt.Printf("Can`t get data by key from time db bucket error | File [%s] | DB [%s] | %v\n", rkey, dbf, err)

				if ignore {
					continue
				}

				db.Close()
				return

			}

			if sdata != -1 {
				continue
			}

			if tdata != -1 {
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

			var readhead OldHeader

			headbuffer := make([]byte, 32)

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
				readhead = OldHeader{}

				if ignore {
					continue
				}

				db.Close()
				return

			}

			headbuffer = nil

			sb := make([]byte, 8)
			Endian.PutUint64(sb, readhead.Size)

			tb := make([]byte, 8)
			Endian.PutUint64(tb, uint64(readhead.Date))

			nsize := readhead.Size
			ntmst := uint64(readhead.Date)
			nvfilemode := readhead.Mode
			nUid := readhead.Uuid
			nGid := readhead.Guid
			nwcrc := readhead.Crcs

			readhead = OldHeader{}

			endbuffer := new(bytes.Buffer)

			head := Header{
				Size: nsize, Date: ntmst, Mode: nvfilemode, Uuid: nUid, Guid: nGid, Comp: uint8(0), Encr: uint8(0), Crcs: nwcrc, Rsvr: uint64(0),
			}

			err = binary.Write(endbuffer, Endian, head)
			if err != nil {

				fmt.Printf("Write binary header data to db error | File [%s] | DB [%s] | Header [%v] | %v\n", rkey, dbf, head, err)
				pdata = nil
				endbuffer.Reset()
				//                                      head = Header{}

				if ignore {
					continue
				}

				db.Close()
				return

			}

			fmt.Printf("Reading/Writing header | File [%s] | Size [%d] | Date [%d] | Crcs [%d] | DB [%s]\n", rkey, nsize, ntmst, nwcrc, dbf)

			_, err = endbuffer.ReadFrom(pread)
			if err != nil && err != io.EOF {

				fmt.Printf("Can`t read readbuffer data error | File [%s] | DB [%s] | %v\n", rkey, dbf, err)
				pdata = nil
				endbuffer.Reset()
				//                                      head = Header{}

				if ignore {
					continue
				}

				db.Close()
				return

			}

			err = db.Update(func(tx *bolt.Tx) error {

				verr := errors.New("bucket not exists")

				b := tx.Bucket([]byte(bucket))
				if b != nil {
					err = b.Put([]byte(rkey), endbuffer.Bytes())
					if err != nil {
						return err
					}

				} else {
					return verr
				}

				return nil

			})
			if err != nil {

				fmt.Printf("Can`t write a file to db error | File [%s] | DB [%s] | %v\n", rkey, dbf, err)

				pdata = nil
				endbuffer.Reset()

				if ignore {
					continue
				}

				db.Close()
				return

			}

			pdata = nil
			endbuffer.Reset()

			err = db.Update(func(tx *bolt.Tx) error {

				verr := errors.New("size bucket not exists")

				b := tx.Bucket([]byte(sbucket))
				if b != nil {
					err = b.Put([]byte(rkey), sb)
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

				if ignore {
					continue
				}

				db.Close()
				return

			}

			err = db.Update(func(tx *bolt.Tx) error {

				verr := errors.New("time bucket not exists")

				b := tx.Bucket([]byte(tbucket))
				if b != nil {
					err = b.Put([]byte(rkey), tb)
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

				if ignore {
					continue
				}

				db.Close()
				return

			}

			loopcount++

			pdata = nil

			elapsed := float64(time.Since(start)) / float64(time.Millisecond)

			if verbose {
				fmt.Printf("Upgrading file | File [%s] | Path [%s] | Elapsed [%f] | DB [%s]\n", rkey, dabs, elapsed, dbf)
			}

		}

		err = db.CompactQuietly()
		if err != nil {

			fmt.Printf("On the fly delayed compaction error | DB [%s] | %v\n", dbf, err)
			db.Close()

			if ignore {
				continue
			}

			return

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
