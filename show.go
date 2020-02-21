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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/eltaline/bolt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"
)

// ZAShowSingle : show contents from single bolt archive
func ZAShowSingle() {
	defer wg.Done()

	var err error

	// Wait Group

	wg.Add(1)

	bucket := ""
	ibucket := "index"

	timeout := time.Duration(locktimeout) * time.Second

	rgxbolt := regexp.MustCompile(`(\.bolt$)`)

	uri := show

	dir := filepath.Dir(uri)
	file := filepath.Base(uri)

	abs := filepath.Clean(dir + "/" + file)

	dbf := filepath.Clean(uri)

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

	var sum int64 = 0
	var loopcount int = 0

	fmt.Printf("Show files from DB: [%s]\n\n", dbf)

	for _, hkey := range keys {

		rkey = hkey.key
		bucket = hkey.val

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

			fmt.Printf("Read header data from db error | Header Buffer [%p] | File [%s] | DB [%s] | %v\n", headbuffer, rkey, dbf, err)
			pdata = nil
			headbuffer = nil

			continue

		}

		hread := bytes.NewReader(headbuffer[:hsizebuffer])

		err = binary.Read(hread, Endian, &readhead)
		if err != nil {

			fmt.Printf("Read binary header data from db error | Header Buffer [%p] | File [%s] | DB [%s] | %v\n", hread, rkey, dbf, err)
			pdata = nil
			headbuffer = nil
			readhead = Header{}

			continue

		}

		headbuffer = nil

		dsize := int64(readhead.Size)
		tmst := int64(readhead.Date)
		modt := time.Unix(tmst, 0)
		hmodt := modt.Format(time.UnixDate)
		mode := strconv.FormatUint(uint64(readhead.Mode), 8)
		crc := strconv.FormatUint(uint64(readhead.Crcs), 16)

		readhead = Header{}

		fmt.Printf("File [%s] | Size [%d] | Date [%s] | Mode [0%s] | CRC [%s] | Bucket: [%s]\n", rkey, dsize, hmodt, mode, crc, bucket)

		sum = sum + dsize
		loopcount++

		pdata = nil

	}

	db.Close()

	fmt.Printf("\nCount Files [%d] | Total Size [%d]\n", loopcount, sum)

}
