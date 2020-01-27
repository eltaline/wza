package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/eltaline/bolt"
	"os"
	"os/user"
	"strconv"
	"time"
	"unsafe"
)

// BoltDB Handlers

// BoltOpenWrite : open BoltDB for write operations
func BoltOpenWrite(dbpath string, fmode os.FileMode, timeout time.Duration, opentries int, freelist string) (*bolt.DB, error) {

	i := 0

	flist := bolt.FreelistMapType

	switch {
	case freelist == "hashmap":
		flist = bolt.FreelistMapType
	case freelist == "array":
		flist = bolt.FreelistArrayType
	}

	for {

		i++

		db, err := bolt.Open(dbpath, fmode, &bolt.Options{Timeout: timeout, FreelistType: flist})
		if err == nil {
			return db, err
		}

		if i >= opentries {
			return db, err
		}

		time.Sleep(defsleep)

	}

}

// BoltOpenRead : open BoltDB for readonly operations
func BoltOpenRead(dbpath string, fmode os.FileMode, timeout time.Duration, opentries int, freelist string) (*bolt.DB, error) {

	i := 0

	flist := bolt.FreelistMapType

	switch {
	case freelist == "hashmap":
		flist = bolt.FreelistMapType
	case freelist == "array":
		flist = bolt.FreelistArrayType
	}

	for {

		i++

		db, err := bolt.Open(dbpath, fmode, &bolt.Options{Timeout: timeout, ReadOnly: true, FreelistType: flist})
		if err == nil {
			return db, err
		}

		if i >= opentries {
			return db, err
		}

		time.Sleep(defsleep)

	}

}

// System Helpers

// DetectEndian : determine system endianess function
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

// DetectUser : determine current system user and group
func DetectUser() {

	cuser, err := user.Current()
	if err != nil {
		fmt.Printf("Can`t determine current user error | %v\n", err)
		os.Exit(1)
	}

	Uid, err = strconv.ParseInt(cuser.Uid, 10, 16)
	if err != nil {
		fmt.Printf("Can`t int convert current user uid error | %v\n", err)
		os.Exit(1)
	}

	Gid, err = strconv.ParseInt(cuser.Gid, 10, 16)
	if err != nil {
		fmt.Printf("Can`t int convert current user gid error | %v\n", err)
		os.Exit(1)
	}

}

// File Helpers

// FileExists : check existence of requested file
func FileExists(filename string) bool {

	if fi, err := os.Stat(filename); err == nil {

		if fi.Mode().IsRegular() {
			return true
		}

	}

	return false

}

// DirExists : check existence of requested directory
func DirExists(filename string) bool {

	if fi, err := os.Stat(filename); err == nil {

		if fi.Mode().IsDir() {
			return true
		}

	}

	return false

}

// RemoveFile : remove requested file
func RemoveFile(file string) error {

	err := os.Remove(file)
	if err != nil {
		return err
	}

	return err

}

// DB Helpers

// KeyExists : check existence of requested key
func KeyExists(db *bolt.DB, ibucket string, file string) (data string, err error) {

	err = db.View(func(tx *bolt.Tx) error {

		b := tx.Bucket([]byte(ibucket))
		if b != nil {

			val := b.Get([]byte(file))
			if val != nil {
				data = string(val)
			}

		}

		return nil

	})

	return data, err

}

// KeyCount : count keys in an index bucket for requested directory
func KeyCount(db *bolt.DB, ibucket string) (cnt int, err error) {

	cnt = 1

	var sts bolt.BucketStats

	err = db.View(func(tx *bolt.Tx) error {

		verr := errors.New("index bucket not exists")

		b := tx.Bucket([]byte(ibucket))
		if b != nil {
			sts = b.Stats()
			cnt = sts.KeyN
		} else {
			return verr
		}

		return nil

	})

	return cnt, err

}

// KeyCountBucket : count keys in a requested bucket
func KeyCountBucket(db *bolt.DB, bucket string) (cnt int, err error) {

	cnt = 1

	var sts bolt.BucketStats

	err = db.View(func(tx *bolt.Tx) error {

		verr := errors.New("bucket not exists")

		b := tx.Bucket([]byte(bucket))
		if b != nil {
			sts = b.Stats()
			cnt = sts.KeyN
		} else {
			return verr
		}

		return nil

	})

	return cnt, err

}

// BucketCount : get count of buckets from count bucket in requested directory
func BucketCount(db *bolt.DB, cbucket string) (cnt uint64, err error) {

	cnt = uint64(0)

	err = db.View(func(tx *bolt.Tx) error {

		verr := errors.New("count bucket not exists")

		b := tx.Bucket([]byte(cbucket))
		if b != nil {

			val := b.Get([]byte("counter"))
			if val != nil {
				cnt = Endian.Uint64(val)
			}

		} else {
			return verr
		}

		return nil

	})

	return cnt, err

}

// BucketStats : get current size of requested bucket
func BucketStats(db *bolt.DB, bucket string) (cnt int, err error) {

	cnt = 0

	var sts bolt.BucketStats

	err = db.View(func(tx *bolt.Tx) error {

		verr := errors.New("bucket not exists")

		b := tx.Bucket([]byte(bucket))
		if b != nil {
			sts = b.Stats()
			cnt = sts.LeafInuse
		} else {
			return verr
		}

		return nil

	})

	return cnt, err

}

// RemoveFileDB : remove requested BoltDB file
func RemoveFileDB(file string) error {

	err := os.Remove(file)
	if err != nil {
		return err
	}

	return err

}

// Check Options With Boolean/Int Functions

// Check : if received value is false, then run DoExit function
func Check(bvar bool, val string, ferr func(string)) {

	if !bvar {
		ferr(val)
	}

}

// DoExit : exit program function
func DoExit(val string) {

	fmt.Printf("Bad option value error | Value [%v]\n", val)
	os.Exit(1)

}

// RBInt : check int32 acceptable range function and then return true or false
func RBInt(i int, min int, max int) bool {

	switch {
	case i >= min && i <= max:
		return true
	default:
		return false
	}

}

// RBInt64 : check int64 acceptable range function and return true or false
func RBInt64(i int64, min int64, max int64) bool {

	switch {
	case i >= min && i <= max:
		return true
	default:
		return false
	}

}
