package main

import (
	"encoding/binary"
	"fmt"
	"github.com/eltaline/bolt"
	"os"
	"os/user"
	"strconv"
	"unsafe"
)

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

func FileExists(filename string) bool {

	if fi, err := os.Stat(filename); err == nil {

		if fi.Mode().IsRegular() {
			return true
		}

	}

	return false

}

// Dir Exists Handler

func DirExists(filename string) bool {

	if fi, err := os.Stat(filename); err == nil {

		if fi.Mode().IsDir() {
			return true
		}

	}

	return false

}

// Remove File Handler

func RemoveFile(file string) error {

	err := os.Remove(file)
	if err != nil {
		return err
	}

	return err

}

// DB Key Exists Handler

func KeyExists(db *bolt.DB, bucket string, file string) (exkey bool, err error) {

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

func KeyCount(db *bolt.DB, bucket string) (cnt int, err error) {

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

func RemoveFileDB(file string) error {

	err := os.Remove(file)
	if err != nil {
		return err
	}

	return err

}

// Check Options With Boolean Functions

func Check(bvar bool, val string, ferr func(string)) {

	if !bvar {
		ferr(val)
	}

}

func DoExit(val string) {

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
