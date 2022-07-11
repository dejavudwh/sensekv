/*
 * @Author: dejavudwh
 * @Date: 2022-07-07 04:17:10
 * @LastEditTime: 2022-07-11 05:47:22
 */
package utils

import (
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

var (
	gopath = path.Join(os.Getenv("GOPATH"), "sensekv") + "/"
)

/* Error Description */
var (
	// ErrKeyNotFound is returned when key isn't found on a txn.Get.
	ErrKeyNotFound = errors.New("Key not found")
	// ErrEmptyKey is returned if an empty key is passed on an update function.
	ErrEmptyKey = errors.New("Key cannot be empty")
	// ErrReWriteFailure reWrite failure
	ErrReWriteFailure = errors.New("reWrite failure")
	// ErrBadMagic bad magic
	ErrBadMagic = errors.New("bad magic")
	// ErrBadChecksum bad check sum
	ErrBadChecksum = errors.New("bad check sum")
	// ErrChecksumMismatch is returned at checksum mismatch.
	ErrChecksumMismatch = errors.New("checksum mismatch")

	ErrTruncate = errors.New("Do truncate")
	ErrStop     = errors.New("Stop")

	// compact
	ErrFillTables = errors.New("Unable to fill tables")

	ErrBlockedWrites  = errors.New("Writes are blocked, possibly due to DropAll or Close")
	ErrTxnTooBig      = errors.New("Txn is too big to fit into one request")
	ErrDeleteVlogFile = errors.New("Delete vlog file")
	ErrNoRoom         = errors.New("No room for write")

	// ErrInvalidRequest is returned if the user request is invalid.
	ErrInvalidRequest = errors.New("Invalid request")
	// ErrNoRewrite is returned if a call for value log GC doesn't result in a log file rewrite.
	ErrNoRewrite = errors.New("Value log GC attempt didn't result in any cleanup")

	// ErrRejected is returned if a value log GC is called either while another GC is running, or
	// after DB::Close has been called.
	ErrRejected = errors.New("Value log GC request rejected")
)

/* AssertTrue asserts that b is true. Otherwise, it would log fatal. */
func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
}

/* AssertTruef is AssertTrue with extra info. */
func AssertTruef(b bool, format string, args ...interface{}) {
	if !b {
		log.Fatalf("%+v", errors.Errorf(format, args...))
	}
}

func CondPanic(condition bool, err error) {
	if condition {
		Panic(err)
	}
}

func Panic(err error) {
	if err != nil {
		panic(err)
	}
}

func Panic2(_ interface{}, err error) {
	Panic(err)
}

func WarpErr(format string, err error) error {
	if err != nil {
		fmt.Printf("%s %s %s", format, location(2, true), err)
	}
	return err
}

func Err(err error) error {
	if err != nil {
		fmt.Printf("%s %s\n", location(2, true), err)
	}
	return err
}

func location(deep int, fullPath bool) string {
	_, file, line, ok := runtime.Caller(deep)
	if !ok {
		file = "???"
		line = 0
	}

	if fullPath {
		if strings.HasPrefix(file, gopath) {
			file = file[len(gopath):]
		}
	} else {
		file = filepath.Base(file)
	}
	return file + ":" + strconv.Itoa(line)
}
