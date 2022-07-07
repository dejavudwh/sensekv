/*
 * @Author: dejavudwh
 * @Date: 2022-07-07 04:17:10
 * @LastEditTime: 2022-07-07 08:38:56
 */
package utils

import (
	"log"

	"github.com/pkg/errors"
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
