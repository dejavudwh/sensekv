/*
 * @Author: dejavudwh
 * @Date: 2022-07-13 12:33:32
 * @LastEditTime: 2022-07-13 12:33:34
 */
package utils

// Copy copies a byte slice and returns the copied slice.
func Copy(a []byte) []byte {
	b := make([]byte, len(a))
	copy(b, a)
	return b
}
