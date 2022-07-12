/*
 * @Author: dejavudwh
 * @Date: 2022-07-12 09:28:00
 * @LastEditTime: 2022-07-12 09:37:44
 */
package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"sensekv/utils"

	"github.com/stretchr/testify/require"
)

func TestBaseManifest(t *testing.T) {
	clearDir()
	recovery := func() {
		// Each run is the equivalent of an accidental reboot
		lsm := buildLSM()
		baseTest(t, lsm, 128)
		lsm.Close()
	}

	runTest(5, recovery)
}

func TestManifestMagic(t *testing.T) {
	helpTestManifestFileCorruption(t, 3, "bad magic")
}

func TestManifestVersion(t *testing.T) {
	helpTestManifestFileCorruption(t, 4, "unsupported version")
}

func TestManifestChecksum(t *testing.T) {
	helpTestManifestFileCorruption(t, 15, "bad check sum")
}

func helpTestManifestFileCorruption(t *testing.T, off int64, errorContent string) {
	clearDir()
	{
		lsm := buildLSM()
		require.NoError(t, lsm.Close())
	}
	fp, err := os.OpenFile(filepath.Join(opt.WorkDir, utils.ManifestFilename), os.O_RDWR, 0)
	require.NoError(t, err)
	// Write an incorrect value
	_, err = fp.WriteAt([]byte{'X'}, off)
	require.NoError(t, err)
	require.NoError(t, fp.Close())
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("recover")
			require.Contains(t, err.(error).Error(), errorContent)
		}
	}()
	// Open lsm here and it will panic
	lsm := buildLSM()
	require.NoError(t, lsm.Close())
}
