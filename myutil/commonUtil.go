package myutil

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

func MaxInt(x, y int) int {
	if x >= y {
		return x
	} else {
		return y
	}
}

func MinInt(x, y int) int {
	if x <= y {
		return x
	} else {
		return y
	}
}

func MaxInt64(x, y int64) int64 {
	if x >= y {
		return x
	} else {
		return y
	}
}

func ExecPath() string {
	file, _ := exec.LookPath(os.Args[0])
	path, _ := filepath.Abs(file)
	index := strings.LastIndex(path, string(os.PathSeparator))
	ret := path[:index]
	return strings.Replace(ret, "\\", "/", -1)
}

func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}

	return false, err
}

func ParseTimeInMySQLFormat(str string) (time.Time, string, error) {
	if t, err := time.Parse("2006-01-02", str); err == nil {
		return t, "2006-01-02", nil
	}
	if t, err := time.Parse("2006/1/2", str); err == nil {
		return t, "2006/1/2", nil
	}
	if t, err := time.Parse("2006-01-02 03:04", str); err == nil {
		return t, "2006-01-02 03:04", nil
	}
	if t, err := time.Parse("2006/1/2 03:04", str); err == nil {
		return t, "2006/1/2 03:04", nil
	}
	if t, err := time.Parse("2006-1-2 03:04:05", str); err == nil {
		return t, "2006-1-2 03:04:05", nil
	}
	if t, err := time.Parse("2006/1/2 03:04:05", str); err == nil {
		return t, "2006/1/2 03:04:05", nil
	}

	return time.Now(), "", errors.New("can not parse")
}

func StringFind(s []string, c string) int {
	for i, v := range s {
		if v == c {
			return i
		}
	}
	return -1
}

func Time2Int(t *time.Time) int64 {
	if t == nil {
		return 1 << 63 - 1
	} else {
		return t.Unix()
	}
}

func Int2Time(x int64) *time.Time {
	if x == 1 << 63 - 1 {
		return nil
	} else {
		res := time.Unix(x, 0)
		return &res
	}
}
