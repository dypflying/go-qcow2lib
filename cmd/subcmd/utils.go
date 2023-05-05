package subcmd

/*
Copyright (c) 2023 Yunpeng Deng
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

import (
	"strconv"
	"strings"
)

func str2Int(sizeStr string) (uint64, bool) {
	var err error
	var val int
	var ret uint64

	sizeStr = strings.TrimSpace(sizeStr)
	if len(sizeStr) < 2 {
		return 0, false
	}
	valStr := sizeStr[:len(sizeStr)-1]
	uintStr := sizeStr[len(sizeStr)-1:]
	uintStr = strings.ToLower(uintStr)

	if val, err = strconv.Atoi(valStr); err != nil {
		return 0, false
	} else if val == 0 {
		return 0, false
	}
	switch uintStr {
	case "k":
		ret = uint64(val) << 10
	case "m":
		ret = uint64(val) << 20
	case "g":
		ret = uint64(val) << 30
	case "t":
		ret = uint64(val) << 40
	default:
		return 0, false
	}
	return ret, true
}
