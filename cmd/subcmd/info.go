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
	"fmt"
	"os"

	"github.com/dypflying/go-qcow2lib/qcow2"
	"github.com/spf13/cobra"
)

type InfoOptions struct {
	FilePath string
	Pretty   bool
	Detail   bool
}

func newInfoCmd() *cobra.Command {

	var opts InfoOptions
	var cmd = &cobra.Command{
		Use:   "info",
		Short: "print the basic information of the specified qcow2 file",
		Long:  "qcow2_utils info <-f filename> [--pretty]",
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.FilePath == "" {
				cmd.Help()
				os.Exit(1)
			}

			err := InfoQcow2(opts.FilePath, opts.Detail, opts.Pretty)
			if err != nil {
				fmt.Printf("open qcow2 file failed, err:%v\n", err)
			}
			return nil
		},
	}
	flags := cmd.Flags()

	flags.StringVarP(&opts.FilePath, "filename", "f", "", "specify the file name")
	flags.BoolVarP(&opts.Pretty, "pretty", "p", false, "")
	flags.BoolVarP(&opts.Detail, "detail", "d", false, "")
	return cmd
}

func InfoQcow2(filename string, detail bool, pretty bool) error {

	var root *qcow2.BdrvChild
	var err error
	opts := make(map[string]any)
	opts[qcow2.OPT_FMT] = "qcow2"
	opts[qcow2.OPT_FILENAME] = filename

	if root, err = qcow2.Blk_Open(filename, opts, os.O_RDONLY); err != nil {
		return fmt.Errorf("failed to open qcow2 file: %s, err: %v", filename, err)
	}
	bs := root.GetBS()
	fmt.Println(bs.Info(detail, pretty))
	qcow2.Blk_Close(root)

	return nil
}
