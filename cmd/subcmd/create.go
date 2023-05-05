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

type CreateOptions struct {
	FilePath    string
	BackingPath string
	Size        string
	SubCluster  bool
}

func newCreateCmd() *cobra.Command {

	var opts CreateOptions
	var cmd = &cobra.Command{
		Use:   "create",
		Short: "create a new qcow2 file",
		Long:  "qcow2_utils create <-f filename> <-s size>",
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.FilePath == "" {
				cmd.Help()
				os.Exit(1)
			}
			size, success := str2Int(opts.Size)
			if !success {
				cmd.Help()
				os.Exit(1)
			}

			err := createQcow2(opts.FilePath, size, opts.SubCluster, opts.BackingPath)
			if err != nil {
				fmt.Printf("create qcow2 file failed, err:%v\n", err)
			} else {
				fmt.Printf("create qcow2 file successfully\n")
			}
			return nil
		},
	}
	flags := cmd.Flags()

	flags.StringVarP(&opts.FilePath, "filename", "f", "", "specify the file path")
	flags.StringVarP(&opts.Size, "size", "s", "", "specify the size of file, valid unit is 'k', 'm', 'g', 't', limited to 4t. ")
	flags.BoolVarP(&opts.SubCluster, "enable-subcluster", "", false, "")
	flags.StringVarP(&opts.BackingPath, "backing", "b", "", "specify the backing file path")
	return cmd
}

func createQcow2(filename string, size uint64, subcluster bool, backing string) error {

	var err error
	opts := make(map[string]any)
	opts[qcow2.OPT_SIZE] = size
	opts[qcow2.OPT_FMT] = "qcow2"
	opts[qcow2.OPT_FILENAME] = filename
	opts[qcow2.OPT_SUBCLUSTER] = subcluster
	opts[qcow2.OPT_BACKING] = backing

	if err = qcow2.Blk_Create(filename, opts); err != nil {
		fmt.Printf("failed to create qcow2 file: %s, err: %v\n", filename, err)
	}
	return err
}
