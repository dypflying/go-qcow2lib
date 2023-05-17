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
	"time"

	"github.com/dypflying/go-qcow2lib/qcow2"
	"github.com/spf13/cobra"
)

// only raw and qcow2 format is supported
const (
	RAW_FORMAT   = "raw"
	QCOW2_FORMAT = "qcow2"
)

const (
	BLOCK_SIZE = qcow2.DEFAULT_SECTOR_SIZE //block size use default sector size (512)
)

type DdOptions struct {
	InputFile    string
	OutputFile   string
	InputFormat  string
	OutputFormat string
	L2CacheSize  string
}

func newDdCmd() *cobra.Command {

	var opts DdOptions
	var cmd = &cobra.Command{
		Use:   "dd",
		Short: "convert and copy from or to qcow2 files",
		Long:  "qcow2_utils dd [-f inputformat] <-i inputfile> <-O outputformat> <-o outputfile> [--l2-cache-size=size]",
		RunE: func(cmd *cobra.Command, args []string) error {
			var l2CacheSize uint64
			var ok bool
			if opts.InputFile == "" || opts.OutputFile == "" || opts.OutputFormat == "" {
				cmd.Help()
				os.Exit(1)
			}
			if opts.InputFormat != "" {
				if _, ok := qcow2.Supported_Types[opts.InputFormat]; !ok {
					fmt.Printf("iutput file format %s is not supported\n", opts.InputFormat)
					os.Exit(1)
				}
			}
			if _, ok := qcow2.Supported_Types[opts.OutputFormat]; !ok {
				fmt.Printf("output file format %s is not supported\n", opts.OutputFormat)
				os.Exit(1)
			}
			if opts.L2CacheSize != "" {
				if l2CacheSize, ok = str2Int(opts.L2CacheSize); !ok {
					cmd.Help()
					os.Exit(1)
				}
			}
			if err := runDD(opts, l2CacheSize); err != nil {
				fmt.Printf("dd finished with err: %v\n", err)
			} else {
				fmt.Println("dd finished successfully")
			}
			return nil
		},
	}
	flags := cmd.Flags()

	flags.StringVarP(&opts.InputFile, "inputfile", "i", "", "specify the input file name")
	flags.StringVarP(&opts.OutputFile, "outputfile", "o", "", "specify the output file name")
	flags.StringVarP(&opts.InputFormat, "inputformat", "f", "", "specify the input file format")
	flags.StringVarP(&opts.OutputFormat, "outputformat", "O", "", "specify the output file format")
	flags.StringVarP(&opts.L2CacheSize, "l2-cache-size", "", "", "specify the l2 cache size")

	return cmd
}

func runDD(opts DdOptions, l2CacheSize uint64) (err error) {
	return execDD(opts.InputFile, opts.InputFormat, opts.OutputFile, opts.OutputFormat, l2CacheSize)
}

// begin to copy data from raw file to qcow2 file
func execDD(inputFile string, inputFormat string, outputFile string, outputFormat string, l2CacheSize uint64) (err error) {

	var inRoot, outRoot *qcow2.BdrvChild
	var size uint64
	var outPos, inPos, blockCount uint64
	var inRet, outRet uint64
	buf := make([]uint8, BLOCK_SIZE)

	if inputFormat == "" {
		if inputFormat, err = qcow2.Blk_Probe(inputFile); err != nil {
			return err
		}
	}
	if inRoot, err = qcow2.Blk_Open(inputFile,
		map[string]any{qcow2.OPT_FMT: inputFormat, qcow2.OPT_L2CACHESIZE: l2CacheSize}, qcow2.BDRV_O_RDWR); err != nil {
		return err
	}
	if size, err = qcow2.Blk_Getlength(inRoot); err != nil {
		goto out
	}

	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		if outputFormat == "qcow2" {
			opts := make(map[string]any)
			opts[qcow2.OPT_SIZE] = size
			opts[qcow2.OPT_FMT] = outputFormat
			opts[qcow2.OPT_FILENAME] = outputFile
			opts[qcow2.OPT_SUBCLUSTER] = true
			qcow2.Blk_Create(outputFile, opts)
		} else if outputFormat == "raw" {
			opts := make(map[string]any)
			opts[qcow2.OPT_SIZE] = size
			opts[qcow2.OPT_FMT] = outputFormat
			opts[qcow2.OPT_FILENAME] = outputFile
			qcow2.Blk_Create(outputFile, opts)
		}
	} else {
		return fmt.Errorf("%s exists", outputFile)
	}
	if outRoot, err = qcow2.Blk_Open(outputFile,
		map[string]any{qcow2.OPT_FMT: outputFormat, qcow2.OPT_L2CACHESIZE: l2CacheSize},
		qcow2.BDRV_O_RDWR); err != nil {
		return err
	}
	for outPos = 0; inPos < size; blockCount++ {

		if inPos+BLOCK_SIZE > size {
			inRet, err = qcow2.Blk_Pread(inRoot, inPos, buf, size-inPos)
		} else {
			inRet, err = qcow2.Blk_Pread(inRoot, inPos, buf, BLOCK_SIZE)
		}
		if err != nil {
			goto out
		}
		inPos += inRet

		outRet, err = qcow2.Blk_Pwrite(outRoot, outPos, buf, inRet, 0)
		if err != nil {
			goto out
		}
		outPos += outRet
		time.Sleep(time.Duration(1) * time.Microsecond)
	}

	//close input and outout
	qcow2.Blk_Close(inRoot)
	qcow2.Blk_Close(outRoot)

out:
	return err
}
