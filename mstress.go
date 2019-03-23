package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/ncw/directio"
)

var r = rand.New(rand.NewSource(time.Now().UnixNano()))

type writeTask struct {
	dir                      string
	deleteSize               uint64
	bufMin, bufMax           int
	minFileSize, maxFileSize int
	clrDir                   bool
	wg                       *sync.WaitGroup
	bytesWritten             uint64
	buf                      []byte
	flush                    bool
	sleepAfterDelete         int64
	direct                   bool
}

type readTask struct {
	dir            string
	bufMin, bufMax int
	wg             *sync.WaitGroup
	buf            []byte
}

func genBufSize(bufMin int, bufMax int) int {
	bufSize := 0
	if bufMin < 0 {
		bufSize = r.Int() % bufMax
	} else if bufMin == bufMax {
		bufSize = bufMin
	} else {
		bufSize = bufMin + r.Int()%(bufMax-bufMin)
	}
	return bufSize
}

func (wt *writeTask) writeOne() {

	// check size of bytes written
	if wt.deleteSize > 0 && wt.bytesWritten > wt.deleteSize {
		fmt.Printf("Deleting all files in %s\n", wt.dir)
		filepath.Walk(wt.dir, func(path string, info os.FileInfo, err error) error {
			if path != wt.dir {
				f, err := os.OpenFile(path, os.O_RDWR, 0666)
				defer f.Close()
				if err != nil {
					log.Println(err)
				} else {
					fmt.Printf("Tuncating %s\n", path)
					f.Truncate(0)
					f.Sync()
					f.Close()
				}
				os.Remove(path)
			}
			return nil
		})
		runtime.GC()
		wt.bytesWritten = 0
		if wt.sleepAfterDelete > 0 {
			fmt.Printf("Sleeping %ds\n", wt.sleepAfterDelete)
			time.Sleep(time.Duration(wt.sleepAfterDelete) * time.Second)
		}
	}

	bufSize := genBufSize(wt.bufMin, wt.bufMax)

	var err error
	f, err := ioutil.TempFile(wt.dir, "td")
	if err != nil {
		log.Fatal(err)
	}

	size := 0
	if wt.minFileSize < 0 { // random up to max
		size = r.Int() % wt.maxFileSize
	} else if wt.minFileSize == wt.maxFileSize {
		size = wt.minFileSize
	} else {
		size = wt.minFileSize + r.Int()%(wt.maxFileSize-wt.minFileSize)
	}

	count := size / bufSize

	// fmt.Printf("%s writing [size=%d, buf=%d, count=%d]\n", tmpFile.Name(), size, bufSize, count)
	ts := time.Now()
	if !wt.direct {
		f, err = os.Create(f.Name())
	} else {
		f, err = directio.OpenFile(f.Name(), os.O_CREATE|os.O_WRONLY, 0666)
	}
	defer f.Close()
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < count; i++ {
		wt.buf[0] = byte(i % 256) // add some more uniquness
		n, err := f.Write(wt.buf[0:bufSize])
		if n != bufSize || err != nil {
			log.Println(err)
			return
		}
		wt.bytesWritten += uint64(bufSize)
	}

	if wt.flush {
		f.Sync()
	}
	f.Close()

	elapsed := time.Since(ts)
	mbs := float64(size/1024/1024) / elapsed.Seconds()
	fmt.Printf("%s wrote [size=%d, buf=%d, count=%d, tput=%.2f MB/s, elapsed=%.3f s]\n", f.Name(), size, bufSize, count, mbs, elapsed.Seconds())

}

func (wt *writeTask) writer(iter int) {

	fmt.Printf("Starting: %s\n", wt.dir)

	if wt.clrDir {
		filepath.Walk(wt.dir, func(path string, info os.FileInfo, err error) error {
			if path != wt.dir {
				os.Remove(path)
			}
			return nil
		})
	}

	os.Mkdir(wt.dir, os.ModePerm)

	count := 0
	for {
		wt.writeOne()
		count++
		if count == iter {
			break
		}
	}

	wt.wg.Done()
}

func (rt *readTask) readOne(path string) {

	bufSize := genBufSize(rt.bufMin, rt.bufMax)

	fi, err := os.Stat(path)
	if err != nil {
		return
	}

	count := fi.Size() / int64(bufSize)

	// fmt.Printf("%s reading [size=%d, buf=%d, count=%d]\n", path, fi.Size(), bufSize, count)
	ts := time.Now()

	f, err := os.Open(path)
	defer f.Close()

	for i := 0; i < int(count); i++ {
		f.Read(rt.buf)
	}
	f.Close()

	elapsed := time.Since(ts)
	mbs := float64(fi.Size()/1024/1024) / elapsed.Seconds()
	fmt.Printf("%s read [size=%d, buf=%d, count=%d, tput=%.2f MB/s, elapsed=%.3f s]\n", path, fi.Size(), bufSize, count, mbs, elapsed.Seconds())

}

func (rt *readTask) reader(iter int) {
	count := 0
	for {
		filepath.Walk(rt.dir, func(path string, info os.FileInfo, err error) error {
			if path != rt.dir {
				rt.readOne(path)
			}
			return nil
		})
		count++
		if count == iter {
			break
		}
	}
	rt.wg.Done()
}

func main() {
	deleteSizeGb := flag.Int("ds", 0, "Size in GB before files in the target directory are deleted")
	minFileSize := flag.Int("minfs", 1, "Min file size in MB")
	maxFileSize := flag.Int("maxfs", 1024, "Max file size in MB")
	bufMin := flag.Int("minbs", 8*1024, "Min block size in bytes")
	bufMax := flag.Int("maxbs", 8*1024, "Max block size in bytes")
	clrDir := flag.Bool("clr", false, "Clear target directory before writing")
	read := flag.Bool("read", false, "Do reads")
	write := flag.Bool("write", false, "Do writes")
	flush := flag.Bool("flush", false, "Flush after each file has been written")
	sleepAfterDelete := flag.Int64("sleep", 0, "Sleep after delete to give file system time to catch up")
	iter := flag.Int("iter", -1, "Number of iterations (default is -1 which means infinite")
	direct := flag.Bool("direct", false, "Direct IO for writes")

	flag.Parse()

	if !*read && !*write {
		fmt.Printf("Need --read or --write\n")
		os.Exit(1)
	}

	if len(flag.Args()) == 0 {
		fmt.Printf("Need target dir\n")
		os.Exit(1)
	}

	if *bufMax <= 0 {
		fmt.Printf("maxbs must be greater 0\n")
		os.Exit(1)
	}

	*minFileSize *= 1024 * 1024
	*maxFileSize *= 1024 * 1024

	if *maxFileSize <= 0 || *maxFileSize < *bufMax {
		fmt.Printf("maxFileSize must be greater 0 and greater than maxbs\n")
		os.Exit(1)
	}

	if *direct && (*bufMax != *bufMin) {
		log.Fatal("block sizes must be the same for direct")
	}

	var deleteSize = uint64(*deleteSizeGb * 1024 * 1024 * 1024)

	// global write buffer for all tasks

	var wgr sync.WaitGroup
	var wgw sync.WaitGroup
	for _, dir := range flag.Args() {

		if *write {
			wgw.Add(1)
			var writeBuf []byte
			if !*direct {
				writeBuf = make([]byte, *bufMax)
			} else {
				writeBuf = directio.AlignedBlock(*bufMax)
			}
			for i := 0; i < len(writeBuf); i++ {
				writeBuf[i] = byte(i % 256)
			}
			wt := writeTask{dir, deleteSize, *bufMin, *bufMax, *minFileSize, *maxFileSize, *clrDir, &wgw, 0, writeBuf, *flush, *sleepAfterDelete, *direct}
			go wt.writer(*iter)
		}
		if *read {
			wgr.Add(1)
			rt := readTask{dir, *bufMin, *bufMax, &wgr, make([]byte, *bufMax)}
			go rt.reader(*iter)
		}
	}
	wgr.Wait()
	wgw.Wait()
}
