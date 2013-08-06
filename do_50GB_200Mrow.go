package main

import (
  "bytes"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"time"
)

var (
	part_files = make(map[int]*os.File, 1000)
	part_buffs = make(map[int]*bytes.Buffer, 1000)

	toDisk  = make(chan *_w_objcet, 1<<8)
	toParse = make(chan []byte, 1<<8)

	waitToDiskDone = make(chan bool)

	readPiece  = 1 << 20 // lines for every read, and yes, it's Monkey.D.Luffy
	writePiece = 1 << 16 // lines for every write, and yes, it's Monkey.D.Luffy

	allLines = 0
	repeated = 0

	start int64 // start time
)

type Bitmap struct {
	Shift uint32
	Mask  uint32
	Max   uint32

	Data []uint32
}

func NewBitmap() *Bitmap {
	bm := &Bitmap{
		Shift: 5,
		Mask:  0x1F,
	}
	// bm.Data = make([]uint32, 16)
	bm.Data = make([]uint32, 1<<25)
	// bm.Data = reused[:0]

	return bm
}

func (bm *Bitmap) idx_of_ints(momoid uint32) uint32 {
	return momoid >> bm.Shift // momoid / 32
}

func (bm *Bitmap) valueByOffset(momoid uint32) uint32 {
	return 1 << (momoid & bm.Mask) // 1 << ( momoid % 32 )
}

func (bm *Bitmap) Put(momoid uint32) {
	if momoid > bm.Max {
		bm.Max = momoid
	}

	idx := bm.idx_of_ints(momoid)

	if oldSize := len(bm.Data); int(idx) >= oldSize {
		tmp := make([]uint32, idx+32)
		for i := 0; i < oldSize; i++ {
			tmp[i] = bm.Data[i]
		}
		bm.Data = tmp
	}

	v := bm.valueByOffset(momoid)
	bm.Data[idx] |= v
}

func (bm *Bitmap) Contains(momoid uint32) bool {
	idx := bm.idx_of_ints(momoid)
	if int(idx) >= len(bm.Data) {
		return false
	}

	v := bm.valueByOffset(momoid)

	return bm.Data[idx]&v == v
}

// Convert uint32 to []byte
func Uint32ToBytes(v uint32) []byte {
	b := make([]byte, 4)
	b[0] = byte(v >> 24)
	b[1] = byte(v >> 16)
	b[2] = byte(v >> 8)
	b[3] = byte(v)

	return b
}

// Convert []byte to uint32
func BytesToUint32(b []byte) uint32 {
	return uint32(b[3]) | uint32(b[2])<<8 | uint32(b[1])<<16 | uint32(b[0])<<24
}

func concat(a, b []byte) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, len(a)+len(b)))
	buf.Write(a)
	buf.Write(b)

	return buf.Bytes()
}

type _w_objcet struct {
	f    *os.File
	data []byte
}

func flush(n6 int) {
	f := part_files[n6]
	var err error
	if f == nil {
		fn := fmt.Sprintf("./parts/%d.part", n6)
		f, err = os.OpenFile(fn, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			fmt.Printf("Error line open %s: %s\n", fn, err)
			os.Exit(-1)
		}

		part_files[n6] = f
	}

	buf := part_buffs[n6]
	toDisk <- &_w_objcet{f, buf.Bytes()}
	part_buffs[n6] = nil
}

func flushAll() {
	for n6, _ := range part_buffs {
		flush(n6)
	}
}

func line(b []byte) {
	allLines++

	n := bytes.IndexByte(b, ' ')
	n6, _ := strconv.Atoi(string(b[n+1 : n+7]))
	n9, _ := strconv.Atoi(string(b[n+7 : n+16]))

	buf := part_buffs[n6]
	if buf == nil {
		buf = bytes.NewBuffer(make([]byte, 0, writePiece+8))
		part_buffs[n6] = buf
	}

	b4 := Uint32ToBytes(uint32(n9))
	buf.Write(b4)

	if buf.Len() >= writePiece {
		flush(n6)
	}
}

func parse(remains, b []byte) []byte {
	if len(remains) > 0 {
		n := bytes.IndexByte(b, '\n')
		if n == -1 {
			return concat(remains, b)
		}

		line(concat(remains, b[:n]))
		b = b[n+1:]
	}

	for {
		n := bytes.IndexByte(b, '\n')
		if n == -1 {
			return b
		}

		// line(b[:n])
		line(b)

		n++
		if len(b) == n {
			return nil
		}

		b = b[n+1:]

	}
}

func loopParse() {
	total := 0
	round := 1 << 20
	rounds := 0
	var remains []byte
	for {
		select {
		case b, ok := <-toParse:
			if !ok {
				flushAll()
				close(toDisk)
				fmt.Println("loopParse done! Read lines: %d, total read: %dMB, spends: %dms \n", allLines, total/(1<<20), (time.Now().UnixNano()-start)/(1000*1000))

				return
			}

			total += len(b)
			remains = parse(remains, b)

			if allLines > rounds {
				rounds += round
				fmt.Printf("lines read:%d, total read:%dMB, spends:%dms \n", allLines, total/(1<<20), (time.Now().UnixNano()-start)/(1000*1000))
			}
		}
	}
}

func loopRead(fi *os.File) {
	for {
		b := make([]byte, readPiece)
		n, err := fi.Read(b)
		if n > 0 {
			toParse <- b[:n]
		}

		if err != nil {
			// fmt.Printf("loopRead %d bytes, err: %s\n", n, err)
			close(toParse)
			break
		}
	}

	fmt.Printf("loopRead done, all spend: %d ms \n", (time.Now().UnixNano()-start)/(1000*1000))

}

func loopWrite() {
	total := 0
	round := 1 << 20
	rounds := 0
	for {
		select {
		case wo, ok := <-toDisk:
			if !ok {
				fmt.Printf("loopWrite done to partfiles%dMB , all spend: %d ms \n", total/(1<<20), (time.Now().UnixNano()-start)/(1000*1000))
				waitToDiskDone <- true
				return
			}

			n, err := wo.f.Write(wo.data)
			if err != nil {
				fmt.Printf("Error write wo.data: %s\n", err)
				os.Exit(-1) // todo

			}

			total += n
			if total > rounds {
				rounds += round
				fmt.Printf("loopWrite to partfiles %dMB , all spend: %d ms \n", total/(1<<20), (time.Now().UnixNano()-start)/(1000*1000))
			}
		}
	}

}

func onePart(n6 int, part *os.File, result *os.File, tmp_slice []uint32) {
	fmt.Printf("part: %s start, all spend: %d ms \n", fmt.Sprintf("%06d", n6), (time.Now().UnixNano()-start)/(1000*1000))
	bmp := NewBitmap()
	bmp_repeated := NewBitmap()
	repeated_slice := tmp_slice[:0]
	// fmt.Printf("part: %s alloc, all spend: %d ms \n", fmt.Sprintf("%06d", n6), (time.Now().UnixNano()-start)/(1000*1000))
	b := make([]byte, 1<<17*4)
	part.Seek(0, 0)
	for {
		n, err := part.Read(b)
		if n > 0 {
			for i := 0; i < n; i += 4 {
				v := BytesToUint32(b[i : i+4])
				if bmp.Contains(v) {
					if !bmp_repeated.Contains(v) {
						repeated_slice = append(repeated_slice, v)
						bmp_repeated.Put(v)
					}
				} else {
					bmp.Put(v)
				}
			}

		}
		if err != nil {
			break
		}
	}
	fmt.Printf("part: %s ltrip, all spend: %d ms \n", fmt.Sprintf("%06d", n6), (time.Now().UnixNano()-start)/(1000*1000))

	buf := bytes.NewBuffer(make([]byte, 0, 1<<19))
	s6 := fmt.Sprintf("%06d", n6)
	for i := 0; i < len(repeated_slice); i++ {
		repeated++
		s9 := fmt.Sprintf("%09d", int(repeated_slice[i]))
		buf.WriteString(s6)
		buf.WriteString(s9)
		buf.WriteByte('\n')
		if buf.Len() > 524272 {
			result.Write(buf.Bytes())
			buf = bytes.NewBuffer(make([]byte, 0, 1<<19))
		}
	}
	result.Write(buf.Bytes())
	fmt.Printf("part: %s done , all spend: %d ms \n", fmt.Sprintf("%06d", n6), (time.Now().UnixNano()-start)/(1000*1000))

	return
}

func main() {
	if err := os.Mkdir("./parts", 0666); err != nil {
		// fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Printf("./parts/ has been created!\n")
	}

	f_50g, err := os.OpenFile("./50g/50g.log", os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		fmt.Printf("Error f_50g: %s\n", err)
		return
	}
	defer f_50g.Close()

	f_result, err := os.OpenFile("./result.txt", os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		fmt.Printf("Error f_result: %s\n", err)
		return
	}
	defer f_result.Close()

	runtime.GOMAXPROCS(4)

	fmt.Println("Begin ...")
	start = time.Now().UnixNano()
	f_50g.Seek(0, 0)

	go loopWrite()

	go loopParse()

	go loopRead(f_50g)

	<-waitToDiskDone

	fmt.Println("read little parts files ...")

	tmp_slice := make([]uint32, 0, 1<<26)
	for n6, part := range part_files {
		onePart(n6, part, f_result, tmp_slice)
		part.Close()
	}

	end := time.Now().UnixNano()
	fmt.Printf("\n-----------------------\n")
	fmt.Printf("Done in %d ms!\n", (end-start)/(1000*1000))
	fmt.Printf("read %d lines\n", allLines)
	fmt.Printf("repeated %d\n", repeated)
}
