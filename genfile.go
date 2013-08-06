package main

import (
	"bytes"
	"container/list"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"time"
)

const (
	line_fillings = `What goes here is not important. It is just stuff to fill lines.What goes here is not important. It is just stuff to fill lines.What goes here is not important. It is just stuff to fill lines.What goes here is not important. One Piece!`
)

var (
	country_code = []string{
		"0001", "0007", "0020", "0027", "0030", "0031", "0032", "0033", "0034", "0036", "0039", "0040", "0041", "0043", "0044", "0045", "0046", "0047", "0048", "0049",
		"0051", "0052", "0053", "0054", "0055", "0056", "0057", "0058", "0060", "0061", "0062", "0063", "0064", "0065", "0066", "0081", "0082", "0084", "0086", "0090",
		"0091", "0092", "0093", "0094", "0095", "0098", "0212", "0213", "0216", "0218", "0220", "0221", "0223", "0224", "0225", "0226", "0228", "0229", "0230", "0231",
		"0232", "0233", "0234", "0235", "0236", "0237", "0239", "0241", "0242", "0243", "0244", "0247", "0248", "0249", "0251", "0252", "0253", "0254", "0255", "0256",
		"0257", "0258", "0260", "0261", "0262", "0263", "0264", "0265", "0266", "0267", "0268", "0327", "0331", "0350", "0351", "0352", "0353", "0354", "0355", "0356",
		"0357", "0358", "0359", "0370", "0371", "0372", "0373", "0374", "0375", "0376", "0377", "0378", "0380", "0381", "0386", "0420", "0421", "0423", "0501", "0502",
		"0503", "0504", "0505", "0506", "0507", "0509", "0591", "0592", "0593", "0594", "0595", "0596", "0597", "0598", "0599", "0673", "0674", "0675", "0676", "0677",
		"0679", "0682", "0684", "0685", "0689", "0850", "0852", "0853", "0855", "0856", "0880", "0886", "0960", "0961", "0962", "0963", "0964", "0965", "0966", "0967",
		"0968", "0970", "0971", "0972", "0973", "0974", "0976", "0977", "0992", "0993", "0994", "0995", "1242", "1246", "1264", "1268", "1345", "1441", "1664", "1670",
		"1671", "1758", "1784", "1787", "1809", "1876", "1890",
	}

	net_code = []string{
		"133", "153", "180", "181", "189", // China Telecom
		"130", "131", "132", "145", "155", "156", "185", "186", // China Unicom
		"134", "135", "136", "137", "138", "139", "147", "150", "151", "152", "157", "158", "159", "182", "183", "184", "187", "188", // CMCC
	}

	country_code_len = len(country_code)
	net_code_len     = len(net_code)

	r  = rand.New(rand.NewSource(time.Now().Unix()))
	r3 = rand.New(rand.NewSource(time.Now().Unix() * 3))
	r4 = rand.New(rand.NewSource(time.Now().Unix() * 4))
	r8 = rand.New(rand.NewSource(time.Now().Unix() * 8))

	repeated      = 0
	torepeat      = 0
	torepeatList  *list.List
	repeatListMax = 1 << 15

	total    = 0 // total file's size actually
	allLines = 0
	// maxLines = 200000000
	onePiece = 1 << 12 // lines for every write, and yes, it's Monkey.D.Luffy

	maxLines = 1 << 16
	// onePiece = 1 << 10 // lines for every write, and yes, it's Monkey.D.Luffy
	bufPiece = make([]byte, 0, onePiece*280)
	//  don't try to reuse this slice in concurrency situation

	toDisk = make(chan []byte, 10)

	round = 1 << 20 // for logging by
	// round  = 1 << 8 // for logging by
	rounds = 0

	start int64 // start time
)

func init() {
	torepeatList = list.New()
}

func chance() bool {
	return r.Int()%3 == 0
}

func next15num() string {
	if chance() {
		e := torepeatList.Front()
		if e != nil {
			n, _ := torepeatList.Remove(e).(string)
			repeated++

			if chance() && torepeatList.Len() < repeatListMax+1000 {
				torepeat++
				torepeatList.PushBack(n)
			}

			return n
		}
	}

	n4 := country_code[r4.Intn(country_code_len)]
	n3 := net_code[r3.Intn(net_code_len)]
	n8 := fmt.Sprintf("%08d", r8.Intn(100000000))

	n := n4 + n3 + n8
	if chance() && torepeatList.Len() < repeatListMax {
		torepeat++
		torepeatList.PushBack(n)
	}

	return n
}

func createPiece() []byte {
	buf := bytes.NewBuffer(bufPiece[:0])
	//  don't try to reuse this slice in concurrency situation

	for i := 0; i < onePiece; i++ {
		allLines++
		buf.WriteString(fmt.Sprintf("%015d", allLines))
		buf.WriteByte(' ')
		buf.WriteString(next15num())
		buf.WriteByte(' ')
		buf.WriteString(line_fillings)
		buf.WriteByte('\n')
	}

	return buf.Bytes()
}

func writeLoop(fi *os.File) {
	for {
		b := createPiece()
		fi.Write(b)
		total += len(b)

		if allLines >= maxLines {
			fmt.Println("writeLoop done")
			fmt.Printf("lines:%d, total:%dMB, spends:%dms \n", allLines, total/(1<<20), (time.Now().UnixNano()-start)/(1000*1000))
			return
		}

		if allLines > (rounds * round) {
			rounds++
			fmt.Printf("lines:%d, total:%dMB, spends:%dms \n", allLines, total/(1<<20), (time.Now().UnixNano()-start)/(1000*1000))
		}
	}
}

func main() {
	fi, err := os.OpenFile("./50g.log", os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	defer fi.Close()
	fi.Seek(0, 0)

	runtime.GOMAXPROCS(3)

	fmt.Println("Begin...")
	start = time.Now().UnixNano()

	writeLoop(fi)

	end := time.Now().UnixNano()

	fmt.Printf("\n-----------------------\n")
	fmt.Printf("Done in %d ms!\n", (end-start)/(1000*1000))
	fmt.Printf("allLines %d\n", allLines)
	fmt.Printf("total %d byte equals %d MB\n", total, total/(1<<20))
	fmt.Printf("repeated %d\n", repeated)
	fmt.Printf("torepeat %d\n", torepeat)
}
