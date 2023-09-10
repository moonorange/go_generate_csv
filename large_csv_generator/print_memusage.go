package large_csv_generator

import (
	"fmt"
	"runtime"
)

// PrintMemUsage outputs the current, total and OS memory being used. As well as the number
// of garage collection cycles completed.
func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// Alloc is bytes of allocated heap objects.
	// "Allocated" heap objects include all reachable objects, as well as unreachable objects that the garbage collector has not yet freed.
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	// TotalAlloc is cumulative bytes allocated for heap objects.
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	// Sys is the total bytes of memory obtained from the OS.
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	// NumGC is the number of completed GC cycles.
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
