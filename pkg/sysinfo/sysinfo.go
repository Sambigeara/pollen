package sysinfo

import (
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/mem"
)

// Sample returns current CPU usage (0-100), memory usage (0-100), and total
// memory in bytes. Errors from the underlying platform calls are silently
// ignored and zero values are returned.
func Sample() (cpuPercent, memPercent uint32, memTotalBytes uint64) {
	if pcts, err := cpu.Percent(0, false); err == nil && len(pcts) > 0 {
		cpuPercent = uint32(pcts[0])
	}
	if vm, err := mem.VirtualMemory(); err == nil && vm != nil {
		memPercent = uint32(vm.UsedPercent)
		memTotalBytes = vm.Total
	}
	return cpuPercent, memPercent, memTotalBytes
}
