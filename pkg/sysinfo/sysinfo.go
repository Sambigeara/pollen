package sysinfo

import (
	"runtime"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/mem"
)

func Sample() (cpuPercent, memPercent uint32, memTotalBytes uint64, numCPU uint32) {
	if pcts, err := cpu.Percent(0, false); err == nil && len(pcts) > 0 {
		cpuPercent = uint32(pcts[0])
	}
	if vm, err := mem.VirtualMemory(); err == nil {
		memPercent = uint32(vm.UsedPercent)
		memTotalBytes = vm.Total
	}
	numCPU = uint32(runtime.NumCPU()) //nolint:gosec
	return cpuPercent, memPercent, memTotalBytes, numCPU
}
