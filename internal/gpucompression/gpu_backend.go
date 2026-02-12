package gpucompression

func (b GPUBackend) String() string {
	switch b {
	case GPUBackendCUDA:
		return "cuda"
	case GPUBackendMetal:
		return "metal"
	case GPUBackendOpenCL:
		return "opencl"
	case GPUBackendWebGPU:
		return "webgpu"
	case GPUBackendVulkan:
		return "vulkan"
	default:
		return "none"
	}
}
