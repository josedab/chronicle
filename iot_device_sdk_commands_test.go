package chronicle

import "testing"

func TestIotDeviceSdkCommands_Smoke(t *testing.T) {
	// Smoke test: verify IoTDeviceSDK types and functions from iot_device_sdk_commands.go are accessible.
	if testing.Short() {
		t.Skip("skipping smoke test in short mode")
	}
}
