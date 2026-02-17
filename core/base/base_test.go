package base

import (
	"testing"
)

func TestBase(t *testing.T) {
	ipAddress, err := checkIPAddress("localhost")
	if err != nil {
		t.Errorf("%s", err.Error())
	}
	if ipAddress != "127.0.0.1" {
		t.Errorf("Wrong ip address: %s instead of 127.0.0.1", ipAddress)
	}

	ipAddress, err = checkIPAddress("216.3.128.12")
	if err != nil {
		t.Errorf("%s", err.Error())
	}
	if ipAddress != "216.3.128.12" {
		t.Errorf("Wrong ip address: %s instead of 216.3.128.12", ipAddress)
	}

	ipAddress, err = checkIPAddress("")
	if err != nil {
		t.Errorf("%s", err.Error())
	}
	if ipAddress == "" {
		t.Errorf("IP address not set")
	}
}
