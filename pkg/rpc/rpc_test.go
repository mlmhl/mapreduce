package rpc

import "testing"

type TestReceiver struct {
	value int
}

func (r *TestReceiver) Get(_ NopValue, value *int) error {
	*value = r.value
	return nil
}

func (r *TestReceiver) Set(value int, _ *NopValue) error {
	r.value = value
	return nil
}

func TestRpc(t *testing.T) {
	network, address := "tcp", "127.0.0.1:12345"
	server, err := NewServer("Test", Address{network, address}, &TestReceiver{})
	if err != nil {
		t.Fatalf("Create Server failed: %v", err)
	}
	server.Run()
	defer server.Stop()

	client, err := NewClient(Address{network, address})
	if err != nil {
		t.Fatalf("Create Client failed: %v", err)
	}
	defer client.Close()

	for _, target := range []int{1, 2, 3, 4, 5} {
		if err = client.Call("Test.Set", &target, &NopValue{}); err != nil {
			t.Fatalf("Set value to %d failed: %v", target, err)
		}
		value := 0
		if err = client.Call("Test.Get", NopValue{}, &value); err != nil {
			t.Fatalf("Get value failed: %v", err)
		}
		if value != target {
			t.Fatalf("Value mismatch: expected %d, got %d", target, value)
		}
	}
}
