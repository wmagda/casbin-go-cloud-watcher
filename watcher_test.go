package watcher

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/bartventer/casbin-go-cloud-watcher/drivers/mempubsub"
	_ "github.com/bartventer/casbin-go-cloud-watcher/drivers/natspubsub"
	"github.com/casbin/casbin/v2"
	gnatsd "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

func TestNATSWatcher(t *testing.T) {
	// Setup nats server
	s := gnatsd.RunDefaultServer()
	defer s.Shutdown()

	natsEndpoint := fmt.Sprintf("nats://localhost:%d", nats.DefaultPort)
	natsSubject := "nats://casbin-policy-updated-subject"

	updaterCh := make(chan string, 1)
	listenerCh := make(chan string, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// gocloud.dev connects to NATS server based on env variable
	os.Setenv("NATS_SERVER_URL", natsEndpoint)

	// updater represents the Casbin enforcer instance that changes the policy in DB
	// Use the endpoint of nats as parameter.
	updater, err := New(ctx, natsSubject)
	if err != nil {
		t.Fatalf("Failed to create updater, error: %s", err)
	}
	defer updater.Close()
	updater.SetUpdateCallback(func(msg string) {
		updaterCh <- "updater"
	})

	// listener represents any other Casbin enforcer instance that watches the change of policy in DB
	listener, err := New(ctx, natsSubject)
	if err != nil {
		t.Fatalf("Failed to create second listener: %s", err)
	}
	defer listener.Close()

	// listener should set a callback that gets called when policy changes
	err = listener.SetUpdateCallback(func(msg string) {
		listenerCh <- "listener"
	})
	if err != nil {
		t.Fatalf("Failed to set listener callback: %s", err)
	}

	// updater changes the policy, and sends the notifications.
	err = updater.Update()
	if err != nil {
		t.Fatalf("The updater failed to send Update: %s", err)
	}

	// Validate that listener received message
	var updaterReceived bool
	var listenerReceived bool
	for {
		select {
		case res := <-listenerCh:
			if res != "listener" {
				t.Fatalf("Message from unknown source: %v", res)
			}
			listenerReceived = true
		case res := <-updaterCh:
			if res != "updater" {
				t.Fatalf("Message from unknown source: %v", res)
			}
			updaterReceived = true
		case <-time.After(time.Second * 10):
			t.Fatal("Updater or listener didn't received message in time")
		}
		if updaterReceived && listenerReceived {
			close(listenerCh)
			close(updaterCh)
			break
		}
	}
}

func TestWithEnforcerNATS(t *testing.T) {
	// Setup nats server
	s := gnatsd.RunDefaultServer()
	defer s.Shutdown()

	natsEndpoint := fmt.Sprintf("nats://localhost:%d", nats.DefaultPort)
	natsSubject := "nats://casbin-policy-updated-subject"

	cannel := make(chan string, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// gocloud.dev connects to NATS server based on env variable
	os.Setenv("NATS_SERVER_URL", natsEndpoint)

	w, err := New(ctx, natsSubject)
	if err != nil {
		t.Fatalf("Failed to create updater, error: %s", err)
	}
	defer w.Close()

	// Initialize the enforcer.
	e, _ := casbin.NewEnforcer("./test_data/model.conf", "./test_data/policy.csv")

	// Set the watcher for the enforcer.
	e.SetWatcher(w)

	// By default, the watcher's callback is automatically set to the
	// enforcer's LoadPolicy() in the SetWatcher() call.
	// We can change it by explicitly setting a callback.
	w.SetUpdateCallback(func(msg string) {
		cannel <- "enforcer"
	})

	// Update the policy to test the effect.
	e.SavePolicy()

	// Validate that listener received message
	select {
	case res := <-cannel:
		if res != "enforcer" {
			t.Fatalf("Got unexpected message :%v", res)
		}
	case <-time.After(time.Second * 10):
		t.Fatal("The enforcer didn't send message in time")
	}
	close(cannel)
}

func TestWithEnforcerMemory(t *testing.T) {

	endpointURL := "mem://topicA"

	cannel := make(chan string, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w, err := New(ctx, endpointURL)
	if err != nil {
		t.Fatalf("Failed to create updater, error: %s", err)
	}
	defer w.Close()

	// Initialize the enforcer.
	e, _ := casbin.NewEnforcer("./test_data/model.conf", "./test_data/policy.csv")

	// Set the watcher for the enforcer.
	e.SetWatcher(w)

	// By default, the watcher's callback is automatically set to the
	// enforcer's LoadPolicy() in the SetWatcher() call.
	// We can change it by explicitly setting a callback.
	w.SetUpdateCallback(func(msg string) {
		cannel <- "enforcer"
	})

	// Update the policy to test the effect.
	e.SavePolicy()

	// Validate that listener received message
	select {
	case res := <-cannel:
		if res != "enforcer" {
			t.Fatalf("Got unexpected message :%v", res)
		}
	case <-time.After(time.Second * 5):
		t.Fatal("The enforcer didn't send message in time")
	}
	close(cannel)
}

// Ensure that we can still use the same topic name
func TestWithEnforcerMemoryB(t *testing.T) {

	// endpointURL := "mem://topicA"
	endpointURL := "mem://topicA"

	cannel := make(chan string, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w, err := New(ctx, endpointURL)
	if err != nil {
		t.Fatalf("Failed to create updater, error: %s", err)
	}
	defer w.Close()

	// Initialize the enforcer.
	e, _ := casbin.NewEnforcer("./test_data/model.conf", "./test_data/policy.csv")

	// Set the watcher for the enforcer.
	e.SetWatcher(w)

	// By default, the watcher's callback is automatically set to the
	// enforcer's LoadPolicy() in the SetWatcher() call.
	// We can change it by explicitly setting a callback.
	w.SetUpdateCallback(func(msg string) {
		cannel <- "enforcer"
	})

	// Update the policy to test the effect.
	e.SavePolicy()

	// Validate that listener received message
	select {
	case res := <-cannel:
		if res != "enforcer" {
			t.Fatalf("Got unexpected message :%v", res)
		}
	case <-time.After(time.Second * 5):
		t.Fatal("The enforcer didn't send message in time")
	}
	close(cannel)
}

func initWithOption(t *testing.T, opt Option) (*Watcher, *casbin.Enforcer) {
	w, err := NewWithOption(context.Background(), "mem://topicA", opt)
	if err != nil {
		t.Fatalf("failed to new watcher: %v", err)
	}

	// create enforcer.
	e, err := casbin.NewEnforcer("test_data/model.conf", "test_data/policy.csv")
	if err != nil {
		t.Fatalf("failed to new enforcer: %v", err)
	}

	return w, e
}

func TestWatcherAddPolicy(t *testing.T) {
	w, e := initWithOption(t, Option{
		LocalID: "local-id",
	})

	if err := e.SetWatcher(w); err != nil {
		t.Fatalf("failed to set watcher: %v", err)
	}

	recv := ""
	if err := w.SetUpdateCallback(func(s string) {
		recv = s
	}); err != nil {
		t.Fatalf("failed to set update callback: %v", err)
	}

	if _, err := e.AddPolicy("alice", "data2", "read"); err != nil {
		t.Fatalf("failed to update: %v", err)
	}

	time.Sleep(time.Millisecond * 300)
	w.Close()

	if recv != `{"method":"UpdateForAddPolicy","id":"local-id","sec":"p","ptype":"p","new_rules":[["alice","data2","read"]]}` {
		t.Fatalf("unexpected msg: %s", recv)
	}
}

func TestWatcherRemovePolicy(t *testing.T) {
	w, e := initWithOption(t, Option{
		LocalID: "local-id",
	})

	if err := e.SetWatcher(w); err != nil {
		t.Fatalf("failed to set watcher: %v", err)
	}

	recv := ""
	if err := w.SetUpdateCallback(func(s string) {
		recv = s
	}); err != nil {
		t.Fatalf("failed to set update callback: %v", err)
	}

	if _, err := e.RemovePolicy("alice", "data1", "read"); err != nil {
		t.Fatalf("failed to update: %v", err)
	}

	time.Sleep(time.Millisecond * 300)
	w.Close()

	if recv != `{"method":"UpdateForRemovePolicy","id":"local-id","sec":"p","ptype":"p","new_rules":[["alice","data1","read"]]}` {
		t.Fatalf("unexpected msg: %s", recv)
	}
}

func TestWatcherSavePolicy(t *testing.T) {
	w, e := initWithOption(t, Option{
		LocalID: "local-id",
	})

	if err := e.SetWatcher(w); err != nil {
		t.Fatalf("failed to set watcher: %v", err)
	}

	recv := ""
	if err := w.SetUpdateCallback(func(s string) {
		recv = s
	}); err != nil {
		t.Fatalf("failed to set update callback: %v", err)
	}

	if err := e.SavePolicy(); err != nil {
		t.Fatalf("failed to update: %v", err)
	}

	time.Sleep(time.Millisecond * 300)
	w.Close()

	if recv != `{"method":"UpdateForSavePolicy","id":"local-id"}` {
		t.Fatalf("unexpected msg: %s", recv)
	}
}

func TestWatcherAddPolicies(t *testing.T) {
	w, e := initWithOption(t, Option{
		LocalID: "local-id",
	})

	if err := e.SetWatcher(w); err != nil {
		t.Fatalf("failed to set watcher: %v", err)
	}

	recv := ""
	if err := w.SetUpdateCallback(func(s string) {
		recv = s
	}); err != nil {
		t.Fatalf("failed to set update callback: %v", err)
	}

	if _, err := e.AddPolicies([][]string{
		{"alice", "data2", "read"},
		{"alice", "data3", "read"},
	}); err != nil {
		t.Fatalf("failed to update: %v", err)
	}

	time.Sleep(time.Millisecond * 300)
	w.Close()

	if recv != `{"method":"UpdateForAddPolicies","id":"local-id","sec":"p","ptype":"p","new_rules":[["alice","data2","read"],["alice","data3","read"]]}` {
		t.Fatalf("unexpected msg: %s", recv)
	}
}

func TestWatcherRemovePolicies(t *testing.T) {
	w, e := initWithOption(t, Option{
		LocalID: "local-id",
	})

	if err := e.SetWatcher(w); err != nil {
		t.Fatalf("failed to set watcher: %v", err)
	}

	recv := ""
	if err := w.SetUpdateCallback(func(s string) {
		recv = s
	}); err != nil {
		t.Fatalf("failed to set update callback: %v", err)
	}

	if _, err := e.RemovePolicies([][]string{
		{"alice", "data1", "read"},
		{"bob", "data2", "write"},
	}); err != nil {
		t.Fatalf("failed to update: %v", err)
	}

	time.Sleep(time.Millisecond * 300)
	w.Close()

	if recv != `{"method":"UpdateForRemovePolicies","id":"local-id","sec":"p","ptype":"p","new_rules":[["alice","data1","read"],["bob","data2","write"]]}` {
		t.Fatalf("unexpected msg: %s", recv)
	}
}

func TestWatcherUpdatePolicy(t *testing.T) {
	w, e := initWithOption(t, Option{
		LocalID: "local-id",
	})

	if err := e.SetWatcher(w); err != nil {
		t.Fatalf("failed to set watcher: %v", err)
	}

	recv := ""
	if err := w.SetUpdateCallback(func(s string) {
		recv = s
	}); err != nil {
		t.Fatalf("failed to set update callback: %v", err)
	}

	if _, err := e.UpdatePolicy(
		[]string{"alice", "data1", "read"},
		[]string{"alice", "data1", "write"},
	); err != nil {
		t.Fatalf("failed to update: %v", err)
	}

	time.Sleep(time.Millisecond * 300)
	w.Close()

	if recv != `{"method":"UpdateForUpdatePolicy","id":"local-id","sec":"p","ptype":"p","old_rules":[["alice","data1","read"]],"new_rules":[["alice","data1","write"]]}` {
		t.Fatalf("unexpected msg: %s", recv)
	}
}

func TestWatcherUpdatePolicies(t *testing.T) {
	w, e := initWithOption(t, Option{
		LocalID: "local-id",
	})

	if err := e.SetWatcher(w); err != nil {
		t.Fatalf("failed to set watcher: %v", err)
	}

	recv := ""
	if err := w.SetUpdateCallback(func(s string) {
		recv = s
	}); err != nil {
		t.Fatalf("failed to set update callback: %v", err)
	}

	if _, err := e.UpdatePolicies(
		[][]string{{"alice", "data1", "read"}},
		[][]string{{"alice", "data1", "write"}},
	); err != nil {
		t.Fatalf("failed to update: %v", err)
	}

	time.Sleep(time.Millisecond * 300)
	w.Close()

	if recv != `{"method":"UpdateForUpdatePolicies","id":"local-id","sec":"p","ptype":"p","old_rules":[["alice","data1","read"]],"new_rules":[["alice","data1","write"]]}` {
		t.Fatalf("unexpected msg: %s", recv)
	}
}

func TestWatcherWithDefaultCallback(t *testing.T) {
	w1, e1 := initWithOption(t, Option{})
	w2, e2 := initWithOption(t, Option{})

	if err := e1.SetWatcher(w1); err != nil {
		t.Fatalf("failed to set watcher1: %v", err)
	}
	if err := e2.SetWatcher(w2); err != nil {
		t.Fatalf("failed to set watcher2: %v", err)
	}

	if err := w1.SetUpdateCallback(DefaultCallback(e1)); err != nil {
		t.Fatalf("failed to set update callback1: %v", err)
	}
	if err := w2.SetUpdateCallback(DefaultCallback(e2)); err != nil {
		t.Fatalf("failed to set update callback2: %v", err)
	}

	if _, err := e1.AddPolicy("foo", "data1", "read"); err != nil {
		t.Fatalf("failed to update: %v", err)
	}

	time.Sleep(time.Millisecond * 300)
	w1.Close()
	w2.Close()

	policies := e1.GetFilteredPolicy(0, "foo")
	if len(policies) == 1 && policies[0][0] == "foo" && policies[0][1] == "data1" && policies[0][2] == "read" {
		return
	}
	t.Fatalf("unexpected policy: %+v", policies)
}
