/*
Package watcher provides an implementation of [persist.WatcherEx], supporting
various pub/sub systems by leveraging the gocloud [pubsub] package.

For more details about the pub/sub systems supported by gocloud, please refer to
https://gocloud.dev/howto/pubsub/.
*/
package watcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	"github.com/casbin/casbin/v2/persist"
	"gocloud.dev/pubsub"
)

var _ persist.WatcherEx = &Watcher{}

var (
	// ErrNotConnected is the error returned when the watcher is not connected.
	ErrNotConnected = errors.New("pubsub not connected, cannot dispatch update message")
)

// Watcher implements Casbin updates watcher to synchronize policy changes
// between the nodes.
type Watcher struct {
	topicURL        string               // the pubsub topic url
	subscriptionURL string               // the pubsub subscription url
	callbackFunc    func(string)         // the update callback function that the watcher will call
	connMu          *sync.RWMutex        // the mutex for pubsub connections
	ctx             context.Context      // the context for pubsub connections
	topic           *pubsub.Topic        // the pubsub topic
	sub             *pubsub.Subscription // the pubsub subscription
	opt             Option               // the watcher option
}

// UpdateType is the type of update.
type UpdateType string

// Defines the update types.
const (
	Update                        UpdateType = "Update"
	UpdateForAddPolicy            UpdateType = "UpdateForAddPolicy"
	UpdateForRemovePolicy         UpdateType = "UpdateForRemovePolicy"
	UpdateForRemoveFilteredPolicy UpdateType = "UpdateForRemoveFilteredPolicy"
	UpdateForSavePolicy           UpdateType = "UpdateForSavePolicy"
	UpdateForAddPolicies          UpdateType = "UpdateForAddPolicies"
	UpdateForRemovePolicies       UpdateType = "UpdateForRemovePolicies"
	UpdateForUpdatePolicy         UpdateType = "UpdateForUpdatePolicy"
	UpdateForUpdatePolicies       UpdateType = "UpdateForUpdatePolicies"
)

// MSG represents the payload for a pub/sub message, detailing the type of update
// and the specifics of the policy change.
type MSG struct {
	Method      UpdateType `json:"method"`                 // Type of update.
	ID          string     `json:"id"`                     // Unique ID of the watcher instance.
	Sec         string     `json:"sec,omitempty"`          // Section of the policy being updated.
	Ptype       string     `json:"ptype,omitempty"`        // Type of policy being updated.
	OldRules    [][]string `json:"old_rules,omitempty"`    // Previous state of the policy rules.
	NewRules    [][]string `json:"new_rules,omitempty"`    // New state of the policy rules.
	FieldIndex  int        `json:"field_index,omitempty"`  // Index of the field being updated.
	FieldValues []string   `json:"field_values,omitempty"` // Values of the field being updated.
}

// MarshalBinary implements the encoding.BinaryMarshaler interface.
func (m MSG) MarshalBinary() ([]byte, error) {
	return json.Marshal(m)
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface.
func (m *MSG) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, m)
}

// DefaultCallback is the default callback function that the watcher will call
// when the policy in DB has been changed by other instances.
func DefaultCallback(e casbin.IEnforcer) func(string) {
	return func(s string) {
		m := &MSG{}
		if err := m.UnmarshalBinary([]byte(s)); err != nil {
			log.Printf("[go-cloud-watcher] failed to unmarshal msg: %v\n", err)
			return
		}

		var (
			res bool
			err error
		)
		switch m.Method {
		case Update, UpdateForSavePolicy:
			err = e.LoadPolicy()
			res = true
		case UpdateForAddPolicy:
			res, err = e.SelfAddPolicy(m.Sec, m.Ptype, m.NewRules[0])
		case UpdateForAddPolicies:
			res, err = e.SelfAddPolicies(m.Sec, m.Ptype, m.NewRules)
		case UpdateForRemovePolicy:
			res, err = e.SelfRemovePolicy(m.Sec, m.Ptype, m.NewRules[0])
		case UpdateForRemoveFilteredPolicy:
			res, err = e.SelfRemoveFilteredPolicy(m.Sec, m.Ptype, m.FieldIndex, m.FieldValues...)
		case UpdateForRemovePolicies:
			res, err = e.SelfRemovePolicies(m.Sec, m.Ptype, m.NewRules)
		case UpdateForUpdatePolicy:
			res, err = e.SelfUpdatePolicy(m.Sec, m.Ptype, m.OldRules[0], m.NewRules[0])
		case UpdateForUpdatePolicies:
			res, err = e.SelfUpdatePolicies(m.Sec, m.Ptype, m.OldRules, m.NewRules)
		default:
			err = fmt.Errorf("unknown update type: %s", m.Method)
		}
		if err != nil {
			log.Printf("[go-cloud-watcher] failed to update policy: %v\n", err)
		}
		if !res {
			log.Println("[go-cloud-watcher] callback update policy failed")
		}
	}
}

// New creates a new watcher
//
// Parameters:
//   - ctx: the context for pubsub connections
//   - url: the pubsub url (e.g. "kafka://my-topic")
//   - subscriptionURL (optional): the pubsub subscription url (e.g. for GCP Pub/Sub)
//
// Returns:
//   - Watcher: the new watcher instance
//   - error: the error if the watcher cannot be created
func New(ctx context.Context, url string, subscriptionURL ...string) (*Watcher, error) {
	if len(subscriptionURL) > 0 {
		return NewWithOption(ctx, url, subscriptionURL[0], Option{})
	}
	return NewWithOption(ctx, url, url, Option{})
}

// NewWithOption creates a new watcher with the option
//
// Parameters:
//   - ctx: the context for pubsub connections
//   - topicURL: the pubsub topic url (e.g. "kafka://my-topic")
//   - subscriptionURL: the pubsub subscription url (e.g. for GCP Pub/Sub)
//   - opt: the watcher option
//
// Returns:
//   - Watcher: the new watcher instance
//   - error: the error if the watcher cannot be created
func NewWithOption(ctx context.Context, topicURL string, subscriptionURL string, opt Option) (*Watcher, error) {
	w := &Watcher{
		topicURL:        topicURL,
		subscriptionURL: subscriptionURL,
		connMu:          &sync.RWMutex{},
		opt:             opt,
	}

	runtime.SetFinalizer(w, finalizer)

	err := w.initializeConnections(ctx)

	return w, err
}

// SetUpdateCallback sets the callback function that the watcher will call
// when the policy in DB has been changed by other instances.
// A classic callback is Enforcer.LoadPolicy().
func (w *Watcher) SetUpdateCallback(callbackFunc func(string)) error {
	w.connMu.Lock()
	w.callbackFunc = callbackFunc
	w.connMu.Unlock()
	return nil
}

// initializeConnections initializes the pubsub connections.
func (w *Watcher) initializeConnections(ctx context.Context) error {
	w.connMu.Lock()
	defer w.connMu.Unlock()
	w.ctx = ctx
	topic, err := pubsub.OpenTopic(ctx, w.topicURL)
	if err != nil {
		return err
	}
	w.topic = topic

	return w.subscribeToUpdates(ctx)
}

// subscribeToUpdates subscribes to the topic to receive updates.
func (w *Watcher) subscribeToUpdates(ctx context.Context) error {
	sub, err := pubsub.OpenSubscription(ctx, w.subscriptionURL)
	if err != nil {
		return fmt.Errorf("failed to open updates subscription, error: %w", err)
	}
	w.sub = sub
	go func() {
		for {
			msg, err := sub.Receive(ctx)
			if err != nil {
				if ctx.Err() == context.Canceled {
					// nothing to do
					return
				}
				log.Printf("Error while receiving an update message: %s\n", err)
				return
			}
			w.executeCallback(msg)

			msg.Ack()
		}
	}()
	return nil
}

// executeCallback executes the callback function.
func (w *Watcher) executeCallback(msg *pubsub.Message) {
	w.connMu.RLock()
	defer w.connMu.RUnlock()
	if w.callbackFunc != nil {
		go w.callbackFunc(string(msg.Body))
	}
}

// Update calls the update callback of other instances to synchronize their policy.
// It is usually called after changing the policy in DB, like Enforcer.SavePolicy(),
// Enforcer.AddPolicy(), Enforcer.RemovePolicy(), etc.
func (w *Watcher) Update() error {
	w.connMu.RLock()
	defer w.connMu.RUnlock()
	if w.topic == nil {
		return ErrNotConnected
	}
	return w.notifyMessage(&MSG{Method: Update, ID: w.GetLocalID()})
}

// Close stops and releases the watcher, the callback function will not be called any more.
func (w *Watcher) Close() {
	finalizer(w)
}

// UpdateForAddPolicy calls the update callback of other instances to synchronize their
// policy. It is called after a policy is added via Enforcer.AddPolicy(), Enforcer.AddNamedPolicy(),
// Enforcer.AddGroupingPolicy() and Enforcer.AddNamedGroupingPolicy().
func (w *Watcher) UpdateForAddPolicy(sec string, ptype string, params ...string) error {
	return w.notifyMessage(&MSG{
		Method:   UpdateForAddPolicy,
		ID:       w.GetLocalID(),
		Sec:      sec,
		Ptype:    ptype,
		NewRules: [][]string{params},
	})
}

// UPdateForRemovePolicy calls the update callback of other instances to
// synchronize their policy. It is called after a policy is removed by
// Enforcer.RemovePolicy(), Enforcer.RemoveNamedPolicy(),
// Enforcer.RemoveGroupingPolicy() and Enforcer.RemoveNamedGroupingPolicy().
func (w *Watcher) UpdateForRemovePolicy(sec string, ptype string, params ...string) error {
	return w.notifyMessage(&MSG{
		Method:   UpdateForRemovePolicy,
		ID:       w.GetLocalID(),
		Sec:      sec,
		Ptype:    ptype,
		NewRules: [][]string{params},
	})
}

// UpdateForRemoveFilteredPolicy calls the update callback of other instances to
// synchronize their policy. It is called after Enforcer.RemoveFilteredPolicy(),
// Enforcer.RemoveFilteredNamedPolicy(), Enforcer.RemoveFilteredGroupingPolicy()
// and Enforcer.RemoveFilteredNamedGroupingPolicy().
func (w *Watcher) UpdateForRemoveFilteredPolicy(sec string, ptype string, fieldIndex int, fieldValues ...string) error {
	return w.notifyMessage(&MSG{
		Method:      UpdateForRemoveFilteredPolicy,
		ID:          w.GetLocalID(),
		Sec:         sec,
		Ptype:       ptype,
		FieldIndex:  fieldIndex,
		FieldValues: fieldValues,
	})
}

// UpdateForSavePolicy calls the update callback of other instances to
// synchronize their policy. It is called after Enforcer.SavePolicy().
func (w *Watcher) UpdateForSavePolicy(model model.Model) error {
	return w.notifyMessage(&MSG{
		Method: UpdateForSavePolicy,
		ID:     w.GetLocalID(),
	})
}

// UpdateForAddPolicies calls the update callback of other instances to
// synchronize their policy. It is called after Enforcer.AddPolicies(),
// Enforcer.AddNamedPolicies(), Enforcer.AddGroupingPolicies() and
// Enforcer.AddNamedGroupingPolicies().
func (w *Watcher) UpdateForAddPolicies(sec string, ptype string, rules ...[]string) error {
	return w.notifyMessage(&MSG{
		Method:   UpdateForAddPolicies,
		ID:       w.GetLocalID(),
		Sec:      sec,
		Ptype:    ptype,
		NewRules: rules,
	})
}

// UpdateForRemovePolicies calls the update callback of other instances to
// synchronize their policy. It is called after Enforcer.RemovePolicies(),
// Enforcer.RemoveNamedPolicies(), Enforcer.RemoveGroupingPolicies() and
// Enforcer.RemoveNamedGroupingPolicies().
func (w *Watcher) UpdateForRemovePolicies(sec string, ptype string, rules ...[]string) error {
	return w.notifyMessage(&MSG{
		Method:   UpdateForRemovePolicies,
		ID:       w.GetLocalID(),
		Sec:      sec,
		Ptype:    ptype,
		NewRules: rules,
	})
}

// UpdateForUpdatePolicy calls the update callback of other instances to synchronize their policy.
// It is called after Enforcer.UpdatePolicy().
func (w *Watcher) UpdateForUpdatePolicy(sec string, ptype string, oldRule, newRule []string) error {
	return w.notifyMessage(&MSG{
		Method:   UpdateForUpdatePolicy,
		ID:       w.GetLocalID(),
		Sec:      sec,
		Ptype:    ptype,
		OldRules: [][]string{oldRule},
		NewRules: [][]string{newRule},
	})
}

// UpdateForUpdatePolicies calls the update callback of other instances to synchronize their policy.
// It is called after Enforcer.UpdatePolicies().
func (w *Watcher) UpdateForUpdatePolicies(sec string, ptype string, oldRules, newRules [][]string) error {
	return w.notifyMessage(&MSG{
		Method:   UpdateForUpdatePolicies,
		ID:       w.GetLocalID(),
		Sec:      sec,
		Ptype:    ptype,
		OldRules: oldRules,
		NewRules: newRules,
	})
}

// finalizer is the destructor for Watcher.
func finalizer(w *Watcher) {
	w.connMu.Lock()
	defer w.connMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if w.topic != nil {
		w.topic = nil
	}

	if w.sub != nil {
		err := w.sub.Shutdown(ctx)
		if err != nil {
			log.Printf("Subscription shutdown failed, error: %s\n", err)
		}
		w.sub = nil
	}

	w.callbackFunc = nil
}

// notifyMessage sends a message to the topic
//
// Parameters:
//   - msg: the message to send
//
// Returns:
// - error: the error if the message cannot be sent.
func (w *Watcher) notifyMessage(msg *MSG) error {
	msgBody, err := msg.MarshalBinary()
	if err != nil {
		return err
	}

	p := &pubsub.Message{Body: msgBody}

	if err := w.topic.Send(w.ctx, p); err != nil {
		return err
	}

	if w.GetVerbose() {
		log.Printf("[go-cloud-watcher] send message: %s\n", string(msgBody))
	}

	return nil
}
