package watcher

import "github.com/google/uuid"

// Option is the option for the watcher.
type Option struct {
	Verbose bool   // Verbose indicates the watcher should print verbose log.
	LocalID string // LocalID indicates the watcher's local ID, used to ignore self update event. Generates a random id if not specified.
}

// GetVerbose gets the verbose for the option.
func (w *Watcher) GetVerbose() bool {
	return w.opt.Verbose
}

// GetLocalID gets the local id for the option.
func (w *Watcher) GetLocalID() string {
	if w.opt.LocalID == "" {
		w.opt.LocalID = uuid.New().String()
	}
	return w.opt.LocalID
}
