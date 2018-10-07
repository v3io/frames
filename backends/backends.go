package backends

import (
	"fmt"
	"strings"
	"sync"

	"github.com/nuclio/logger"
	"github.com/v3io/frames"
)

var (
	factories     map[string]Factory
	lock          sync.RWMutex
	normalizeType = strings.ToLower
)

// Factory is a backend factory
type Factory func(logger.Logger, *frames.BackendConfig) (frames.DataBackend, error)

// Register registers a backend factory for a type
func Register(typ string, factory Factory) error {
	lock.Lock()
	defer lock.Unlock()

	if factories == nil {
		factories = make(map[string]Factory)
	}

	if _, ok := factories[typ]; ok {
		return fmt.Errorf("backend %q already registered", typ)
	}

	factories[normalizeType(typ)] = factory
	return nil
}

// GetFactory returns factory for a backend
func GetFactory(typ string) Factory {
	lock.RLock()
	defer lock.RUnlock()

	return factories[normalizeType(typ)]
}
