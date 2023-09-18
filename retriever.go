package cdretriever

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.linecorp.com/centraldogma"
)

// Retriever is a configuration struct for a CentralDogma retriever.
type Retriever struct {
	Watcher *centraldogma.Watcher
}

// NewRetriever creates a new CentralDogma retriever.
func NewRetriever(baseURL, token, project, repo, path string, opts ...Option) (*Retriever, error) {
	cfg := &config{}
	for _, opt := range opts {
		opt(cfg)
	}

	c, err := centraldogma.NewClientWithToken(baseURL, token, nil)
	if err != nil {
		return nil, fmt.Errorf("cdretriever: failed to create a new CentralDogma client: %w", err)
	}

	watcher, err := c.FileWatcher(project, repo, &centraldogma.Query{
		Path: path,
		Type: centraldogma.Identity,
	})
	if err != nil {
		return nil, fmt.Errorf("cdretriever: failed to create a new CentralDogma watcher: %w", err)
	}

	if cfg.awaitInitialValueWith != nil {
		if result := watcher.AwaitInitialValueWith(*cfg.awaitInitialValueWith); result.Err != nil {
			return nil, &ErrAwaitInitialValue{cause: result.Err}
		}
	}

	return &Retriever{Watcher: watcher}, nil
}

// Retrieve retrieves the configuration from CentralDogma.
func (r *Retriever) Retrieve(_ context.Context) ([]byte, error) {
	result := r.Watcher.Latest()
	if result.Err != nil {
		return nil, fmt.Errorf("cdretriever: failed to retrieve the latest value: %w", result.Err)
	}

	return result.Entry.Content, nil
}

// Close closes the CentralDogma retriever.
func (r *Retriever) Close() error {
	r.Watcher.Close()
	return nil
}

type Option func(*config)

type config struct {
	awaitInitialValueWith *time.Duration
}

func WithAwaitInitialValue(dur time.Duration) Option {
	return func(c *config) {
		c.awaitInitialValueWith = &dur
	}
}

func IsErrAwaitInitialValue(err error) bool {
	var errAwaitInitialValue *ErrAwaitInitialValue
	ok := errors.As(err, &errAwaitInitialValue)
	return ok
}

type ErrAwaitInitialValue struct {
	cause error
}

func (e *ErrAwaitInitialValue) Error() string {
	return fmt.Sprintf("cdretriever: failed to retrieve the initial value: %s", e.cause)
}
