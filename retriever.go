package cdretriever

import (
	"context"
	"fmt"
	"time"

	"go.linecorp.com/centraldogma"
)

// Retriever is a configuration struct for a CentralDogma retriever.
type Retriever struct {
	Watcher *centraldogma.Watcher
}

// NewRetriever creates a new CentralDogma retriever.
func NewRetriever(baseURL, token, project, repo, path string) (*Retriever, error) {
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

	if result := watcher.AwaitInitialValueWith(30 * time.Second); result.Err != nil {
		return nil, fmt.Errorf("cdretriever: failed to retrieve the initial value: %w", result.Err)
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
