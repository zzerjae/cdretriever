package cdretriever_test

import (
	"context"
	"os"
	"testing"

	"github.com/zzerjae/cdretriever"

	"github.com/stretchr/testify/assert"
)

func TestRetriever_Retrieve(t *testing.T) {
	// Let's Assume you already have an accessible CentralDogma running and have the environment variables
	// in place to access that server.
	r, err := cdretriever.NewRetriever(
		os.Getenv("CDRETRIEVER_BASE_URL"),
		os.Getenv("CDRETRIEVER_TOKEN"),
		os.Getenv("CDRETRIEVER_PROJECT"),
		os.Getenv("CDRETRIEVER_REPO"),
		"flag-config.yaml",
	)
	if err != nil {
		t.Fatal(err)
	}

	want := []byte(`test-flag:
  variations:
    true_var: true
    false_var: false
  targeting:
    - query: key eq "random-key"
      percentage:
        true_var: 0
        false_var: 100
  defaultRule:
    variation: false_var
`)

	got, err := r.Retrieve(context.Background())
	if !assert.NoErrorf(t, err, "Retrieve() error = %v", err) {
		return
	}
	assert.Equalf(t, want, got, "Retrieve() got = %v, want %v", got, want)

	_ = r.Close()
}
