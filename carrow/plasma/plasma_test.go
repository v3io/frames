package plasma

import (
	"testing"

	"github.com/stretchr/testify/require"
)


func TestRandomID(t *testing.T) {
	require := require.New(t)

	oid, err := RandomID()
	require.NoError(err, "create id")
	require.Len(oid, 20, "bad length")
}