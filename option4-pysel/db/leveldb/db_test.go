package leveldb_test

import (
	"os"
	"testing"

	"github.com/pysel/dkvs/db/leveldb"
	"github.com/stretchr/testify/require"
)

func TestNewLevelDB(t *testing.T) {
	db, err := leveldb.NewLevelDB("test")
	require.NoError(t, err)

	defer db.Close()
	defer require.NoError(t, os.RemoveAll("test"))

	if db == nil {
		t.Error("db is nil")
	}
}
