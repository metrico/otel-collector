package ch

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// Regression test for #142: a profile carrying an empty tree (treeSize <= 0)
// used to make readTreeFromMap return (nil, nil), and the caller then
// dereferenced the nil *PooledTree (tree = append(tree, _tree.data)),
// crashing with SIGSEGV.
func TestReadTreeFromMap_EmptyTreeDoesNotPanic(t *testing.T) {
	m := pcommon.NewMap()
	// A single 0x00 byte decodes as varint 0 -> treeSize == 0.
	m.PutEmptyBytes("tree").FromRaw([]byte{0x00})

	res, err := readTreeFromMap(m)
	require.NoError(t, err)
	require.NotNil(t, res, "empty tree must yield a non-nil PooledTree, not nil")
	assert.Empty(t, res.data)

	// Both the caller's data append and the pool release must be safe.
	assert.NotPanics(t, func() {
		_ = append([]interface{}{}, res.data)
		trees.put(res)
	})
}
