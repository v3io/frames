// +build arrow

/*
Copyright 2018 Iguazio Systems Ltd.

Licensed under the Apache License, Version 2.0 (the "License") with
an addition restriction as set forth herein. You may not use this
file except in compliance with the License. You may obtain a copy of
the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.

In addition, you may not use the software for any purposes that are
illegal under applicable law, and the grant of the foregoing license
under the Apache 2.0 license is conditioned upon your compliance with
such restriction.
*/

package frames

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestColumns(t *testing.T) {
	require := require.New(t)

	col1, err := NewArrowColumn("col1", []int64{1, 2, 3, 4})
	require.NoError(err, "col1")
	col2, err := NewArrowColumn("col2", []float64{1, 2, 3, 4})
	require.NoError(err, "col2")
	idx, err := NewArrowColumn("idx1", []string{"a", "b", "c", "d"})
	require.NoError(err, "idx1")

	cols := []Column{col1, col2}
	idxs := []Column{idx}

	frame, err := NewArrowFrame(cols, idxs, nil)
	require.NoError(err, "new frame")
	require.Equal(2, len(frame.Names()))

	idxs2 := frame.Indices()
	require.Equal(1, len(idxs2), "indices")
	require.Equal(idx.Name(), idxs2[0].Name(), "index name")
}
