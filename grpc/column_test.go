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

package grpc

import (
	"testing"
	"time"

	"github.com/v3io/frames/pb"
)

func TestLabelColTimeAt(t *testing.T) {
	ts := time.Now()
	col := colImpl{
		msg: &pb.Column{
			Times: []int64{ts.UnixNano()},
			Dtype: pb.DType_TIME,
			Kind:  pb.Column_LABEL,
			Size:  20,
		},
	}

	ts1, err := col.TimeAt(3)
	if err != nil {
		t.Fatal(err)
	}

	if ts1.Round(time.Millisecond) != ts.Round(time.Millisecond) {
		t.Fatalf("bad time %v != %v", ts1, ts)
	}

}
