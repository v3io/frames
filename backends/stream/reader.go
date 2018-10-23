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

package stream

import (
	"encoding/json"
	"fmt"
	"github.com/v3io/frames"
	"github.com/v3io/v3io-go-http"
	"github.com/v3io/v3io-tsdb/pkg/utils"
	"strings"
	"time"
)

type streamIterator struct {
	request      *frames.ReadRequest
	err          error
	currFrame    frames.Frame
	nextLocation string
	b            *Backend
	endTime      int
	isLast       bool
}

func (b *Backend) Read(request *frames.ReadRequest) (frames.FrameIterator, error) {

	iter := streamIterator{request: request, b: b}

	if request.Table == "" || request.Seek == "" || request.ShardID == "" {
		return nil, fmt.Errorf("missing essential paramaters, need: table, seek, shard parameters")
	}
	input := v3io.SeekShardInput{Path: request.Table + "/" + request.ShardID}

	if request.MaxInMessage == 0 {
		request.MaxInMessage = 1024
	}

	if request.End != "" {
		endTime, err := utils.Str2unixTime(request.End)
		if err != nil {
			return nil, err
		}
		iter.endTime = int(endTime)
	}

	switch strings.ToLower(request.Seek) {
	case "time":
		input.Type = v3io.SeekShardInputTypeTime
		seekTime, err := utils.Str2unixTime(request.Start)
		if err != nil {
			return nil, err
		}
		input.Timestamp = int(seekTime / 1000)
	case "seq", "sequence":
		input.Type = v3io.SeekShardInputTypeSequence
		input.StartingSequenceNumber = request.Sequence
	case "latest", "late":
		input.Type = v3io.SeekShardInputTypeLatest
	case "earliest":
		input.Type = v3io.SeekShardInputTypeEarliest
	default:
		return nil, fmt.Errorf(
			"Stream seek type %s is invalid, use time | seq | latest | earliest", request.Start)

	}

	resp, err := b.container.Sync.SeekShard(&input)
	if err != nil {
		return nil, fmt.Errorf("Error in Seek operation - %v", err)
	}
	iter.nextLocation = resp.Output.(*v3io.SeekShardOutput).Location

	return &iter, nil
}

func (i *streamIterator) Next() bool {

	if i.isLast {
		return false
	}

	resp, err := i.b.container.Sync.GetRecords(&v3io.GetRecordsInput{
		Path:     i.request.Table + "/" + i.request.ShardID,
		Location: i.nextLocation,
		Limit:    i.request.MaxInMessage,
	})

	if err != nil {
		i.err = fmt.Errorf("Error in GetRecords operation (%v)", err)
		return false
	}

	output := resp.Output.(*v3io.GetRecordsOutput)
	rows := []map[string]interface{}{}
	var lastSequence int
	for _, r := range output.Records {

		if i.endTime > 0 && r.ArrivalTimeSec > i.endTime {
			i.isLast = true
			break
		}

		recTime := time.Unix(int64(r.ArrivalTimeSec), int64(r.ArrivalTimeNSec))
		i.b.logger.DebugWith("got stream record", "Time:", recTime, "Seq:", r.SequenceNumber, "Body:", string(r.Data))

		row := map[string]interface{}{}
		err := json.Unmarshal(r.Data, &row)
		if err != nil {
			i.err = fmt.Errorf("Failed to unmarshal result - %v", err)
			return false
		}
		row["stream_time"] = recTime

		rows = append(rows, row)
	}

	labels := map[string]interface{}{"last_seq": lastSequence}
	frame, err := frames.NewFrameFromRows(rows, []string{}, labels)
	if err != nil {
		i.err = fmt.Errorf("Failed to create frame - %v", err)
		return false
	}
	i.currFrame = frame

	i.nextLocation = output.NextLocation
	i.isLast = i.isLast || (output.RecordsBehindLatest == 0)

	// TODO: add timeout option, keep polling on stream for t more time
	return true
}

func (i *streamIterator) Err() error {
	return i.err
}

func (i *streamIterator) At() frames.Frame {
	return i.currFrame
}
