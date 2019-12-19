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

package v3ioutils

import (
	"net/http"

	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"github.com/v3io/v3io-go/pkg/dataplane"
	v3ioerrors "github.com/v3io/v3io-go/pkg/errors"
)

// ItemsCursor iterates over items
type ItemsCursor interface {
	Err() error
	Next() bool
	GetField(name string) interface{}
	GetFields() map[string]interface{}
}

// AsyncItemsCursor is async item cursor
type AsyncItemsCursor struct {
	currentItem        v3io.Item
	currentError       error
	itemIndex          int
	items              []v3io.Item
	input              *v3io.GetItemsInput
	container          v3io.Container
	logger             logger.Logger
	numberOfPartitions int

	responseChan  chan *v3io.Response
	workers       int
	totalSegments int
	lastShards    int
	Cnt           int
	limit         int
}

// NewAsyncItemsCursor return new AsyncItemsCursor
func NewAsyncItemsCursor(container v3io.Container, input *v3io.GetItemsInput, workers int, shardingKeys []string,
	logger logger.Logger, limit int, partitions []string,
	sortKeyRangeStart string, sortKeyRangeEnd string) (*AsyncItemsCursor, error) {

	// TODO: use workers from Context.numWorkers (if no ShardingKey)
	if workers == 0 || input.ShardingKey != "" {
		workers = 1
	}

	newAsyncItemsCursor := &AsyncItemsCursor{
		container:          container,
		input:              input,
		workers:            workers,
		logger:             logger.GetChild("AsyncItemsCursor"),
		limit:              limit,
		numberOfPartitions: len(partitions),
	}

	if len(shardingKeys) > 0 {
		newAsyncItemsCursor.workers = len(shardingKeys)
		newAsyncItemsCursor.responseChan = make(chan *v3io.Response, len(partitions)*newAsyncItemsCursor.workers)

		for _, partition := range partitions {
			for i := 0; i < newAsyncItemsCursor.workers; i++ {
				input := v3io.GetItemsInput{
					Path:              partition,
					AttributeNames:    input.AttributeNames,
					Filter:            input.Filter,
					ShardingKey:       shardingKeys[i],
					SortKeyRangeStart: sortKeyRangeStart,
					SortKeyRangeEnd:   sortKeyRangeEnd,
				}
				_, err := container.GetItems(&input, &input, newAsyncItemsCursor.responseChan)

				if err != nil {
					return nil, err
				}
			}
		}
	} else {
		newAsyncItemsCursor.totalSegments = workers
		newAsyncItemsCursor.responseChan = make(chan *v3io.Response, len(partitions)*newAsyncItemsCursor.workers)

		for _, partition := range partitions {
			for i := 0; i < newAsyncItemsCursor.workers; i++ {
				input := v3io.GetItemsInput{
					Path:           partition,
					AttributeNames: input.AttributeNames,
					Filter:         input.Filter,
					TotalSegments:  newAsyncItemsCursor.totalSegments,
					Segment:        i,
				}
				_, err := container.GetItems(&input, &input, newAsyncItemsCursor.responseChan)

				if err != nil {
					// TODO: proper exit, release requests which passed
					return nil, err
				}
			}
		}
	}

	return newAsyncItemsCursor, nil
}

// Err returns the last error
func (ic *AsyncItemsCursor) Err() error {
	return ic.currentError
}

// Release releases a cursor and its underlying resources
func (ic *AsyncItemsCursor) Release() {
	// TODO:
}

// Next gets the next matching item. this may potentially block as this lazy loads items from the collection
func (ic *AsyncItemsCursor) Next() bool {
	item, err := ic.NextItem()

	if item == nil || err != nil {
		ic.currentError = err
		return false
	}

	return true
}

// NextItem gets the next matching item. this may potentially block as this lazy loads items from the collection
func (ic *AsyncItemsCursor) NextItem() (v3io.Item, error) {

	// are there any more items left in the previous response we received?
	if ic.itemIndex < len(ic.items) {
		// if we read more rows than limit, return EOF
		if ic.limit > 0 && ic.Cnt > ic.limit {
			return nil, nil
		}

		ic.currentItem = ic.items[ic.itemIndex]
		ic.currentError = nil

		// next time we'll give next item
		ic.itemIndex++
		ic.Cnt++

		return ic.currentItem, nil
	}

	// are there any more items up stream? did all the shards complete ?
	if ic.lastShards == ic.workers*ic.numberOfPartitions {
		ic.currentError = nil
		return nil, nil
	}

	// Read response from channel
	resp := <-ic.responseChan
	resp.Release()

	// Ignore 404s
	if e, hasErrorCode := resp.Error.(v3ioerrors.ErrorWithStatusCode); hasErrorCode && e.StatusCode() == http.StatusNotFound {
		ic.logger.Debug("Got 404 - error: %v, request: %v", resp.Error, resp.Request().Input)
		ic.lastShards++
		return ic.NextItem()
	}
	if resp.Error != nil {
		ic.logger.Warn("error reading from response channel: %v, error: %v, request: %v", resp, resp.Error, resp.Request().Input)
		return nil, errors.Wrap(resp.Error, "Failed to get next items")
	}

	getItemsResp := resp.Output.(*v3io.GetItemsOutput)

	// set the cursor items and reset the item index
	ic.items = getItemsResp.Items
	ic.itemIndex = 0

	if !getItemsResp.Last {

		// if not last, make a new request to that shard
		input := resp.Context.(*v3io.GetItemsInput)

		// set next marker
		input.Marker = getItemsResp.NextMarker

		_, err := ic.container.GetItems(input, input, ic.responseChan)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to request next items")
		}

	} else {
		// Mark one more shard as completed
		ic.lastShards++
	}

	// and recurse into next now that we repopulated response
	return ic.NextItem()
}

// All returns all items
func (ic *AsyncItemsCursor) All() ([]v3io.Item, error) {
	var items []v3io.Item

	for ic.Next() {
		items = append(items, ic.GetItem())
	}

	if ic.Err() != nil {
		return nil, ic.Err()
	}

	return items, nil
}

// GetField returns a field
func (ic *AsyncItemsCursor) GetField(name string) interface{} {
	return ic.currentItem[name]
}

// GetFieldInt returns a field as int
func (ic *AsyncItemsCursor) GetFieldInt(name string) (int, error) {
	return ic.currentItem.GetFieldInt(name)
}

// GetFieldString returns a field as string
func (ic *AsyncItemsCursor) GetFieldString(name string) (string, error) {
	return ic.currentItem.GetFieldString(name)
}

// GetFields returns all fields
func (ic *AsyncItemsCursor) GetFields() map[string]interface{} {
	return ic.currentItem
}

// GetItem returns item
func (ic *AsyncItemsCursor) GetItem() v3io.Item {
	return ic.currentItem
}
