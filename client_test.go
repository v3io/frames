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
	"testing/quick"
)

func TestNewClient(t *testing.T) {
	fn := func(url string, apiKey string) bool {
		client, err := NewClient(url, apiKey, nil)
		if err != nil {
			t.Logf("can't create client - %s", err)
			return false
		}

		if client.URL != url {
			t.Logf("URL mismatch: %q != %q", client.URL, url)
			return false
		}

		if client.apiKey != apiKey {
			t.Logf("api key mismatch: %q != %q", client.apiKey, apiKey)
			return false
		}

		if client.logger == nil {
			t.Log("no logger")
			return false
		}

		return true
	}

	if err := quick.Check(fn, nil); err != nil {
		t.Fatal(err)
	}
}

func TestClientRead(t *testing.T) {
	t.Skip("TODO")
}

func TestClientWrite(t *testing.T) {
	t.Skip("TODO")
}
