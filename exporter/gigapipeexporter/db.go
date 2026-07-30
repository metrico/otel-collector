// Copyright  The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gigapipeexporter

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// chConn is the narrow subset of clickhouse.Conn that the exporters actually use.
// It exists purely as a testing seam: the *clickhouse.Conn value returned by
// clickhouse.Open already satisfies it, so production wiring and runtime
// behaviour are unchanged, while tests can substitute an in-memory fake.
type chConn interface {
	PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error)
	Close() error
}
