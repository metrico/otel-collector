// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package qrynexporter

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
)

// generateInsertQuery creates a SQL insert query for a slice of structs.
// tableName is the base SQL command for insertion, e.g., "INSERT INTO table_name (column1, column2, ...)"
func generateInsertQuery(data interface{}, tableName string) (string, []interface{}) {
	// Reflect on the data slice to dynamically generate the SQL query and collect values.
	v := reflect.ValueOf(data)
	if v.Kind() != reflect.Slice {
		panic("generateInsertQuery expects a slice of structs")
	}

	var placeholders strings.Builder
	var vals []interface{}
	for i := 0; i < v.Len(); i++ {
		item := v.Index(i)
		if item.Kind() != reflect.Struct {
			panic("generateInsertQuery expects a slice of structs")
		}
		if i > 0 {
			placeholders.WriteString(", ")
		}
		placeholders.WriteString("(")
		for j := 0; j < item.NumField(); j++ {
			if j > 0 {
				placeholders.WriteString(", ")
			}
			placeholders.WriteString("?")
			vals = append(vals, item.Field(j).Interface())
		}
		placeholders.WriteString(")")
	}

	query := tableName + " VALUES " + placeholders.String()
	return query, vals
}

func executeBatchInsert(ctx context.Context, tx *sql.Tx, data interface{}, sqlBase string) error {
	query, values := generateInsertQuery(data, sqlBase)
	_, err := tx.ExecContext(ctx, query, values...)
	return err
}

func withTransaction(ctx context.Context, db *sql.DB, fn func(*sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		tx.Rollback() // Rollback on error.
		return err
	}
	return tx.Commit() // Commit if all operations are successful.
}
