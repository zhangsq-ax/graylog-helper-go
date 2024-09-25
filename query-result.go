package graylog_helper

import (
	"fmt"
	"github.com/zhangsq-ax/logs"
	"time"
)

var (
	logger = logs.NewJSONLogger("graylog-helper")
)

type ColumnSchema struct {
	Name       string `json:"name"`
	ColumnType string `json:"column_type"`
	Type       string `json:"type"`
	Field      string `json:"field"`
	Function   string `json:"function"`
}

type QueryResult struct {
	DataRows [][]any         `json:"datarows"`
	Schemas  []*ColumnSchema `json:"schema"`
}

func (qr *QueryResult) FieldName(index int) string {
	schema := qr.Schemas[index]
	if schema != nil {
		return schema.Field
	}
	return ""
}

func (qr *QueryResult) Fields() []string {
	result := make([]string, 0)
	for _, schema := range qr.Schemas {
		result = append(result, schema.Field)
	}
	return result
}

func (qr *QueryResult) Decode() []map[string]any {
	res, _ := qr.Transform(func(fields []string, row []any) (any, error) {
		item := map[string]any{}
		for idx, value := range row {
			field := fields[idx]
			if field == "timestamp" {
				t, err := time.Parse(time.RFC3339, value.(string))
				if err != nil {
					item[field] = value
				} else {
					item[field] = t.UnixMilli()
				}
			} else {
				item[field] = value
			}
		}
		return item, nil
	}, true)

	result := make([]map[string]any, 0)
	for _, item := range res {
		if i, ok := item.(map[string]any); ok {
			result = append(result, i)
		}
	}
	return result
}

func (qr *QueryResult) Transform(dataRowHandler func(colFields []string, dataRow []any) (any, error), ignoreErr ...bool) ([]any, error) {
	ignore := false
	if len(ignoreErr) > 0 {
		ignore = ignoreErr[0]
	}
	fields := qr.Fields()
	result := make([]any, 0)
	for _, row := range qr.DataRows {
		if len(fields) != len(row) {
			if ignore {
				continue
			} else {
				return nil, fmt.Errorf("fields length not match")
			}
		}
		item, err := dataRowHandler(fields, row)
		if err != nil {
			if ignore {
				continue
			} else {
				return nil, err
			}
		}
		result = append(result, item)
	}

	return result, nil
}
