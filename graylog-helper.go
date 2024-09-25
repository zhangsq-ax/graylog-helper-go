package graylog_helper

import (
	"fmt"
	"github.com/go-resty/resty/v2"
	"github.com/json-iterator/go"
	resty_helper "github.com/zhangsq-ax/resty-helper-go"
	"go.uber.org/zap"
	"strconv"
)

type GraylogHelperOptions struct {
	BasicAuth *resty_helper.BasicAuth
	BaseUrl   string // http://127.0.0.1:9000
	Streams   []string
}

type GraylogHelper struct {
	opts   *GraylogHelperOptions
	client *resty.Client
}

func NewGraylogHelper(opts *GraylogHelperOptions) *GraylogHelper {
	return &GraylogHelper{
		opts: opts,
	}
}

func (helper *GraylogHelper) generateQueryTimeRange(from, to int64) any {
	// 如果是秒级时间戳，转换为毫秒
	from = standardTimestamp(from)
	to = standardTimestamp(to)

	return map[string]any{
		"type": "absolute",
		"from": from,
		"to":   to,
	}
}

func (helper *GraylogHelper) generateQueryFields(fields ...string) []string {
	baseFields := []string{"message", "timestamp"}
	if len(fields) > 0 {
		fields = append(fields, baseFields...)
	} else {
		fields = baseFields
	}
	return fields
}

// generateQueryBody 生成日志查询请求体
func (helper *GraylogHelper) generateQueryBody(from, to int64, query string, limit, offset int64, timeSequence bool, fields ...string) map[string]any {
	return helper.generateQueryBodyWithStreams(helper.opts.Streams, from, to, query, limit, offset, timeSequence, fields...)
}

func (helper *GraylogHelper) generateQueryBodyWithStreams(streams []string, from, to int64, query string, limit, offset int64, timeSequence bool, fields ...string) map[string]any {
	if limit == 0 {
		limit = 100
	}
	order := "desc"
	if timeSequence {
		order = "asc"
	}
	return map[string]any{
		"streams":    streams,
		"query":      query,
		"fields":     helper.generateQueryFields(fields...),
		"timerange":  helper.generateQueryTimeRange(from, to),
		"size":       limit,
		"from":       offset,
		"sort":       "timestamp",
		"sort_order": order,
	}
}

func (helper *GraylogHelper) generateAggregateGroupStage(groupFields []string) []map[string]any {
	groupStage := make([]map[string]any, 0)
	for _, field := range groupFields {
		groupStage = append(groupStage, map[string]any{
			"field": field,
		})
	}
	return groupStage
}

func (helper *GraylogHelper) generateAggregateMetricsStage(metrics [][2]string) []map[string]any {
	metricsStage := make([]map[string]any, 0)
	for _, m := range metrics {
		item := map[string]any{
			"function": m[0],
		}
		if m[1] != "" {
			item["field"] = m[1]
		}
		metricsStage = append(metricsStage, item)
	}
	return metricsStage
}

func (helper *GraylogHelper) generateAggregateBodyWithStreams(streams []string, from, to int64, query string, groupFields []string, metrics [][2]string) map[string]any {
	return map[string]any{
		"streams":   streams,
		"query":     query,
		"timerange": helper.generateQueryTimeRange(from, to),
		"group_by":  helper.generateAggregateGroupStage(groupFields),
		"metrics":   helper.generateAggregateMetricsStage(metrics),
	}
}

// generateAggregateBody 生成聚合查询请求体
func (helper *GraylogHelper) generateAggregateBody(from, to int64, query string, groupFields []string, metrics [][2]string) map[string]any {
	return helper.generateAggregateBodyWithStreams(helper.opts.Streams, from, to, query, groupFields, metrics)
}

func (helper *GraylogHelper) getRestyClient() *resty.Client {
	return resty_helper.GetRestyClient(&resty_helper.RestyClientOptions{
		BasicAuth: helper.opts.BasicAuth,
		BaseUrl:   helper.opts.BaseUrl,
		Headers: map[string]string{
			"X-Requested-By": "graylog-helper-go",
		},
	})
}

// GetLogsWithStreamsByProcess 在指定的 streams 中查询日志并进行自定义处理
func (helper *GraylogHelper) GetLogsWithStreamsByProcess(streams []string, from, to int64, query string, limit, offset int64, timeSequence bool, processor func(result *QueryResult) (any, error), fields ...string) (any, error) {
	reqBody := helper.generateQueryBodyWithStreams(streams, from, to, query, limit, offset, timeSequence, fields...)

	logger.Infow("query-graylog", zap.Reflect("body", reqBody), zap.String("url", "/api/search/messages"))
	res, err := resty_helper.RequestWithProcess(helper.getRestyClient(), "post", "/api/search/messages", func(body []byte) (any, error) {
		result := &QueryResult{}
		err := jsoniter.Unmarshal(body, result)
		if err != nil {
			return nil, err
		}
		return processor(result)
	}, reqBody)
	if err != nil {
		logger.Errorw("query-graylog-failed", zap.Error(err), zap.Reflect("body", reqBody), zap.String("url", "/api/search/messages"))
		return nil, err
	}
	return res.([]map[string]any), nil
}

// GetLogsByProcess 查询日志并进行自定义处理
func (helper *GraylogHelper) GetLogsByProcess(from, to int64, query string, limit, offset int64, timeSequence bool, processor func(result *QueryResult) (any, error), fields ...string) (any, error) {
	return helper.GetLogsWithStreamsByProcess(helper.opts.Streams, from, to, query, limit, offset, timeSequence, processor, fields...)
}

// GetLogsWithStreams 在指定的 streams 中查询日志
func (helper *GraylogHelper) GetLogsWithStreams(streams []string, from, to int64, query string, limit, offset int64, timeSequence bool) ([]map[string]any, error) {
	res, err := helper.GetLogsWithStreamsByProcess(streams, from, to, query, limit, offset, timeSequence, func(result *QueryResult) (any, error) {
		return result.Decode(), nil
	})
	if err != nil {
		return nil, err
	}
	return res.([]map[string]any), nil
}

// GetLogs 查询日志
func (helper *GraylogHelper) GetLogs(from, to int64, query string, limit, offset int64, timeSequence bool) ([]map[string]any, error) {
	return helper.GetLogsWithStreams(helper.opts.Streams, from, to, query, limit, offset, timeSequence)
}

// AggregateWithStreams 在指定的 streams 中进行聚合查询
func (helper *GraylogHelper) AggregateWithStreams(streams []string, from, to int64, query string, groupFields []string, metrics [][2]string) (*QueryResult, error) {
	reqBody := helper.generateAggregateBodyWithStreams(streams, from, to, query, groupFields, metrics)

	res, err := resty_helper.RequestWithProcess(helper.getRestyClient(), "post", "/api/search/aggregate", func(body []byte) (any, error) {
		result := &QueryResult{}
		err := jsoniter.Unmarshal(body, result)
		if err != nil {
			return nil, err
		}
		return result, nil
	}, reqBody)
	if err != nil {
		return nil, err
	}

	return res.(*QueryResult), nil
}

// Aggregate 进行聚合查询
func (helper *GraylogHelper) Aggregate(from, to int64, query string, groupFields []string, metrics [][2]string) (*QueryResult, error) {
	return helper.AggregateWithStreams(helper.opts.Streams, from, to, query, groupFields, metrics)
}

// CountLogsWithStreams 在指定的 streams 中统计日志总数
func (helper *GraylogHelper) CountLogsWithStreams(streams []string, from, to int64, query string) (int64, error) {
	result, err := helper.AggregateWithStreams(streams, from, to, query, []string{"count"}, [][2]string{{"count", ""}})
	if err != nil {
		return 0, err
	}
	if len(result.DataRows) > 0 {
		for idx, schema := range result.Schemas {
			if schema.Function == "count" {
				return int64(result.DataRows[0][idx].(float64)), nil
			}
		}
		return 0, fmt.Errorf("invalid result: %v", result.DataRows[0])
	}
	return 0, fmt.Errorf("no result")
}

// CountLogs 统计日志总数
func (helper *GraylogHelper) CountLogs(from, to int64, query string) (int64, error) {
	return helper.CountLogsWithStreams(helper.opts.Streams, from, to, query)
}

// standardTimestamp 将时间戳转换为毫秒级
func standardTimestamp(ts int64) int64 {
	if len(strconv.FormatInt(ts, 10)) == 10 {
		ts = ts * 1000
	}
	return ts
}
