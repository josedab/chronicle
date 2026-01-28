//go:build wasm

// Package main provides a WebAssembly interface to Chronicle.
// This allows Chronicle to be used in browser and edge JavaScript runtimes.
//
// Build with: GOOS=js GOARCH=wasm go build -o chronicle.wasm ./wasm
package main

import (
	"encoding/json"
	"syscall/js"
	"time"

	"github.com/chronicle-db/chronicle"
)

var db *chronicle.DB

func main() {
	js.Global().Set("chronicle", js.ValueOf(map[string]interface{}{
		"open":       js.FuncOf(jsOpen),
		"close":      js.FuncOf(jsClose),
		"write":      js.FuncOf(jsWrite),
		"writeBatch": js.FuncOf(jsWriteBatch),
		"query":      js.FuncOf(jsQuery),
		"execute":    js.FuncOf(jsExecute),
	}))

	// Keep the Go runtime alive
	select {}
}

// jsOpen opens a Chronicle database.
// chronicle.open(path: string, config?: object): Promise<void>
func jsOpen(this js.Value, args []js.Value) interface{} {
	if len(args) < 1 {
		return wrapError("path is required")
	}

	path := args[0].String()
	cfg := chronicle.DefaultConfig(path)

	if len(args) > 1 && args[1].Type() == js.TypeObject {
		configObj := args[1]
		if v := configObj.Get("bufferSize"); v.Type() == js.TypeNumber {
			cfg.BufferSize = v.Int()
		}
		if v := configObj.Get("partitionDuration"); v.Type() == js.TypeString {
			if d, err := time.ParseDuration(v.String()); err == nil {
				cfg.PartitionDuration = d
			}
		}
	}

	return promisify(func() (interface{}, error) {
		var err error
		db, err = chronicle.Open(path, cfg)
		return nil, err
	})
}

// jsClose closes the database.
// chronicle.close(): Promise<void>
func jsClose(this js.Value, args []js.Value) interface{} {
	return promisify(func() (interface{}, error) {
		if db == nil {
			return nil, nil
		}
		err := db.Close()
		db = nil
		return nil, err
	})
}

// jsWrite writes a single point.
// chronicle.write(metric: string, value: number, tags?: object): Promise<void>
func jsWrite(this js.Value, args []js.Value) interface{} {
	if len(args) < 2 {
		return wrapError("metric and value are required")
	}

	metric := args[0].String()
	value := args[1].Float()

	tags := make(map[string]string)
	if len(args) > 2 && args[2].Type() == js.TypeObject {
		tagsObj := args[2]
		keys := js.Global().Get("Object").Call("keys", tagsObj)
		for i := 0; i < keys.Length(); i++ {
			key := keys.Index(i).String()
			tags[key] = tagsObj.Get(key).String()
		}
	}

	return promisify(func() (interface{}, error) {
		if db == nil {
			return nil, jsError("database not open")
		}
		return nil, db.Write(chronicle.Point{
			Metric:    metric,
			Value:     value,
			Tags:      tags,
			Timestamp: time.Now().UnixNano(),
		})
	})
}

// jsWriteBatch writes multiple points.
// chronicle.writeBatch(points: Array<{metric: string, value: number, tags?: object, timestamp?: number}>): Promise<void>
func jsWriteBatch(this js.Value, args []js.Value) interface{} {
	if len(args) < 1 {
		return wrapError("points array is required")
	}

	pointsArr := args[0]
	if pointsArr.Type() != js.TypeObject {
		return wrapError("points must be an array")
	}

	return promisify(func() (interface{}, error) {
		if db == nil {
			return nil, jsError("database not open")
		}

		length := pointsArr.Get("length").Int()
		points := make([]chronicle.Point, length)

		for i := 0; i < length; i++ {
			p := pointsArr.Index(i)
			points[i] = chronicle.Point{
				Metric: p.Get("metric").String(),
				Value:  p.Get("value").Float(),
			}

			if ts := p.Get("timestamp"); ts.Type() == js.TypeNumber {
				points[i].Timestamp = int64(ts.Float())
			}

			if tagsObj := p.Get("tags"); tagsObj.Type() == js.TypeObject {
				points[i].Tags = make(map[string]string)
				keys := js.Global().Get("Object").Call("keys", tagsObj)
				for j := 0; j < keys.Length(); j++ {
					key := keys.Index(j).String()
					points[i].Tags[key] = tagsObj.Get(key).String()
				}
			}
		}

		return nil, db.WriteBatch(points)
	})
}

// jsQuery runs a SQL-like query.
// chronicle.query(sql: string): Promise<Array<{metric, tags, value, timestamp}>>
func jsQuery(this js.Value, args []js.Value) interface{} {
	if len(args) < 1 {
		return wrapError("query string is required")
	}

	sql := args[0].String()

	return promisify(func() (interface{}, error) {
		if db == nil {
			return nil, jsError("database not open")
		}

		parser := &chronicle.QueryParser{}
		q, err := parser.Parse(sql)
		if err != nil {
			return nil, err
		}

		result, err := db.Execute(q)
		if err != nil {
			return nil, err
		}

		return pointsToJS(result.Points), nil
	})
}

// jsExecute runs a query object.
// chronicle.execute(query: object): Promise<Array<{metric, tags, value, timestamp}>>
func jsExecute(this js.Value, args []js.Value) interface{} {
	if len(args) < 1 {
		return wrapError("query object is required")
	}

	queryObj := args[0]

	return promisify(func() (interface{}, error) {
		if db == nil {
			return nil, jsError("database not open")
		}

		q := &chronicle.Query{
			Metric: queryObj.Get("metric").String(),
		}

		if start := queryObj.Get("start"); start.Type() == js.TypeNumber {
			q.Start = int64(start.Float())
		}
		if end := queryObj.Get("end"); end.Type() == js.TypeNumber {
			q.End = int64(end.Float())
		}
		if limit := queryObj.Get("limit"); limit.Type() == js.TypeNumber {
			q.Limit = limit.Int()
		}

		if tags := queryObj.Get("tags"); tags.Type() == js.TypeObject {
			q.Tags = make(map[string]string)
			keys := js.Global().Get("Object").Call("keys", tags)
			for i := 0; i < keys.Length(); i++ {
				key := keys.Index(i).String()
				q.Tags[key] = tags.Get(key).String()
			}
		}

		result, err := db.Execute(q)
		if err != nil {
			return nil, err
		}

		return pointsToJS(result.Points), nil
	})
}

func pointsToJS(points []chronicle.Point) interface{} {
	arr := make([]interface{}, len(points))
	for i, p := range points {
		tags := make(map[string]interface{})
		for k, v := range p.Tags {
			tags[k] = v
		}
		arr[i] = map[string]interface{}{
			"metric":    p.Metric,
			"value":     p.Value,
			"timestamp": p.Timestamp,
			"tags":      tags,
		}
	}
	return arr
}

func promisify(fn func() (interface{}, error)) js.Value {
	handler := js.FuncOf(func(this js.Value, args []js.Value) interface{} {
		resolve := args[0]
		reject := args[1]

		go func() {
			result, err := fn()
			if err != nil {
				reject.Invoke(js.Global().Get("Error").New(err.Error()))
				return
			}
			resolve.Invoke(toJS(result))
		}()

		return nil
	})

	return js.Global().Get("Promise").New(handler)
}

func toJS(v interface{}) js.Value {
	if v == nil {
		return js.Undefined()
	}

	switch val := v.(type) {
	case []interface{}:
		arr := js.Global().Get("Array").New(len(val))
		for i, item := range val {
			arr.SetIndex(i, toJS(item))
		}
		return arr
	case map[string]interface{}:
		obj := js.Global().Get("Object").New()
		for k, item := range val {
			obj.Set(k, toJS(item))
		}
		return obj
	default:
		// For primitives, js.ValueOf handles them
		return js.ValueOf(v)
	}
}

func wrapError(msg string) js.Value {
	return js.Global().Get("Promise").Call("reject",
		js.Global().Get("Error").New(msg))
}

type jsErrorType string

func (e jsErrorType) Error() string {
	return string(e)
}

func jsError(msg string) error {
	return jsErrorType(msg)
}

// SerializePoints converts points to JSON for WASM transport.
func SerializePoints(points []chronicle.Point) ([]byte, error) {
	return json.Marshal(points)
}
