// Copyright 2022 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:generate go get -u modernc.org/golex
//go:generate golex -o=promlex.l.go promlex.l

package textparse

import (
	"bytes"
	"io"
	"log"
	"strings"
	"time"

	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/parquet"
	parquet_reader "github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
)

// Need to expose this interface
// type ParquetFile interface {
// 	io.Seeker
// 	io.Reader
// 	io.Writer
// 	io.Closer
// 	Open(name string) (ParquetFile, error)
// 	Create(name string) (ParquetFile, error)
// }
type ParquetBytesReader struct {
	reader *bytes.Reader
	buffer []byte
}

func (pr ParquetBytesReader) Open(name string) (source.ParquetFile, error) {
	// ParquetReader creates a separate ParquetFile for each column. Cannot re-use
	// this ParquetBytesReader as each file reader will seek to different location
	return ParquetBytesReader{reader: bytes.NewReader(pr.buffer)}, nil
}

func (pr ParquetBytesReader) Close() error {
	return nil
}

func (pr ParquetBytesReader) Read(p []byte) (n int, err error) {
	return pr.reader.Read(p)
}

func (pr ParquetBytesReader) Write(p []byte) (n int, err error) {
	// should be no-op
	return 0, nil
}

func (pr ParquetBytesReader) Seek(offset int64, whence int) (int64, error) {
	return pr.reader.Seek(offset, whence)
}

func (pr ParquetBytesReader) Create(name string) (source.ParquetFile, error) {
	// should never happen here
	return pr, nil
}

func NewParquetBytesReader(buffer []byte) ParquetBytesReader {
	return ParquetBytesReader{reader: bytes.NewReader(buffer), buffer: buffer}
}

type ParquetParser struct {
	reader            parquet_reader.ParquetReader
	metric_name       []byte   // assume a single one per parquet file -  read from the file metadata
	series            []string // list of column/series names
	val               float64
	ts                int64
	current_row       int
	total_row_count   int
	current_series    int
	index_column_name string
}

// NewParquetParser returns a new parser of the byte slice.
func NewParquetParser(b []byte) Parser {
	bytes_reader := NewParquetBytesReader(b)

	reader, err := parquet_reader.NewParquetReader(bytes_reader, nil, 4)
	if err != nil {
		log.Println("Creating parquet reader failed", err)
		// what now?
		return &ParquetParser{}
	}

	// Extract desired data from the file footer
	metric_name := []byte{}
	row_count := int(reader.GetNumRows())
	series := []string{}
	index_column_name := ""

	// Read file metadata
	for _, v := range reader.Footer.GetKeyValueMetadata() {
		// fmt.Println(v.Key, *(v.Value))
		switch v.Key {
		case "metric_name":
			metric_name = []byte(*(v.Value))
		case "tags":
			// grab
		}
	}

	// Read schema column names
	for _, schema := range reader.Footer.GetSchema() {
		name := schema.GetName()
		// fmt.Println(schema.Name, schema.Type, schema.GetTypeLength(), schema.GetLogicalType())
		switch t := schema.GetType(); t {
		case parquet.SchemaElement_Type_DEFAULT: // think this the schema itself
			continue
		case parquet.Type_DOUBLE:
			series = append(series, name)
		case parquet.Type_INT64:
			if strings.Contains(name, "__index_level_0__") { // should also check the LogicalType to check the timestamp units!
				index_column_name = name
			} else {
				log.Println("Warning: column '", name, "' has type int64 but is not an index - this is not permitted")
			}
		default:
			log.Println("Warning: column '", name, "' has unrecognised type: ", t)
		}
	}

	if index_column_name == "" {
		log.Println("Warning: no time index column found!")
		return &ParquetParser{}
	}

	log.Println("INDEX", index_column_name)
	for i, name := range series {
		log.Println("SERIES", i, string(name))
	}

	return &ParquetParser{reader: *reader, metric_name: metric_name, current_row: 0, current_series: 0, total_row_count: row_count, series: series, index_column_name: index_column_name}
}

// Series returns the bytes of the series, the timestamp if set, and the value
// of the current sample.
func (p *ParquetParser) Series() ([]byte, *int64, float64) {
	return []byte(p.series[p.current_series]), &p.ts, p.val
}

// Help returns the metric name and help text in the current entry.
// Must only be called after Next returned a help entry.
// The returned byte slices become invalid after the next call to Next.
func (p *ParquetParser) Help() ([]byte, []byte) {
	return p.metric_name, nil
}

// Type returns the metric name and type in the current entry.
// Must only be called after Next returned a type entry.
// The returned byte slices become invalid after the next call to Next.
func (p *ParquetParser) Type() ([]byte, MetricType) {
	return p.metric_name, MetricTypeGauge
}

// Unit returns the metric name and unit in the current entry.
// Must only be called after Next returned a unit entry.
// The returned byte slices become invalid after the next call to Next.
func (p *ParquetParser) Unit() ([]byte, []byte) {
	// The Prometheus format does not have units.
	return nil, nil
}

// Comment returns the text of the current comment.
// Must only be called after Next returned a comment entry.
// The returned byte slice becomes invalid after the next call to Next.
func (p *ParquetParser) Comment() []byte {
	return nil
}

// Metric writes the labels of the current sample into the passed labels.
// It returns the string from which the metric was parsed.
func (p *ParquetParser) Metric(l *labels.Labels) string {
	// Allocate the full immutable string immediately, so we just
	// have to create references on it below.
	s := string(p.series[p.current_series])

	// TODO: grab labels for each 'column'

	return s
}

// Exemplar writes the exemplar of the current sample into the passed
// exemplar. It returns if an exemplar exists.
func (p *ParquetParser) Exemplar(e *exemplar.Exemplar) bool {
	return false
}

// Next advances the parser to the next sample. It returns false if no
// more samples were read or an error occurred.
func (p *ParquetParser) Next() (Entry, error) {
	var err error

	if p.current_row >= p.total_row_count {
		return EntryInvalid, io.EOF
	}

	// DUMB! Much better to read row group in bulk to do row-by-row

	if p.current_series == 0 {
		// Get next time stamp
		res := make([]int64, 1)
		err = p.reader.ReadPartial(&res, common.ReformPathStr("schema.__index_level_0__")) // p.index_column_name)) <-WHY??
		if err != nil {
			log.Println("Read error", err)
		}
		log.Println("TIME", time.Unix(0, res[0]*int64(time.Microsecond)))
		p.ts = res[0]
	}

	// res := make([]float64, len(p.series)+1)
	// err = p.reader.Read(&v);

	// res, err := p.reader.ReadPartialByNumber(1, common.ReformPathStr("schema.cortex_writes_gateway_rps_5xx"))
	// num := int(p.reader.GetNumRows())

	// num_rows := int(p.reader.GetNumRows())
	// res, err := p.reader.ReadByNumber(num_rows)
	// if err != nil {
	// 	log.Println("Can't read", err)
	// 	return EntryInvalid, err
	// }

	// This successfully reads an entry from 1 column at a time.
	res := make([]float64, 1)
	err = p.reader.ReadPartial(&res, common.ReformPathStr("schema."+strings.ToLower(p.series[p.current_series])))

	if err != nil {
		log.Println("Read error", err)
	}

	if len(res) > 0 {
		p.val = res[0]
	} else {
		p.val = 0 //???  //TODO can we distinguish NaN??
	}

	log.Println("Read", p.val)
	// return EntryInvalid, io.EOF

	p.current_series += 1
	if p.current_series == len(p.series) {
		p.current_series = 0
		p.current_row += 1
	}

	// for i := 0; i < num; i++ {
	// 	stus := make([]Metric, 1)
	// 	if err = pr.Read(&stus); err != nil {
	// 		log.Println("Read error", err)
	// 	}
	// 	log.Println(stus)
	// }
	// pr.ReadStop()
	// f.Close()

	// // PREVIOUS!
	// p.start = p.l.i
	// p.offsets = p.offsets[:0]

	// switch t := p.nextToken(); t {
	// case tEOF:
	// 	return EntryInvalid, io.EOF
	// case tLinebreak:
	// 	// Allow full blank lines.
	// 	return p.Next()

	// case tHelp, tType:
	// 	switch t2 := p.nextToken(); t2 {
	// 	case tMName:
	// 		p.offsets = append(p.offsets, p.l.start, p.l.i)
	// 	default:
	// 		return EntryInvalid, parseError("expected metric name after "+t.String(), t2)
	// 	}
	// 	switch t2 := p.nextToken(); t2 {
	// 	case tText:
	// 		if len(p.l.buf()) > 1 {
	// 			p.text = p.l.buf()[1:]
	// 		} else {
	// 			p.text = []byte{}
	// 		}
	// 	default:
	// 		return EntryInvalid, fmt.Errorf("expected text in %s", t.String())
	// 	}
	// 	switch t {
	// 	case tType:
	// 		switch s := yoloString(p.text); s {
	// 		case "counter":
	// 			p.mtype = MetricTypeCounter
	// 		case "gauge":
	// 			p.mtype = MetricTypeGauge
	// 		case "histogram":
	// 			p.mtype = MetricTypeHistogram
	// 		case "summary":
	// 			p.mtype = MetricTypeSummary
	// 		case "untyped":
	// 			p.mtype = MetricTypeUnknown
	// 		default:
	// 			return EntryInvalid, errors.Errorf("invalid metric type %q", s)
	// 		}
	// 	case tHelp:
	// 		if !utf8.Valid(p.text) {
	// 			return EntryInvalid, errors.Errorf("help text is not a valid utf8 string")
	// 		}
	// 	}
	// 	if t := p.nextToken(); t != tLinebreak {
	// 		return EntryInvalid, parseError("linebreak expected after metadata", t)
	// 	}
	// 	switch t {
	// 	case tHelp:
	// 		return EntryHelp, nil
	// 	case tType:
	// 		return EntryType, nil
	// 	}
	// case tComment:
	// 	p.text = p.l.buf()
	// 	if t := p.nextToken(); t != tLinebreak {
	// 		return EntryInvalid, parseError("linebreak expected after comment", t)
	// 	}
	// 	return EntryComment, nil

	// case tMName:
	// 	p.offsets = append(p.offsets, p.l.i)
	// 	p.series = p.l.b[p.start:p.l.i]

	// 	t2 := p.nextToken()
	// 	if t2 == tBraceOpen {
	// 		if err := p.parseLVals(); err != nil {
	// 			return EntryInvalid, err
	// 		}
	// 		p.series = p.l.b[p.start:p.l.i]
	// 		t2 = p.nextToken()
	// 	}
	// 	if t2 != tValue {
	// 		return EntryInvalid, parseError("expected value after metric", t)
	// 	}
	// 	if p.val, err = parseFloat(yoloString(p.l.buf())); err != nil {
	// 		return EntryInvalid, err
	// 	}
	// 	// Ensure canonical NaN value.
	// 	if math.IsNaN(p.val) {
	// 		p.val = math.Float64frombits(value.NormalNaN)
	// 	}
	// 	p.hasTS = false
	// 	switch p.nextToken() {
	// 	case tLinebreak:
	// 		break
	// 	case tTimestamp:
	// 		p.hasTS = true
	// 		if p.ts, err = strconv.ParseInt(yoloString(p.l.buf()), 10, 64); err != nil {
	// 			return EntryInvalid, err
	// 		}
	// 		if t2 := p.nextToken(); t2 != tLinebreak {
	// 			return EntryInvalid, parseError("expected next entry after timestamp", t)
	// 		}
	// 	default:
	// 		return EntryInvalid, parseError("expected timestamp or new record", t)
	// 	}
	// 	return EntrySeries, nil

	// default:
	// 	err = errors.Errorf("%q is not a valid start token", t)
	// }
	return EntryInvalid, err
}
