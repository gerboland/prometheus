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
	"fmt"
	"io"
	"log"

	"github.com/xitongsys/parquet-go/reader"
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
	b      []byte
}

func (pr ParquetBytesReader) Open(name string) (source.ParquetFile, error) {
	return pr, nil
}

func (pr ParquetBytesReader) Close() error {
	return nil
}

func (pr ParquetBytesReader) Bytes() []byte {
	return pr.b
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
	return pr, nil
}

func NewParquetBytesReader(b []byte) ParquetBytesReader {
	return ParquetBytesReader{reader: bytes.NewReader(b), b: b}
}

type ParquetParser struct {
	r           ParquetBytesReader
	metric_name []byte   // assume a single one per parquet file -  read from the file metadata
	series      [][]byte // list of column/series names
	text        []byte   //??
	val         float64
	ts          int64
	// start          int
	// offsets        []int
	current_row    int
	current_series int
}

// NewParquetParser returns a new parser of the byte slice.
func NewParquetParser(b []byte) Parser {
	return &ParquetParser{r: NewParquetBytesReader(b), current_row: 0}
}

// Series returns the bytes of the series, the timestamp if set, and the value
// of the current sample.
func (p *ParquetParser) Series() ([]byte, *int64, float64) {
	return p.series[p.current_series], &p.ts, p.val
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
	return p.text
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

	pr, err := reader.NewParquetReader(p.r, nil, 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return EntryInvalid, err
	}

	num := int(pr.GetNumRows())

	if p.current_row >= num {
		return EntryInvalid, io.EOF
	}

	// Read file metadata
	for _, v := range pr.Footer.GetKeyValueMetadata() {
		fmt.Println(v.Key, *(v.Value))
		if v.Key == "metric_name" {
			p.metric_name = []byte(*(v.Value))
		}
	}

	// Read schema column names
	for _, schema := range pr.Footer.GetSchema() {
		fmt.Println(schema.Name)
		p.series = append(p.series, []byte(schema.Name))
	}

	res, err := pr.ReadByNumber(p.current_row)
	if err != nil {
		log.Println("Can't read", err)
		return EntryInvalid, err
	}

	log.Println("Read", res)

	p.current_row += 1

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
