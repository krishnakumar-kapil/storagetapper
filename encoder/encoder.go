// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package encoder

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/uber/storagetapper/types"
	msgpack "gopkg.in/vmihailenco/msgpack.v2"
)

//encoderConstructor initializes encoder plugin
type encoderConstructor func(service string, db string, table string) (Encoder, error)

//plugins insert their constructors into this map
var encoders map[string]encoderConstructor

//registerPlugin should be called from plugin's init
func registerPlugin(name string, init encoderConstructor) {
	if encoders == nil {
		encoders = make(map[string]encoderConstructor)
	}
	encoders[name] = init
}

//Encoder is unified interface to encode data from transit formats(row, common)
type Encoder interface {
	Row(tp int, row *[]interface{}, seqNo uint64) ([]byte, error)
	CommonFormat(cf *types.CommonFormatEvent) ([]byte, error)
	UpdateCodec() error
	Type() string
	Schema() *types.TableSchema
}

//Create is a factory which create encoder of given type for given service, db,
//table
func Create(encType string, s string, d string, t string) (Encoder, error) {
	init := encoders[strings.ToLower(encType)]

	enc, err := init(s, d, t)
	if err != nil {
		return nil, err
	}

	err = enc.UpdateCodec()

	return enc, err
}

//GetRowKey concatenates row primary key fields into string
//TODO: Should we encode into byte array instead?
func GetRowKey(s *types.TableSchema, row *[]interface{}) string {
	var key string
	for i := 0; i < len(s.Columns); i++ {
		if s.Columns[i].Key == "PRI" {
			if row == nil {
				k := fmt.Sprintf("%v", s.Columns[i].Name)
				key += fmt.Sprintf("%v%v", len(k), k)
			} else {
				k := fmt.Sprintf("%v", (*row)[i])
				key += fmt.Sprintf("%v%v", len(k), k)
			}
		}
	}
	return key
}

//GetCommonFormatKey concatenates common format key into string
func GetCommonFormatKey(cf *types.CommonFormatEvent) string {
	var key string
	for _, v := range cf.Key {
		s := fmt.Sprintf("%v", v)
		key += fmt.Sprintf("%v%v", len(s), s)
	}
	return key
}

// CommonFormatEncode encodes a CommonFormatEvent into the given
// encoding type
func CommonFormatEncode(c *types.CommonFormatEvent, encType string) ([]byte, error) {
	if encType == "json" {
		return json.Marshal(c)
	} else if encType == "msgpack" {
		return msgpack.Marshal(c)
	} else {
		return nil, fmt.Errorf("Use supported encoders")
	}
}

// DecodeToCommonFormat decodes a byte array into a
// CommonFormatEvent based on the given encoding
func DecodeToCommonFormat(b []byte, encType string) (*types.CommonFormatEvent, error) {
	res := &types.CommonFormatEvent{}
	var err error
	if encType == "json" {
		err = json.Unmarshal(b, res)
	} else if encType == "msgpack" {
		err = msgpack.Unmarshal(b, res)
	}
	return res, err
}
