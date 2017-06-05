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

import
// "github.com/tinylib/msgp"

"github.com/uber/storagetapper/types"

//go:generate msgp

func init() {
	registerPlugin("msgpack", initMsgPackEncoder)
}

// msgPackEncoder implements Encoder interface into message pack format.
// It inherits the methods from commonFormatEnocder.
type msgPackEncoder struct {
	c commonFormatEncoder
}

func initMsgPackEncoder(service string, db string, table string) (Encoder, error) {
	return &msgPackEncoder{c: commonFormatEncoder{Service: service, Db: db, Table: table}}, nil
}

//Schema returns table schema
func (e *msgPackEncoder) Schema() *types.TableSchema {
	return e.c.inSchema
}

//Row encodes row into CommonFormat
func (e *msgPackEncoder) Row(tp int, row *[]interface{}, seqno uint64) ([]byte, error) {
	cf := e.c.convertRowToCommonFormat(tp, row, e.c.inSchema, seqno, e.c.filter)
	return e.CommonFormatEncode(cf)
}

/*UpdateCodec refreshes the schema from state DB */
func (e *msgPackEncoder) UpdateCodec() error {
	return e.c.UpdateCodec()
}

//CommonFormat encodes common format event into byte array
func (e *msgPackEncoder) CommonFormat(cf *types.CommonFormatEvent) ([]byte, error) {
	if cf.Type == "schema" {
		err := e.c.UpdateCodec()
		if err != nil {
			return nil, err
		}
	}
	//FIXME: Assume that cf is in input schema format, so we need to filter it
	//to conform to output schema
	return e.CommonFormatEncode(cf)
}

//Type returns this encoder type
func (e *msgPackEncoder) Type() string {
	return "msgpack"
}

// CommonFormatEncode encodes CommonFormatEvent into byte array based on the message pack
// encoding system
// By overriding these 2 methods we get full functionality of commonFormatEncoder
// that implements MessagePack
func (e *msgPackEncoder) CommonFormatEncode(c *types.CommonFormatEvent) ([]byte, error) {
	return c.MarshalMsg(nil)
	// return msgpack.Marshal(c)
}

// CommonFormatDecode decodes CommonFormatEvent from byte array based on the msgpack encoding system
func (e *msgPackEncoder) CommonFormatDecode(b []byte) (*types.CommonFormatEvent, error) {
	res := &types.CommonFormatEvent{}
	// err := msgpack.Unmarshal(b, res)
	_, err := res.UnmarshalMsg(b)
	return res, err
	// return res, err
}
