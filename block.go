package bstream

import (
	"fmt"
	"reflect"

	pbbstream "github.com/streamingfast/pbgo/sf/bstream/v1"
	"google.golang.org/protobuf/proto"
)

type Block = pbbstream.Block

func ToProtocol[B proto.Message](blk *pbbstream.Block) B {
	var b B
	value := reflect.New(reflect.TypeOf(b).Elem()).Interface().(B)
	if err := blk.Payload.UnmarshalTo(value); err != nil {
		panic(fmt.Errorf("unable to unmarshal block %s payload (kind: %s): %w", blk, blk.Payload.TypeUrl, err))
	}
	return value
}
