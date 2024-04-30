package transform

import (
	"time"

	"github.com/streamingfast/bstream"
	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	"google.golang.org/protobuf/proto"
)

type Transform interface {
	String() string
}

type PreprocessTransform interface {
	Transform(readOnlyBlk *pbbstream.Block, in Input) (Output, error)
}

type Input interface {
	Type() string
	Obj() proto.Message // Most of the time a `pbsol.Block` or `pbeth.Block`
}

type Output proto.Message

type BlockMetadata struct {
	Ref       bstream.BlockRef
	ParentRef bstream.BlockRef
	LibNum    uint64
	Timestamp time.Time
}

const (
	NilObjectType string = "nil"
)

type NilObj struct{}

func NewNilObj() *NilObj             { return &NilObj{} }
func (n *NilObj) Type() string       { return NilObjectType }
func (n *NilObj) Obj() proto.Message { return nil }

type InputObj struct {
	_type string
	obj   proto.Message
	ttl   int
}

func (i *InputObj) Obj() proto.Message {
	return i.obj
}

func (i *InputObj) Type() string {
	return i._type
}
