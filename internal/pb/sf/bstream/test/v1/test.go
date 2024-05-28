package pbtest

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (b *Block) ID() string {
	number, forkLine, _ := b.splitID()
	return fmt.Sprintf("%d%s", number, forkLine)
}

func (b *Block) Number() uint64 {
	number, _, _ := b.splitID()
	return number
}

func (b *Block) ParentID() string {
	number, _, parentLine := b.splitID()
	if number == 0 {
		return ""
	}

	return fmt.Sprintf("%d%s", number-1, parentLine)
}

func (b *Block) ParentNumber() uint64 {
	number, _, _ := b.splitID()
	if number == 0 {
		return 0
	}

	return number - 1
}

func (b *Block) LIBNum() uint64 {
	number, _, _ := b.splitID()
	if number == 0 {
		return 0
	}

	return number - 1
}

func (b *Block) Time() time.Time {
	return time.Unix(0, 0).UTC().Add(time.Minute * time.Duration(b.Number()))
}

var idRegex = regexp.MustCompile(`^(\d+)(.*?)(_.*)?$`)

func (b *Block) splitID() (number uint64, forkLine string, parentLine string) {
	matches := idRegex.FindStringSubmatch(b.Id)
	if len(matches) != 4 {
		panic(fmt.Errorf("invalid block ID %q received, must match regex %s", b.Id, idRegex.String()))
	}

	number, _ = strconv.ParseUint(matches[1], 10, 64)
	forkLine = matches[2]
	parentLine = forkLine
	if matches[3] != "" {
		parentLine = forkLine
		forkLine = strings.TrimPrefix(matches[3], "_")
	}

	return
}

func (b *Block) ToPbbstreamBlock() *pbbstream.Block {
	payload, err := anypb.New(b)
	if err != nil {
		panic(fmt.Errorf("unable to create anypb: %s", err))
	}

	return &pbbstream.Block{
		Id:        b.ID(),
		Number:    b.Number(),
		ParentNum: b.ParentNumber(),
		ParentId:  b.ParentID(),
		Timestamp: timestamppb.New(b.Time()),
		LibNum:    b.LIBNum(),
		Payload:   payload,
	}
}
