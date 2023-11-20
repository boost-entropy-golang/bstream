package bstream

import (
	"context"
	"strings"

	pbbstream "github.com/streamingfast/bstream/types/pb/sf/bstream/v1"

	"github.com/streamingfast/dstore"
	"go.uber.org/zap"
)

func FetchBlockFromOneBlockStore(
	ctx context.Context,
	num uint64,
	id string,
	store dstore.Store,
) (*pbbstream.Block, error) {
	if obfs, err := listOneBlocks(ctx, num, num+1, store); err == nil {
		canonicalID := NormalizeBlockID(id)
		for _, obf := range obfs {
			if strings.HasSuffix(canonicalID, obf.ID) {
				data, err := obf.Data(ctx, OneBlockDownloaderFromStore(store))
				if err != nil {
					return nil, err
				}
				return decodeOneblockfileData(data)
			}
		}
	}
	return nil, dstore.ErrNotFound
}

func FetchBlockFromMergedBlocksStore(
	ctx context.Context,
	num uint64,
	store dstore.Store,
) (*pbbstream.Block, error) {
	var foundBlock *pbbstream.Block
	h := HandlerFunc(func(blk *pbbstream.Block, _ interface{}) error {
		if blk.Number < num {
			return nil
		}
		if blk.Number > num {
			return dstore.StopIteration
		}
		foundBlock = blk
		return nil
	})
	fs := NewFileSource(
		store,
		num,
		h,
		zap.NewNop(),
		FileSourceWithStopBlock(num),
	)
	fs.Run()
	<-fs.Terminated()
	if foundBlock != nil {
		return foundBlock, nil
	}

	return nil, dstore.ErrNotFound
}
