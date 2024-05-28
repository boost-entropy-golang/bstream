// Copyright 2019 dfuse Platform Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hub

import (
	"context"
	"fmt"
	"github.com/streamingfast/dstore"
	"io"
	"strings"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/forkable"
	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

// ForkableHub gives you block Sources for blocks close to head
// it keeps reversible segment in a Forkable
// it keeps small final segment in a buffer
type ForkableHub struct {
	*shutter.Shutter

	forkable *forkable.Forkable

	keepFinalBlocks int

	optionalHandler   bstream.Handler
	subscribers       []*Subscription
	sourceChannelSize int

	liveSourceFactory bstream.SourceFactory
	oneBlocksStore    dstore.Store

	Ready chan struct{}
}

func NewForkableHub(liveSourceFactory bstream.SourceFactory, keepFinalBlocks int, oneBlocksStore dstore.Store, extraForkableOptions ...forkable.Option) *ForkableHub {
	hub := &ForkableHub{
		Shutter:           shutter.New(),
		liveSourceFactory: liveSourceFactory,
		keepFinalBlocks:   keepFinalBlocks,
		sourceChannelSize: 100, // number of blocks that can add up before the subscriber processes them
		oneBlocksStore:    oneBlocksStore,
		Ready:             make(chan struct{}),
	}

	hub.forkable = forkable.New(bstream.HandlerFunc(hub.broadcastBlock),
		forkable.HoldBlocksUntilLIB(),
		forkable.WithKeptFinalBlocks(keepFinalBlocks),
	)

	for _, opt := range extraForkableOptions {
		opt(hub.forkable)
	}

	hub.OnTerminating(func(err error) {
		for _, sub := range hub.subscribers {
			sub.Shutdown(err)
		}
	})

	return hub
}

func (h *ForkableHub) LowestBlockNum() uint64 {
	if h != nil && h.IsReady() {
		return h.forkable.LowestBlockNum()
	}
	return 0
}

func (h *ForkableHub) GetBlock(num uint64, id string) (out *pbbstream.Block) {
	if id == "" {
		return h.forkable.CanonicalBlockAt(num)
	}
	for _, blk := range h.forkable.AllBlocksAt(num) {
		if id == blk.Id {
			return blk
		}
	}
	return nil
}

func (h *ForkableHub) GetBlockByHash(id string) (out *pbbstream.Block) {
	return h.forkable.GetBlockByHash(id)
}

func (h *ForkableHub) HeadInfo() (headNum uint64, headID string, headTime time.Time, libNum uint64, err error) {
	if h != nil && h.IsReady() {
		headNum, headID, headTime, libNum, err = h.forkable.HeadInfo()
		zlog.Debug("forkable hub head info", zap.Uint64("head_num", headNum), zap.String("head_id", headID), zap.Time("head_time", headTime), zap.Uint64("lib_num", libNum))
		return
	}
	zlog.Debug("forkable hub not ready")
	err = fmt.Errorf("not ready")
	return
}

func (h *ForkableHub) HeadNum() uint64 {
	if h != nil && h.IsReady() {
		return h.forkable.HeadNum()
	}
	return 0
}
func (h *ForkableHub) MatchSuffix(req string) bool {
	ids := h.forkable.AllIDs()
	for _, id := range ids {
		if strings.HasSuffix(id, req) {
			return true
		}
	}
	return false
}

func (h *ForkableHub) IsReady() bool {
	select {
	case <-h.Ready:
		return true
	default:
		return false
	}
}

// subscribe must be called while hub is locked
func (h *ForkableHub) subscribe(handler bstream.Handler, initialBlocks []*bstream.PreprocessedBlock) *Subscription {
	chanSize := h.sourceChannelSize + len(initialBlocks)
	sub := NewSubscription(handler, chanSize)
	for _, ppblk := range initialBlocks {
		_ = sub.push(ppblk)
	}
	h.subscribers = append(h.subscribers, sub)
	return sub
}

// unsubscribe must be called while hub is locked
func (h *ForkableHub) unsubscribe(removeSub *Subscription) {
	var newSubscriber []*Subscription
	for _, sub := range h.subscribers {
		if sub != removeSub {
			newSubscriber = append(newSubscriber, sub)
		}
	}
	h.subscribers = newSubscriber
}

func (h *ForkableHub) SourceFromBlockNum(num uint64, handler bstream.Handler) (out bstream.Source) {
	if h == nil {
		return nil
	}

	err := h.forkable.CallWithBlocksFromNum(num, func(blocks []*bstream.PreprocessedBlock) { // Running callback func while forkable is locked
		out = h.subscribe(handler, blocks)
	}, false)
	if err != nil {
		zlog.Debug("error getting source_from_block_num", zap.Error(err))
		return nil
	}
	return
}

func (h *ForkableHub) SourceFromBlockNumWithForks(num uint64, handler bstream.Handler) (out bstream.Source) {
	if h == nil {
		return nil
	}

	err := h.forkable.CallWithBlocksFromNum(num, func(blocks []*bstream.PreprocessedBlock) { // Running callback func while forkable is locked
		out = h.subscribe(handler, blocks)
	}, true)
	if err != nil {
		zlog.Debug("error getting source_from_block_num", zap.Error(err))
		return nil
	}
	return
}

func (h *ForkableHub) SourceFromCursor(cursor *bstream.Cursor, handler bstream.Handler) (out bstream.Source) {
	if h == nil {
		return nil
	}

	err := h.forkable.CallWithBlocksFromCursor(cursor, func(blocks []*bstream.PreprocessedBlock) { // Running callback func while forkable is locked
		out = h.subscribe(handler, blocks)
	})
	if err != nil {
		zlog.Debug("error getting source_from_cursor", zap.Error(err))
		return nil
	}
	return
}

func (h *ForkableHub) SourceThroughCursor(startBlock uint64, cursor *bstream.Cursor, handler bstream.Handler) (out bstream.Source) {
	if h == nil {
		return nil
	}

	// cursor has already passed, ignoring it
	if cursor.Block.Num() < startBlock {
		return h.SourceFromBlockNum(startBlock, handler)
	}

	err := h.forkable.CallWithBlocksThroughCursor(startBlock, cursor, func(blocks []*bstream.PreprocessedBlock) { // Running callback func while forkable is locked
		out = h.subscribe(handler, blocks)
	})
	if err != nil {
		zlog.Debug("error getting source_from_cursor", zap.Error(err))
		return nil
	}
	return
}

func (h *ForkableHub) bootstrap() error {
	ctx := context.Background()

	sortedOneBlocksFiles, err := h.WalkOneBlocksStore(ctx)
	if err != nil {
		return fmt.Errorf("walking through one blocks files: %w", err)
	}

	if len(sortedOneBlocksFiles) == 0 {
		return fmt.Errorf("no one blocks found")
	}

	mostRecentOneBlock := sortedOneBlocksFiles[len(sortedOneBlocksFiles)-1]

	_, _, _, libNumAsRef, _, err := bstream.ParseFilename(mostRecentOneBlock)
	if err != nil {
		return fmt.Errorf("parsing filename: %w", err)
	}

	oneBlocksUpToLibRef := make([]*pbbstream.Block, 0)
	for _, filename := range sortedOneBlocksFiles {
		blockNum, suffixID, _, _, _, err := bstream.ParseFilename(filename)
		if err != nil {
			return fmt.Errorf("parsing filename: %w", err)
		}

		if blockNum < libNumAsRef {
			continue
		}

		if availableBlock := h.forkable.GetBlockByHashSuffix(suffixID); availableBlock != nil {
			//Block already known by the forkable
			continue
		}

		currentBlock, err := decodeOneBlockFromFilename(ctx, filename, h.oneBlocksStore)
		if err != nil {
			return fmt.Errorf("decoding %s from block store: %w", filename, err)
		}

		oneBlocksUpToLibRef = append(oneBlocksUpToLibRef, currentBlock)

		err = h.forkable.ProcessBlock(currentBlock, nil)
		if err != nil {
			return fmt.Errorf("processing block: %w", err)
		}
	}

	if !h.forkable.Linkable(oneBlocksUpToLibRef[len(oneBlocksUpToLibRef)-1]) {
		return fmt.Errorf("most recent one block is not linkable")
	}

	return nil
}

func (h *ForkableHub) Run() {
	liveSource := h.liveSourceFactory(h)
	liveSource.OnTerminating(h.reconnect)

	err := h.bootstrap()
	if err != nil {
		zlog.Warn("bootstrapping incomplete", zap.Error(err))
	} else {
		close(h.Ready)
	}

	liveSource.Run()

	zlog.Info("Hub is ready")

}
func (h *ForkableHub) ProcessBlock(blk *pbbstream.Block, obj interface{}) error {
	ctx := context.Background()

	lastKnownLib := h.forkable.LowestBlockNum()

	if !h.forkable.Linkable(blk) {
		sortedOneBlocksFiles, err := h.WalkOneBlocksStore(ctx)
		if err != nil {
			return fmt.Errorf("walking through one blocks files: %w", err)
		}

		if len(sortedOneBlocksFiles) == 0 {
			return fmt.Errorf("no one blocks found")
		}

		for _, filename := range sortedOneBlocksFiles {
			blockNum, suffixID, _, _, _, err := bstream.ParseFilename(filename)
			if err != nil {
				return fmt.Errorf("parsing filename: %w", err)
			}

			if blockNum < lastKnownLib {
				continue
			}
			if availableBlock := h.forkable.GetBlockByHashSuffix(suffixID); availableBlock != nil {
				//Block already known by the forkable
				continue
			}

			blockFromFile, err := decodeOneBlockFromFilename(ctx, filename, h.oneBlocksStore)
			if err != nil {
				return fmt.Errorf("decoding %s from block store: %w", filename, err)
			}

			if blockFromFile.Number == blk.LibNum {
				if !h.forkable.Linkable(blockFromFile) {
					return fmt.Errorf("cannot link block after reconnection, restart required")
				}
			}

			err = h.forkable.ProcessBlock(blockFromFile, obj)
			if err != nil {
				return fmt.Errorf("processing block %d: %w", blockFromFile.Number, err)
			}

		}
	}

	if !h.IsReady() {
		close(h.Ready)
	}

	return h.forkable.ProcessBlock(blk, obj)
}

func (h *ForkableHub) WalkOneBlocksStore(ctx context.Context) ([]string, error) {
	sortedOneBlocksFiles := make([]string, 0)
	err := h.oneBlocksStore.Walk(
		ctx,
		"",
		func(filename string) error {
			sortedOneBlocksFiles = append(sortedOneBlocksFiles, filename)
			return nil
		})
	return sortedOneBlocksFiles, err
}
func (h *ForkableHub) WalkOneBlocksStoreFrom(ctx context.Context, startingBlock uint64) ([]string, error) {
	startingPoint := fmt.Sprintf("%010d", startingBlock)
	sortedOneBlocksFiles := make([]string, 0)
	err := h.oneBlocksStore.WalkFrom(
		ctx,
		"",
		startingPoint,
		func(filename string) error {
			sortedOneBlocksFiles = append(sortedOneBlocksFiles, filename)
			return nil
		})
	return sortedOneBlocksFiles, err
}

func decodeOneBlockFromFilename(ctx context.Context, filename string, store dstore.Store) (*pbbstream.Block, error) {
	reader, err := store.OpenObject(ctx, filename)
	if err != nil {
		return nil, fmt.Errorf("fetching %s from block store: %w", filename, err)
	}

	defer reader.Close()

	readerData, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("reading %s from block store: %w", filename, err)
	}

	return bstream.DecodeOneblockfileData(readerData)
}

// Notes: that function is called by the forkable when a block is processed
func (h *ForkableHub) broadcastBlock(blk *pbbstream.Block, obj interface{}) error {
	zlog.Debug("process_block", zap.Stringer("blk", blk.AsRef()), zap.Any("obj", obj.(*forkable.ForkableObject).Step()))
	preprocBlock := &bstream.PreprocessedBlock{Block: blk, Obj: obj}

	subscribers := h.subscribers // we may remove some from the original slice during the loop

	for _, sub := range subscribers {
		err := sub.push(preprocBlock)
		if err != nil {
			h.unsubscribe(sub)
			sub.Shutdown(err)
		}

	}
	return nil
}

func (h *ForkableHub) reconnect(err error) {
	failFunc := func() {
		h.Shutdown(fmt.Errorf("cannot link new blocks to chain after a reconnection"))
	}

	rh := newReconnectionHandler(
		h.forkable,
		h.forkable.HeadNum,
		time.Minute,
		failFunc,
	)

	zlog.Info("reconnecting hub after disconnection. expecting to reconnect and get blocks linking to headnum within delay",
		zap.Duration("delay", rh.timeout),
		zap.Uint64("current_head_block_num", rh.previousHeadBlock),
		zap.Error(err))

	liveSource := h.liveSourceFactory(rh)
	liveSource.OnTerminating(func(err error) {
		if rh.success {
			h.reconnect(err)
			return
		}
		failFunc()
	})
	go liveSource.Run()
}

func substractAndRoundDownBlocks(blknum, sub uint64) uint64 {
	var out uint64
	if blknum < sub {
		out = 0
	} else {
		out = blknum - sub
	}
	out = out / 100 * 100

	if out < bstream.GetProtocolFirstStreamableBlock {
		return bstream.GetProtocolFirstStreamableBlock
	}

	return out
}

type reconnectionHandler struct {
	start             time.Time
	timeout           time.Duration
	previousHeadBlock uint64
	headBlockGetter   func() uint64
	handler           bstream.Handler
	success           bool
	onFailure         func()
}

func newReconnectionHandler(
	h bstream.Handler,
	headBlockGetter func() uint64,
	timeout time.Duration,
	onFailure func(),
) *reconnectionHandler {
	return &reconnectionHandler{
		handler:           h,
		headBlockGetter:   headBlockGetter,
		previousHeadBlock: headBlockGetter(),
		timeout:           timeout,
		onFailure:         onFailure,
	}
}

func (rh *reconnectionHandler) ProcessBlock(blk *pbbstream.Block, obj interface{}) error {
	if !rh.success {
		if rh.start.IsZero() {
			rh.start = time.Now()
		}
		if time.Since(rh.start) > rh.timeout {
			rh.onFailure()
			return fmt.Errorf("reconnection failed")
		}
		// head block has moved, the blocks are linkable
		if rh.headBlockGetter() > rh.previousHeadBlock {
			zlog.Info("reconnection successful")
			rh.success = true
		}
	}

	err := rh.handler.ProcessBlock(blk, obj)
	if err != nil {
		if rh.headBlockGetter() == rh.previousHeadBlock {
			rh.onFailure()
		}
		return err
	}
	return nil
}
