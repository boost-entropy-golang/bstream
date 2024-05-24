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
	"fmt"
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

	ready bool
	Ready chan struct{}

	liveSourceFactory                  bstream.SourceFactory
	oneBlocksSourceFactory             bstream.SourceFromNumFactory
	oneBlocksSourceFactoryWithSkipFunc bstream.SourceFromNumFactoryWithSkipFunc
}

func NewForkableHub(liveSourceFactory bstream.SourceFactory, oneBlocksSourceFactory interface{}, keepFinalBlocks int, extraForkableOptions ...forkable.Option) *ForkableHub {
	hub := &ForkableHub{
		Shutter:           shutter.New(),
		liveSourceFactory: liveSourceFactory,
		keepFinalBlocks:   keepFinalBlocks,
		sourceChannelSize: 100, // number of blocks that can add up before the subscriber processes them
		Ready:             make(chan struct{}),
	}

	switch fact := oneBlocksSourceFactory.(type) {
	case bstream.SourceFromNumFactoryWithSkipFunc:
		hub.oneBlocksSourceFactoryWithSkipFunc = fact
	case bstream.SourceFromNumFactory:
		hub.oneBlocksSourceFactory = fact
	default:
		panic("invalid oneBlocksSourceFactory interface")
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
	if h != nil && h.ready {
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
	if h != nil && h.ready {
		headNum, headID, headTime, libNum, err = h.forkable.HeadInfo()
		zlog.Debug("forkable hub head info", zap.Uint64("head_num", headNum), zap.String("head_id", headID), zap.Time("head_time", headTime), zap.Uint64("lib_num", libNum))
		return
	}
	zlog.Debug("forkable hub not ready")
	err = fmt.Errorf("not ready")
	return
}

func (h *ForkableHub) HeadNum() uint64 {
	if h != nil && h.ready {
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
	return h.ready
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
	//todo: list all the one block available
	//todo: load those block from the most recent
	//todo: process each block up to the lib using "h.forkable.ProcessBlock(blk, nil);"
	//Note: processing the newest block should set the lib on the forkable
	//todo: when the lib block is reached "h.forkable.Linkable(firstOneBlockProcessed)" should return true and the hub is ready!
	//todo: if not just return error and relayer  should shutdown

	return nil

	//zlog.Info("bootstrapping ForkableHub", zap.Stringer("blk", blk.AsRef()))
	//
	//// don't try bootstrapping from one-block-files if we are not at HEAD
	//if blk.Number < h.forkable.HeadNum() {
	//	zlog.Info("skip bootstrapping ForkableHub from one-block-files", zap.Stringer("blk", blk.AsRef()), zap.Uint64("forkdb_head_num", h.forkable.HeadNum()))
	//	return h.forkable.ProcessBlock(blk, nil)
	//}
	//
	//if !h.forkable.Linkable(blk) {
	//	startBlock := substractAndRoundDownBlocks(blk.LibNum, uint64(h.keepFinalBlocks))
	//	zlog.Info("bootstrapping on un-linkable block", zap.Uint64("start_block", startBlock), zap.Stringer("head_block", blk.AsRef()))
	//
	//	var oneBlocksSource bstream.Source
	//	if h.oneBlocksSourceFactoryWithSkipFunc != nil {
	//		skipFunc := func(idSuffix string) bool {
	//			return h.MatchSuffix(idSuffix)
	//		}
	//		oneBlocksSource = h.oneBlocksSourceFactoryWithSkipFunc(startBlock, h.forkable, skipFunc)
	//	} else {
	//		oneBlocksSource = h.oneBlocksSourceFactory(startBlock, h.forkable)
	//	}
	//
	//	if oneBlocksSource == nil {
	//		zlog.Debug("no oneBlocksSource from factory, not bootstrapping hub yet")
	//		return nil
	//	}
	//	zlog.Info("bootstrapping ForkableHub from one-block-files", zap.Uint64("start_block", startBlock), zap.Stringer("head_block", blk.AsRef()))
	//	go oneBlocksSource.Run()
	//	select {
	//	case <-oneBlocksSource.Terminating():
	//		break
	//	case <-h.Terminating():
	//		return h.Err()
	//	}
	//}
	//
	//if err := h.forkable.ProcessBlock(blk, nil); err != nil {
	//	return err
	//}
	//
	//if !h.forkable.Linkable(blk) {
	//	fdb_head := h.forkable.HeadNum()
	//	if blk.Number < fdb_head {
	//		zlog.Info("live block not linkable yet, will retry when we reach forkDB's HEAD", zap.Stringer("blk_from_live", blk.AsRef()), zap.Uint64("forkdb_head_num", fdb_head))
	//		return nil
	//	}
	//	zlog.Warn("cannot initialize forkDB from one-block-files (hole between live and one-block-files). Will retry on every incoming live block.", zap.Uint64("forkdb_head_block", fdb_head), zap.Stringer("blk_from_live", blk.AsRef()))
	//	return nil
	//}
	//zlog.Info("hub is now Ready")
	//
	//h.ready = true
	//close(h.Ready)
	//return nil
}

func (h *ForkableHub) Run() {
	liveSource := h.liveSourceFactory(h)
	liveSource.OnTerminating(h.reconnect)
	err := h.bootstrap()
	if err != nil {
		h.Shutdown(err)
		return
	}
	liveSource.Run()
}

func (h *ForkableHub) ProcessBlock(blk *pbbstream.Block, obj interface{}) error {
	if h.ready {
		//todo: before processing the current block, check if the block is linkable "if !h.forkable.Linkable(blk) {"
		//todo: if not, list all the one block available up to the lib of the current block
		//todo: check if the block is already known by the forkable "h.Forkable.GetBlockByHash(blk.Id)"
		//todo: if not, process the block "h.forkable.ProcessBlock(blk, obj)"

		//todo: @stepd. Think we may have a race here where the parent block is always uploaded after we receive the next block through the live source
		//todo: @stepd: That mean the end user will always be behind by one block. (This is a edge case, but still a bug)

		return h.forkable.ProcessBlock(blk, obj)
	}

	return nil
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
