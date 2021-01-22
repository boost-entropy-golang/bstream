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

package forkable

import (
	"context"
	"fmt"
	"time"

	"github.com/dfuse-io/bstream"
	pbblockmeta "github.com/dfuse-io/pbgo/dfuse/blockmeta/v1"
	"go.uber.org/zap"
)

type Forkable struct {
	logger        *zap.Logger
	handler       bstream.Handler
	forkDB        *ForkDB
	lastBlockSent *bstream.Block
	filterSteps   StepType

	ensureBlockFlows                   bstream.BlockRef
	ensureBlockFlowed                  bool
	ensureAllBlocksTriggerLongestChain bool
	includeInitialLIB                  bool

	irrChecker                      *irreversibilityChecker
	lastLIBNumFromStartOrIrrChecker uint64

	lastLongestChain []*node
}

type irreversibilityChecker struct {
	answer             chan bstream.BasicBlockRef
	blockIDClient      pbblockmeta.BlockIDClient
	delayBetweenChecks time.Duration
	lastCheckTime      time.Time
}

func (ic *irreversibilityChecker) CheckAsync(blk bstream.BlockRef, libNum uint64) {
	if blk.Num() < libNum+bstream.GetMaxNormalLIBDistance { // only kick in when lib gets too far
		return
	}
	if time.Since(ic.lastCheckTime) < ic.delayBetweenChecks {
		return
	}
	ic.lastCheckTime = time.Now()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), ic.delayBetweenChecks)
		defer cancel()

		resp, err := ic.blockIDClient.NumToID(ctx, &pbblockmeta.NumToIDRequest{
			BlockNum: blk.Num(),
		})
		if err != nil {
			zlog.Warn("forkable cannot fetch BlockIDServer (blockmeta) to resolve block ID", zap.Error(err), zap.Uint64("block_num", blk.Num()))
			return
		}
		if resp.Irreversible && resp.Id == blk.ID() {
			ic.answer <- bstream.NewBlockRef(blk.ID(), blk.Num())
		}
	}()
}

func (ic *irreversibilityChecker) Found() (out bstream.BasicBlockRef, found bool) {
	select {
	case out = <-ic.answer:
		found = true
		return
	default:
	}
	return out, false
}

type ForkableObject struct {
	Step StepType

	HandoffCount int

	// The three following fields are filled when handling multi-block steps, like when passing Irreversibile segments, the whole segment is represented in here.
	StepCount  int                          // Total number of steps in multi-block steps.
	StepIndex  int                          // Index for the current block
	StepBlocks []*bstream.PreprocessedBlock // You can decide to process them when StepCount == StepIndex +1 or when StepIndex == 0 only.

	ForkDB *ForkDB // ForkDB is a reference to the `Forkable`'s ForkDB instance. Provided you don't use it in goroutines, it is safe for use in `ProcessBlock` calls.

	// Object that was returned by PreprocessBlock(). Could be nil
	Obj interface{}
}

type ForkableBlock struct {
	Block     *bstream.Block
	Obj       interface{}
	SentAsNew bool
}

func New(h bstream.Handler, opts ...Option) *Forkable {
	f := &Forkable{
		filterSteps:      StepsAll,
		handler:          h,
		forkDB:           NewForkDB(),
		ensureBlockFlows: bstream.BlockRefEmpty,
		logger:           zlog,
	}

	for _, opt := range opts {
		opt(f)
	}

	// Done afterwards so forkdb can get configured forkable logger from options
	f.forkDB.logger = f.logger

	return f
}

func (p *Forkable) targetChainBlock(blk *bstream.Block) bstream.BlockRef {
	if p.ensureBlockFlows.ID() != "" && !p.ensureBlockFlowed {
		return p.ensureBlockFlows
	}

	return blk
}

func (p *Forkable) matchFilter(filter StepType) bool {
	return p.filterSteps&filter != 0
}

func (p *Forkable) computeNewLongestChain(ppBlk *ForkableBlock) ([]*node, error) {
	longestChain := p.lastLongestChain
	blk := ppBlk.Block

	canSkipRecompute := false
	if len(longestChain) != 0 &&
		blk.PreviousID() == longestChain[len(longestChain)-1].ID() && // optimize if adding block linearly
		p.forkDB.LIB().Num()+1 == longestChain[0].Num() { // do not optimize if the lib moved (should truncate up to lib)
		canSkipRecompute = true
	}

	if canSkipRecompute {
		longestChain = append(longestChain, &node{
			ref: bstream.NewBlockRef(blk.ID(), blk.Num()),
			obj: ppBlk,

			// NOTE: we don't want "Previous" because reversibleSegment does not give them
			prev: nil,
		})
	} else {
		var err error
		longestChain, err = p.forkDB.reversibleSegment(p.targetChainBlock(blk))
		if err != nil {
			return nil, fmt.Errorf("reversible segment: %w", err)
		}
	}
	p.lastLongestChain = longestChain
	return longestChain, nil

}

func (p *Forkable) ProcessBlock(blk *bstream.Block, obj interface{}) error {
	curLIB := p.forkDB.LIB()
	if blk.Num() < curLIB.Num() && p.lastBlockSent != nil {
		return nil
	}

	zlogBlk := p.logger.With(zap.Stringer("block", blk))

	// TODO: consider an `initialHeadBlockID`, triggerNewLongestChain also when the initialHeadBlockID's BlockNum == blk.Num()
	triggersNewLongestChain := p.triggersNewLongestChain(blk)

	if traceEnabled {
		zlogBlk.Debug("processing block", zap.Bool("new_longest_chain", triggersNewLongestChain))
	} else if blk.Number%600 == 0 {
		zlogBlk.Debug("processing block (1/600 sampling)", zap.Bool("new_longest_chain", triggersNewLongestChain))
	}

	ppBlk := &ForkableBlock{Block: blk, Obj: obj}

	if p.includeInitialLIB && p.lastBlockSent == nil && blk.ID() == curLIB.ID() {
		return p.processInitialInclusiveIrreversibleBlock(blk, obj)
	}

	var undos, redos []*ForkableBlock
	if p.matchFilter(StepUndo | StepRedo) {
		if triggersNewLongestChain && p.lastBlockSent != nil {
			var err error
			undos, redos, err = p.sentChainSwitchSegments(zlogBlk, p.lastBlockSent.AsRef(), blk.PreviousRef())
			if err != nil {
				return fmt.Errorf("sent chain switch segments: %w", err)
			}
		}
	}

	previousRef := bstream.NewBlockRef(blk.PreviousID(), blk.Num()-1)
	if exists := p.forkDB.AddLink(blk, previousRef, ppBlk); exists {
		return nil
	}

	if !p.forkDB.HasLIB() { // always skip processing until LIB is set
		p.forkDB.TrySetLIB(blk, previousRef, blk.LIBNum())
	}

	if !p.forkDB.HasLIB() {
		return nil
	}

	// All this code isn't reachable unless a LIB is set in the ForkDB
	if p.irrChecker != nil && p.lastLIBNumFromStartOrIrrChecker == 0 {
		p.lastLIBNumFromStartOrIrrChecker = p.forkDB.LIB().Num()
	}

	longestChain, err := p.computeNewLongestChain(ppBlk)
	if err != nil {
		return fmt.Errorf("compute new longest chain: %w", err)
	}

	if !triggersNewLongestChain || len(longestChain) == 0 {
		return nil
	}

	if traceEnabled {
		zlogBlk.Debug("got longest chain", zap.Int("chain_length", len(longestChain)), zap.Int("undos_length", len(undos)), zap.Int("redos_length", len(redos)))
	} else if blk.Number%600 == 0 {
		zlogBlk.Debug("got longest chain (1/600 sampling)", zap.Int("chain_length", len(longestChain)), zap.Int("undos_length", len(undos)), zap.Int("redos_length", len(redos)))
	}

	if p.matchFilter(StepUndo) {
		if err := p.processBlockIDs(blk.ID(), undos, StepUndo); err != nil {
			return err
		}
	}

	if p.matchFilter(StepRedo) {
		if err := p.processBlockIDs(blk.ID(), redos, StepRedo); err != nil {
			return err
		}
	}

	if err := p.processNewBlocks(longestChain); err != nil {
		return err
	}

	if p.lastBlockSent == nil {
		return nil
	}

	newLIBNum := p.lastBlockSent.LIBNum()
	newHeadBlock := p.lastBlockSent

	if newLIBNum < p.lastLIBNumFromStartOrIrrChecker {
		// we've been truncated before
		newLIBNum = p.lastLIBNumFromStartOrIrrChecker
	}

	libRef := p.forkDB.BlockInCurrentChain(newHeadBlock, newLIBNum)
	if libRef.ID() == "" {

		// this happens when the lib was set initially and we have not yet filled the lib->head buffer
		if traceEnabled {
			zlogBlk.Debug("missing links to reach lib_num", zap.Stringer("new_head_block", newHeadBlock), zap.Uint64("new_lib_num", newLIBNum))
		} else if newHeadBlock.Number%600 == 0 {
			zlogBlk.Debug("missing links to reach lib_num (1/600 sampling)", zap.Stringer("new_head_block", newHeadBlock), zap.Uint64("new_lib_num", newLIBNum))
		}

		return nil
	}

	if p.irrChecker != nil {
		p.irrChecker.CheckAsync(p.lastBlockSent, newLIBNum)
		if newLIB, found := p.irrChecker.Found(); found {
			if newLIB.Num() > libRef.Num() && newLIB.Num() < newHeadBlock.Num() {
				zlog.Info("irreversibilityChecker moving LIB from blockmeta reference because it is not advancing in chain", zap.Stringer("new_lib", newLIB), zap.Uint64("dposLIBNum", blk.LIBNum()))
				libRef = newLIB
				p.lastLIBNumFromStartOrIrrChecker = newLIB.Num()
			}
		}
	}

	// TODO: check preconditions here, and decide on whether we
	// continue or not early return would be perfect if there's no
	// `irreversibleSegment` or `stalledBlocks` to process.
	hasNew, irreversibleSegment, stalledBlocks, err := p.forkDB.checkNewIrreversibleSegment(libRef)
	if err != nil {
		return fmt.Errorf("check new irreversible segment: %w", err)
	}

	if !hasNew {
		return nil
	}

	if traceEnabled {
		zlogBlk.Debug("moving lib", zap.Stringer("lib", libRef))
	} else if libRef.Num()%600 == 0 {
		zlogBlk.Debug("moving lib (1/600)", zap.Stringer("lib", libRef))
	}

	p.forkDB.MoveLIB(libRef)

	if err := p.processIrreversibleSegment(irreversibleSegment); err != nil {
		return err
	}

	if err := p.processStalledSegment(stalledBlocks); err != nil {
		return err
	}

	return nil
}

func (p *Forkable) sentChainSwitchSegments(zlogger *zap.Logger, oldHeadRef, newHeadPreviousRef bstream.BlockRef) (undos []*ForkableBlock, redos []*ForkableBlock, err error) {
	if oldHeadRef == newHeadPreviousRef {
		return
	}

	undoIDs, redoIDs := p.forkDB.chainSwitchSegments(oldHeadRef, newHeadPreviousRef)

	undos, err = p.sentChainSegment(undoIDs, false)
	if err != nil {
		return nil, nil, fmt.Errorf("undo segment: %w", err)
	}

	redos, err = p.sentChainSegment(redoIDs, true)
	if err != nil {
		return nil, nil, fmt.Errorf("redo segment: %w", err)
	}

	return
}

func (p *Forkable) sentChainSegment(refs []bstream.BlockRef, doingRedos bool) (ppBlocks []*ForkableBlock, err error) {
	for _, ref := range refs {
		link := p.forkDB.nodeForRef(ref)
		if link == nil {
			return nil, fmt.Errorf("unknown node for ref %q", ref)
		}

		ppBlock := link.obj.(*ForkableBlock)
		if doingRedos && !ppBlock.SentAsNew {
			continue
		}

		ppBlocks = append(ppBlocks, ppBlock)
	}

	return ppBlocks, nil
}

func (p *Forkable) processBlockIDs(currentBlockID string, blocks []*ForkableBlock, step StepType) error {
	var objs []*bstream.PreprocessedBlock
	for _, block := range blocks {
		objs = append(objs, &bstream.PreprocessedBlock{
			Block: block.Block,
			Obj:   block.Obj,
		})
	}

	for idx, block := range blocks {
		err := p.handler.ProcessBlock(block.Block, &ForkableObject{
			Step:   step,
			ForkDB: p.forkDB,
			Obj:    block.Obj,

			StepIndex:  idx,
			StepCount:  len(blocks),
			StepBlocks: objs,
		})

		p.logger.Debug("sent block", zap.Stringer("block", block.Block), zap.Stringer("step_type", step))
		if err != nil {
			return fmt.Errorf("process block [%s] step=%q: %w", block.Block, step, err)
		}
	}
	return nil
}

func (p *Forkable) processNewBlocks(longestChain []*node) (err error) {
	for _, b := range longestChain {
		ppBlk := b.obj.(*ForkableBlock)
		if ppBlk.SentAsNew {
			// Sadly, there was a debug log line here, but it's so a pain to have when debug, since longuest
			// chain is iterated over and over again generating tons of this (now gone) log line. For this,
			// it was removed to make it easier to track what happen.
			continue
		}

		if p.matchFilter(StepNew) {
			err = p.handler.ProcessBlock(ppBlk.Block, &ForkableObject{
				Step:   StepNew,
				ForkDB: p.forkDB,
				Obj:    ppBlk.Obj,
			})
			if err != nil {
				return
			}
		}

		if traceEnabled {
			p.logger.Debug("sending block as new to consumer", zap.Stringer("block", ppBlk.Block))
		} else if ppBlk.Block.Number%600 == 0 {
			p.logger.Debug("sending block as new to consumer (1/600 sampling)", zap.Stringer("block", ppBlk.Block))
		}

		p.blockFlowed(ppBlk.Block)
		ppBlk.SentAsNew = true
		p.lastBlockSent = ppBlk.Block
	}

	return
}

func (p *Forkable) processInitialInclusiveIrreversibleBlock(blk *bstream.Block, obj interface{}) error {
	// Normally extracted from ForkDB, we create it here:
	singleBlock := &node{
		// Other fields not needed by `processNewBlocks`
		obj: &ForkableBlock{
			// WARN: this ForkDB doesn't have a reference to the current block, hopefully downstream doesn't need that (!)
			Block: blk,
			Obj:   obj,
		},
	}

	tinyChain := []*node{singleBlock}

	if err := p.processNewBlocks(tinyChain); err != nil {
		return err
	}

	if err := p.processIrreversibleSegment(tinyChain); err != nil {
		return err
	}

	return nil
}

func (p *Forkable) processIrreversibleSegment(irreversibleSegment []*node) error {
	if p.matchFilter(StepIrreversible) {
		var irrGroup []*bstream.PreprocessedBlock
		for _, irrBlock := range irreversibleSegment {
			preprocBlock := irrBlock.obj.(*ForkableBlock)
			irrGroup = append(irrGroup, &bstream.PreprocessedBlock{
				Block: preprocBlock.Block,
				Obj:   preprocBlock.Obj,
			})
		}

		for idx, irrBlock := range irreversibleSegment {
			preprocBlock := irrBlock.obj.(*ForkableBlock)

			objWrap := &ForkableObject{
				Step:   StepIrreversible,
				ForkDB: p.forkDB,
				Obj:    preprocBlock.Obj,

				StepIndex:  idx,
				StepCount:  len(irreversibleSegment),
				StepBlocks: irrGroup,
			}

			if err := p.handler.ProcessBlock(preprocBlock.Block, objWrap); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *Forkable) processStalledSegment(stalledBlocks []*node) error {
	if p.matchFilter(StepStalled) {
		var stalledGroup []*bstream.PreprocessedBlock
		for _, staleBlock := range stalledBlocks {
			preprocBlock := staleBlock.obj.(*ForkableBlock)
			stalledGroup = append(stalledGroup, &bstream.PreprocessedBlock{
				Block: preprocBlock.Block,
				Obj:   preprocBlock.Obj,
			})
		}

		for idx, staleBlock := range stalledBlocks {
			preprocBlock := staleBlock.obj.(*ForkableBlock)

			objWrap := &ForkableObject{
				Step:   StepStalled,
				ForkDB: p.forkDB,
				Obj:    preprocBlock.Obj,

				StepIndex:  idx,
				StepCount:  len(stalledBlocks),
				StepBlocks: stalledGroup,
			}

			if err := p.handler.ProcessBlock(preprocBlock.Block, objWrap); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *Forkable) blockFlowed(blockRef bstream.BlockRef) {
	if p.ensureBlockFlows.ID() == "" {
		return
	}

	if p.ensureBlockFlowed {
		return
	}

	if blockRef.ID() == p.ensureBlockFlows.ID() {
		p.ensureBlockFlowed = true
	}
}

func (p *Forkable) triggersNewLongestChain(blk *bstream.Block) bool {
	if p.ensureAllBlocksTriggerLongestChain {
		return true
	}

	if p.lastBlockSent == nil {
		return true
	}

	if blk.Num() > p.lastBlockSent.Num() {
		return true
	}

	return false
}
