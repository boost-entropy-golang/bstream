package bstream

/**


Usage:

	a.tracker.AddGetter(bstream.StreamHeadTarget, dexer.Hub)
	a.tracker.AddGetter(bstream.StreamHeadTarget, timeline.ParallelGetter(relayerClient.GetterFactory(a.config.RelayerAddr), blockmeta.TrackerGetterFactory(a.config.BlockmetaAddr)))
	a.tracker.AddGetter(bstream.StreamHeadTarget, codec.GetterFactory(eosAPI))
	//a.tracker.AddGetter(dmesh.ArchiveHeadTarget, dmesh.TrackerGetterFactory(dmeshServer))
	// dmesh/interface.go
	// const ArchiveTailTarget = dtrack.Target("archive-tail")
	blockRef, err := a.tracker.Latest(dmesh.ArchiveTailTarget)

REVIEW THE ABOVE.. find a nice pattern ^^
*/

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"
)

//
// Types to support the Tracker
//

// BlockRefGetter is a function to retrieve a block ref from any system.
type BlockRefGetter func(context.Context) (BlockRef, error)

// StartBlockResolver should give you a start block number that will
// guarantee covering all necessary blocks to handle forks before the block
// that you want. This requires chain-specific implementations.
//
// A StartBlockResolver helps determine what is the lowest block that you
// have to fetch from your block source to ensure that you can handle forks
// for a given target start block
//
// ex: I want to start at block 1000 and I may have to start at block 700 if
// I don't have knowledge of which block 1000 is "irreversible")
//  * the DumbStartBlockResolver may simply tell you to start at block 500 and be done with it.
//  * a StartBlockResolver based on more data could tell you that you can start at block 1000
//    but that you need to set the irreversible ID to "00001000deadbeef" in your `forkable`
//    (InclusiveLIB) so that you don't start on a forked block that can't be resolved
//  * a StartBlockResolver based on a blocksource for EOSIO could fetch the "dposLIBNum"
//    of your targetStartBlock, and tell you to start at that block (ex: 727)
type StartBlockResolverFunc func(ctx context.Context, targetBlockNum uint64) (startBlockNum uint64, previousIrreversibleID string, err error)

var ErrTrackerBlockNotFound = errors.New("tracker block not found")
var ErrGetterUndefined = errors.New("tracker getter not defined for given target")

type Target string

const (
	FileSourceHeadTarget  = Target("filesource-head")
	LiveSourceHeadTarget  = Target("livesource-head")
	LiveSourceTailTarget  = Target("livesource-tail")
	NetworkHeadTarget     = Target("network-head")
	NetworkLIBTarget      = Target("network-lib")
	BlockStreamHeadTarget = Target("bstream-head")
	BlockStreamLIBTarget  = Target("bstream-lib")
	HubHeadTarget         = Target("hub-head")
	HubLIBTarget          = Target("hub-lib")
	// DatabaseHeadTarget    = Target("db-head")
	// DmeshTailTarget       = Target("dmesh-tail")
	// DmeshHeadTarget       = Target("dmesh-head")
)

// Tracker tracks the chain progress and block history. Allows many
// processes to take decisions on the state of different pieces being
// sync'd (live) or in catch-up mode.
type Tracker struct {
	getters   map[Target][]BlockRefGetter
	resolvers []StartBlockResolverFunc

	// Number of blocks between two targets before we consider the
	// first as "near" the second. Like a relayer stream being near
	// the tip of the network.
	nearBlocksCount int64
}

func NewTracker(nearBlocksCount uint64) *Tracker {
	return &Tracker{
		getters:         make(map[Target][]BlockRefGetter),
		nearBlocksCount: int64(nearBlocksCount),
	}
}

func (t *Tracker) AddGetter(target Target, f BlockRefGetter) {
	t.getters[target] = append(t.getters[target], f)
}

// AddResolver adds a start block resolver
func (t *Tracker) AddResolver(resolver StartBlockResolverFunc) {
	t.resolvers = append(t.resolvers, resolver)
}

func (t *Tracker) IsNear(ctx context.Context, from Target, to Target) (bool, error) {
	fromBlk, err := t.Get(ctx, from)
	if err != nil {
		return false, err
	}

	toBlk, err := t.Get(ctx, to)
	if err != nil {
		return false, err
	}

	if int64(toBlk.Num())-int64(fromBlk.Num()) < t.nearBlocksCount {
		return true, nil
	}

	return false, nil
}

func (t *Tracker) Get(ctx context.Context, target Target) (BlockRef, error) {
	getters := t.getters[target]
	if len(getters) == 0 {
		return nil, ErrGetterUndefined
	}
	var errs []string
	for _, f := range getters {
		ref, err := f(ctx)
		if err != nil {
			errs = append(errs, err.Error())
			continue
		}
		return ref, nil
	}

	return nil, errors.New("tracking fetch errors in order: " + strings.Join(errs, ", "))

	// Get from EOS API
	// Get from HeadInfo from any service ()
	// Get from SubscriptionHub  (use `tracker.AddGetter(bstream.StreamHeadTarget, hub.HeadTracker)`)
	// Get from blockmeta
	// Get from blkdb/trxdb's last written block
	// Get from fluxdb's latest written block
	// Get from command-line checkpoint?
}

// ResolveStartBlock gives you previous references to start processing
// in a fork-aware manner. forkDBInitRef is guaranteed to have a Num()
// if forkDBInitRef has an ID, you SetLIB() on the ForkDB with it.
func (t *Tracker) ResolveStartBlock(ctx context.Context, targetBlockNum uint64) (startBlockNum uint64, previousIrreversibleID string, err error) {
	if len(t.resolvers) == 0 {
		err = fmt.Errorf("no resolvers configured")
		return
	}
	var errs []string
	for _, f := range t.resolvers {
		startBlockNum, previousIrreversibleID, err = f(ctx, targetBlockNum)
		if err != nil {
			errs = append(errs, err.Error())
			continue
		}
		return
	}
	err = errors.New("resolving block reference: " + strings.Join(errs, ", "))
	return

	// Potential implementations:
	//
	// Fetch from blockmeta, ask for irreversible block at startBlock height.
	//   If blockmeta doesn't have it irreversible at that height, it could
	//   return the irreversible block prior to the requested startBlock
	// Fetch from blocks logs which includes blocks logs, inspect it
	//   and return the dposlib num we find in there.
	// Fetch from blkdb for all blocks at a given number,
	//   fetch whether any of those is irreversible
	// Fetch from blkdb for the given block number
	//   and return its dposlib num
	// Dmesh based LIB for the given number
	// Dummy resolver, merely returns blocks from the past.
	// Command-line source for a num + ID where to start.
}

func (t *Tracker) ResolveRelativeBlock(ctx context.Context, potentiallyNegativeBlockNum int64, target Target) (uint64, error) {
	if potentiallyNegativeBlockNum < 0 {
		blk, err := t.Get(ctx, target)
		if err != nil {
			return 0, err
		}
		return uint64(int64(blk.Num()) + potentiallyNegativeBlockNum), nil
	}
	return uint64(potentiallyNegativeBlockNum), nil
}

func HighestBlockRefGetter(getters ...BlockRefGetter) BlockRefGetter {
	type resp struct {
		ref BlockRef
		err error
	}

	return func(ctx context.Context) (BlockRef, error) {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		outChan := make(chan *resp)
		for _, getter := range getters {
			go func() {
				ref, err := getter(ctx)
				resp := &resp{
					ref: ref,
					err: err,
				}
				select {
				case outChan <- resp:
				case <-ctx.Done():
				}
			}()
		}

		var errs []string
		var highest BlockRef
		for cnt := 0; cnt < len(getters); cnt++ {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case resp := <-outChan:
				if resp.err != nil {
					errs = append(errs, resp.err.Error())
				} else {
					if highest == nil {
						highest = resp.ref
					} else {
						if resp.ref.Num() > highest.Num() {
							highest = resp.ref
						}
					}
				}
			}
		}

		if len(errs) == len(getters) {
			return nil, errors.New("all parallel getters failed: " + strings.Join(errs, ", "))
		}

		return highest, nil
	}
}

// ParallelStartResolver will call multiple resolvers to get the fastest answer.
func ParallelBlockResolver(resolvers ...StartBlockResolverFunc) StartBlockResolverFunc {
	type resp struct {
		startBlockNum          uint64
		previousIrreversibleID string
		err                    error
	}

	return func(ctx context.Context, targetBlockNum uint64) (startBlockNum uint64, previousIrreversibleID string, err error) {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		outChan := make(chan *resp)
		for _, resolver := range resolvers {
			go func() {
				startBlockNum, previousIrreversibleID, err := resolver(ctx, targetBlockNum)
				resp := &resp{
					startBlockNum:          startBlockNum,
					previousIrreversibleID: previousIrreversibleID,
					err:                    err,
				}
				select {
				case outChan <- resp:
				case <-ctx.Done():
				}
			}()
		}

		var errs []string
		for cnt := 0; cnt < len(resolvers); cnt++ {
			select {
			case <-ctx.Done():
				return 0, "", ctx.Err()
			case resp := <-outChan:
				if resp.err != nil {
					errs = append(errs, resp.err.Error())
				} else {
					return resp.startBlockNum, resp.previousIrreversibleID, nil
				}
			}
		}

		return 0, "", errors.New("all parallel resolvers failed: " + strings.Join(errs, ", "))
	}
}

func RetryableBlockResolver(attempts int, next StartBlockResolverFunc) StartBlockResolverFunc {
	return func(ctx context.Context, targetBlockNum uint64) (startBlockNum uint64, previousIrreversibleID string, err error) {
		var errs []string
		for attempt := 0; attempts == -1 || attempt <= attempts; attempt++ {
			if err = ctx.Err(); err != nil {
				errs = append(errs, err.Error())
				break
			}

			startBlockNum, previousIrreversibleID, err = next(ctx, targetBlockNum)
			if err != nil {
				zlog.Debug("got an error from a block resolver", zap.Error(err))
				errs = append(errs, err.Error())
				attempt++
				time.Sleep(time.Second)
				continue
			}

			zlog.Debug("resolved start block num", zap.Uint64("target_start_block_num", targetBlockNum), zap.Uint64("start_block_num", startBlockNum), zap.String("previous_irreversible_id", previousIrreversibleID))
		}
		err = fmt.Errorf("retryable resolver failed: %s", strings.Join(errs, ", "))
		return
	}
}

// func CachedStartBlockResolver(f StartBlockResolverFunc) StartBlockResolverFunc {
// 	var lastElements []uint64
// 	resolvedRefs := map[uint64]BlockRef{}

// 	return func(ctx context.Context, targetBlockNum uint64) {

// 	}
// }

var DumbStartBlockResolver = OffsetStartBlockResolver

// OffsetStartBlockResolver will help you start x blocks before your target start block
func OffsetStartBlockResolver(precedingBlocks uint64) StartBlockResolverFunc {
	return func(_ context.Context, targetBlockNum uint64) (uint64, string, error) {
		if targetBlockNum <= precedingBlocks {
			return 0, "", nil
		}
		return targetBlockNum - precedingBlocks, "", nil
	}
}

// type GetInfoTwoCallsCache struct {
// 	api *eos.API
// 	lastCall time.Time
// 	cachedLastCall *eos.InfoResp
// }

// func init() {
// 	cacher := &GetInfo..{}
// 	tracker.AddGetter(bstream.StreamHeadTarget, cacher.StreamHeadGetter)
// 	tracker.AddGetter(bstream.StreamLIBTarget, cacher.StreamLIBGetter)
// }

// func (c *GetInfoTwoCallsCache) StreamHeadGetter() BlockRefGetterFunc {
// 	if time.Since(lastCall) < 100 * time.Millisecond {
// 		return cachedLastCall
// 	}
// 	c.callTheShitAndCache()
// 	return
// }
