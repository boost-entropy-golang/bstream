package bstream

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/streamingfast/dstore"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

type oneBlocksSource struct {
	*shutter.Shutter
	oneBlockFiles []*OneBlockFile
	downloader    OneBlockDownloaderFunc
	handler       Handler
	ctx           context.Context
	skipperFunc   func(idSuffix string) bool
	logger        *zap.Logger
}

type OneBlocksSourceOption func(*oneBlocksSource)

// OneBlocksSourceLogger configures the logger to use on the one block source
func OneBlocksSourceLogger(logger *zap.Logger) OneBlocksSourceOption {
	return func(s *oneBlocksSource) {
		s.logger = logger
	}
}

// OneBlocksSourceWithSkipperFunc allows a lookup function to prevent downloading the same file over and over
func OneBlocksSourceWithSkipperFunc(f func(string) bool) OneBlocksSourceOption {
	return func(s *oneBlocksSource) {
		s.skipperFunc = f
	}
}

func NewOneBlocksSource(
	lowestBlockNum uint64,
	store dstore.Store,
	handler Handler,
	options ...OneBlocksSourceOption,
) (*oneBlocksSource, error) {

	ctx := context.Background()
	listCtx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	sourceCtx, cancel := context.WithCancel(ctx)

	src := &oneBlocksSource{
		downloader: OneBlockDownloaderFromStore(store),
		handler:    handler,
		ctx:        sourceCtx,
		logger:     zlog,
		Shutter: shutter.New(
			shutter.RegisterOnTerminating(func(_ error) {
				cancel()
			}),
		),
	}
	for _, opt := range options {
		opt(src)
	}

	var err error
	src.oneBlockFiles, err = listOneBlocks(listCtx, lowestBlockNum, 0, store)
	if err != nil {
		src.logger.Warn("error listing oneblocks", zap.Uint64("lowest_block_num", lowestBlockNum), zap.Error(err))
		return nil, err
	}

	return src, nil
}

func (s *oneBlocksSource) Run() {
	s.Shutdown(s.run())
}

func (s *oneBlocksSource) run() error {
	s.logger.Debug("running one blocks source", zap.Int("count", len(s.oneBlockFiles)))

	for _, file := range s.oneBlockFiles {
		if s.skipperFunc != nil && s.skipperFunc(file.ID) {
			continue
		}

		data, err := s.downloader(s.ctx, file)
		if err != nil {
			return err
		}

		reader := bytes.NewReader(data)

		blockReader, err := NewDBinBlockReader(reader)
		if err != nil {
			return fmt.Errorf("unable to create block reader: %w", err)
		}
		blk, err := blockReader.Read()
		if err != nil && err != io.EOF {
			return fmt.Errorf("block reader failed: %w", err)
		}

		if err := s.handler.ProcessBlock(blk, nil); err != nil {
			return err
		}

	}
	zlog.Debug("one_blocks_source finish sending blocks", zap.Int("count", len(s.oneBlockFiles)))
	return nil
}

func listOneBlocks(ctx context.Context, from uint64, to uint64, store dstore.Store) (out []*OneBlockFile, err error) {
	fromStr := fmt.Sprintf("%010d", from)
	err = store.WalkFrom(ctx, "", fromStr, func(filename string) error {
		obf, err := NewOneBlockFile(filename)
		if err != nil {
			return nil
		}
		if to != 0 && obf.Num > to {
			return dstore.StopIteration
		}
		out = append(out, obf)
		return nil
	})
	return
}

// findOneBlockFile finds a single block file that matches the given check function. If no file is found, it returns
// nil, nil.
func findOneBlockFile(ctx context.Context, store dstore.Store, check func(file *OneBlockFile) bool) (out *OneBlockFile, err error) {
	err = store.Walk(ctx, "", func(filename string) error {
		obf, err := NewOneBlockFile(filename)
		if err != nil {
			return nil
		}

		if check(obf) {
			out = obf
			return dstore.StopIteration
		}

		return nil
	})

	return
}
