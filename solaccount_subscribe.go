// 使用 Solana Yellowstone/Geyser 订阅池子余额变化并推送
package main

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand"
	"time"

	"generated/geyser"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

var _ = tls.VersionTLS13

type (
	CommitmentLevel int
)

const (
	CommitmentLevel_CONFIRMED CommitmentLevel = 1
)

type (
	SubscribeRequest struct {
		Commitment CommitmentLevel
		Filters    *SubscribeRequest_Filters
	}
	SubscribeRequest_Filters struct{ Accounts *AccountSubscribeFilter }
	AccountSubscribeFilter   struct {
		Account           []string
		AccountsDataSlice []*DataSlice
	}
	DataSlice               struct{ Offset, Length uint32 }
	SubscribeUpdate         struct{ Account *SubscribeUpdate_Account }
	SubscribeUpdate_Account struct{ Account *AccountInfo }
	AccountInfo             struct {
		Pubkey []byte
		Data   []byte
	}
)

type (
	GeyserClient interface {
		Subscribe(ctx context.Context, opts ...grpc.CallOption) (Geyser_SubscribeClient, error)
	}
	Geyser_SubscribeClient interface {
		Send(*SubscribeRequest) error
		Recv() (*SubscribeUpdate, error)
	}
)

type Side uint8

const (
	Base  Side = iota // token0
	Quote             // token1
)

type VaultRef struct {
	PoolID string
	Side   Side
}

type PairState struct {
	Base  *uint64
	Quote *uint64
}

type Snapshot struct {
	PoolID        string `json:"pool_id"`
	Token0Balance uint64 `json:"token0_balance"`
	Token1Balance uint64 `json:"token1_balance"`
}

type SymbolSource interface {
	ListYellowstoneSymbols(ctx context.Context) ([]SymbolTriple, error)
}

type SymbolTriple struct {
	Symbol     string // pool_id
	BaseVault  string
	QuoteVault string
}

type Publisher struct {
	log *slog.Logger
	ch  *amqp.Channel
	x   string
}

func NewPublisher(log *slog.Logger, amqpURL, exchange string) (*Publisher, error) {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	if err := ch.ExchangeDeclare(exchange, "direct", true, false, false, false, nil); err != nil {
		return nil, err
	}
	return &Publisher{log: log, ch: ch, x: exchange}, nil
}

func (p *Publisher) BindPoolQueue(poolID string) error {
	q, err := p.ch.QueueDeclare("pool_"+poolID, true, false, false, false, amqp.Table{
		"x-max-length": int32(1),
		"x-overflow":   "drop-head",
	})
	if err != nil {
		return err
	}
	return p.ch.QueueBind(q.Name, poolID, p.x, false, nil)
}

func (p *Publisher) Publish(ctx context.Context, s Snapshot) error {
	b, _ := json.Marshal(s)
	return p.ch.PublishWithContext(ctx, p.x, s.PoolID, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        b,
	})
}

type Streamer struct {
	log     *slog.Logger
	pub     *Publisher
	catalog SymbolSource

	endpoints []string
	tokens    []string
	curEP     int

	offset uint32
	length uint32

	vaultMap map[string]VaultRef
	pairs    map[string]PairState
}

func NewStreamer(log *slog.Logger, pub *Publisher, src SymbolSource) *Streamer {
	return &Streamer{
		log:      log,
		pub:      pub,
		catalog:  src,
		offset:   64,
		length:   8,
		vaultMap: make(map[string]VaultRef),
		pairs:    make(map[string]PairState),
	}
}

func (s *Streamer) ConfigureEndpoints(endpoints, tokens []string) {
	s.endpoints, s.tokens = endpoints, tokens
}

func (s *Streamer) Run(ctx context.Context) error {
	if err := s.loadAndBind(ctx); err != nil {
		return err
	}
	for _, batch := range chunk(s.sortedVaults(), 10) {
		go s.runStream(ctx, batch)
	}
	go s.hotReload(ctx, 60*time.Second)
	<-ctx.Done()
	return ctx.Err()
}

func (s *Streamer) loadAndBind(ctx context.Context) error {
	syms, err := s.catalog.ListYellowstoneSymbols(ctx)
	if err != nil {
		return err
	}
	seenPool := map[string]struct{}{}
	for _, it := range syms {
		if it.BaseVault == "" || it.QuoteVault == "" {
			continue
		}
		s.vaultMap[it.BaseVault] = VaultRef{PoolID: it.Symbol, Side: Base}
		s.vaultMap[it.QuoteVault] = VaultRef{PoolID: it.Symbol, Side: Quote}
		if _, ok := seenPool[it.Symbol]; ok {
			continue
		}
		if err := s.pub.BindPoolQueue(it.Symbol); err != nil {
			return err
		}
		seenPool[it.Symbol] = struct{}{}
	}
	s.log.Info("vault set initialized", slog.Int("vaults", len(s.vaultMap)), slog.Int("pools", len(seenPool)))
	return nil
}

func (s *Streamer) runStream(ctx context.Context, vaults []string) {
	backoff := time.Second
	for {
		addr, tok := s.nextEndpoint()
		cli, conn, err := s.dialClient(ctx, addr)
		if err != nil {
			s.sleepBackoff(&backoff)
			continue
		}

		md := metadata.Pairs("x-token", tok)
		cctx := metadata.NewOutgoingContext(ctx, md)
		stream, err := cli.Subscribe(cctx)
		if err != nil {
			s.log.Warn("subscribe failed", slog.String("addr", addr), slog.String("err", err.Error()))
			conn.Close()
			s.sleepBackoff(&backoff)
			continue
		}

		if err := stream.Send(s.buildSubReq(vaults)); err != nil {
			s.log.Warn("send subreq failed", slog.String("err", err.Error()))
			conn.Close()
			s.sleepBackoff(&backoff)
			continue
		}

		recvErr := s.recvLoop(stream)
		_ = conn.Close()
		s.log.Warn("stream closed", slog.String("addr", addr), slog.String("err", recvErr.Error()))
		s.sleepBackoff(&backoff)
	}
}

func (s *Streamer) dialClient(ctx context.Context, addr string) (GeyserClient, *grpc.ClientConn, error) {
	cc, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})), grpc.WithBlock(), grpc.FailOnNonTempDialError(true))
	if err != nil {
		return nil, nil, err
	}
	return nil, cc, errors.New("TODO: construct real geyser client")
}

func (s *Streamer) buildSubReq(vaults []string) *SubscribeRequest {
	return &SubscribeRequest{
		Commitment: CommitmentLevel_CONFIRMED,
		Filters: &SubscribeRequest_Filters{Accounts: &AccountSubscribeFilter{
			Account:           vaults,
			AccountsDataSlice: []*DataSlice{{Offset: s.offset, Length: s.length}},
		}},
	}
}

func (s *Streamer) recvLoop(stream Geyser_SubscribeClient) error {
	for {
		upd, err := stream.Recv()
		if err != nil {
			return err
		}
		s.onUpdate(upd)
	}
}

func (s *Streamer) onUpdate(upd *SubscribeUpdate) {
	acc := upd.Account
	if acc == nil || acc.Account == nil {
		return
	}

	pubkey := encodeBase58(acc.Account.Pubkey)
	info, ok := s.vaultMap[pubkey]
	if !ok {
		return
	}

	data := acc.Account.Data
	if uint32(len(data)) < s.length {
		return
	}
	amount := binary.LittleEndian.Uint64(data[:8])

	st := s.pairs[info.PoolID]
	if info.Side == Base {
		st.Base = &amount
	} else {
		st.Quote = &amount
	}

	if st.Base != nil && st.Quote != nil {
		sp := Snapshot{PoolID: info.PoolID, Token0Balance: *st.Base, Token1Balance: *st.Quote}
		st.Base, st.Quote = nil, nil
		s.pairs[info.PoolID] = st
		if err := s.pub.Publish(context.Background(), sp); err != nil {
			s.log.Warn("publish failed", slog.String("pool", sp.PoolID), slog.String("err", err.Error()))
		}
	} else {
		s.pairs[info.PoolID] = st
	}
}

func (s *Streamer) hotReload(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		syms, err := s.catalog.ListYellowstoneSymbols(ctx)
		if err != nil {
			s.log.Warn("hot reload failed", slog.String("err", err.Error()))
			continue
		}
		var newly []string
		for _, it := range syms {
			if it.BaseVault == "" || it.QuoteVault == "" {
				continue
			}
			if _, ok := s.vaultMap[it.BaseVault]; ok {
				continue
			}
			s.vaultMap[it.BaseVault] = VaultRef{PoolID: it.Symbol, Side: Base}
			s.vaultMap[it.QuoteVault] = VaultRef{PoolID: it.Symbol, Side: Quote}
			_ = s.pub.BindPoolQueue(it.Symbol)
			newly = append(newly, it.BaseVault, it.QuoteVault)
		}
		for _, batch := range chunk(newly, 10) {
			go s.runStream(ctx, batch)
		}
	}
}

func (s *Streamer) nextEndpoint() (addr, token string) {
	if len(s.endpoints) == 0 || len(s.tokens) == 0 {
		return "", ""
	}
	i := s.curEP
	s.curEP = (s.curEP + 1) % len(s.endpoints)
	return s.endpoints[i], s.tokens[i%len(s.tokens)]
}

func (s *Streamer) sleepBackoff(b *time.Duration) {
	jitter := time.Duration(rand.Int63n(int64(*b/2 + 1)))
	t := *b + jitter
	if t > 8*time.Second {
		t = 8 * time.Second
	}
	time.Sleep(t)
	if *b < 8*time.Second {
		*b *= 2
	}
}

func (s *Streamer) sortedVaults() []string {
	out := make([]string, 0, len(s.vaultMap))
	for k := range s.vaultMap {
		out = append(out, k)
	}
	return out
}

func chunk[T any](in []T, n int) [][]T {
	if n <= 0 || len(in) == 0 {
		return nil
	}
	var out [][]T
	for i := 0; i < len(in); i += n {
		j := i + n
		if j > len(in) {
			j = len(in)
		}
		out = append(out, in[i:j])
	}
	return out
}

func encodeBase58(b []byte) string { return string(b) }

func main() {
	log := slog.New(slog.NewTextHandler(nil, &slog.HandlerOptions{Level: slog.LevelInfo}))

	pub, err := NewPublisher(log, "amqp://guest:guest@localhost:5672/", "yellowstone")
	if err != nil {
		log.Error("mq init", slog.String("err", err.Error()))
		return
	}

	var src SymbolSource

	st := NewStreamer(log, pub, src)
	st.ConfigureEndpoints(
		[]string{"damp-purple-layer.solana-mainnet.quiknode.pro:10000"},
		[]string{"xxx"},
	)

	ctx := context.Background()
	if err := st.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("run", slog.String("err", err.Error()))
	}
}
