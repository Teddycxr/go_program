package price

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type Logger interface {
	Infof(format string, args ...any)
	Errorf(format string, args ...any)
}

type PricePoint struct {
	T  int64   `json:"T"`
	TS int64   `json:"ts"`
	P  float64 `json:"p"`
	PB float64 `json:"pb"`
	PS float64 `json:"ps"`
}

type FiveMinCandle struct {
	T int64   `json:"t"`
	O float64 `json:"o"`
	H float64 `json:"h"`
	L float64 `json:"l"`
	C float64 `json:"c"`
}

type TakerLevelCfg struct {
	StopPriceRanges struct {
		Dex struct {
			Period int64 `json:"period"`
		} `json:"dex"`
	} `json:"stop_price_ranges"`
}

type PriceBase struct {
	Chain           string
	Log             Logger
	Redis           *redis.Client
	HTTP            *http.Client
	TakerLevel      TakerLevelCfg
	errorMu         sync.Mutex
	errorTracker    map[string]*symbolError
	klinePeriodMins int64
}

type symbolError struct {
	firstErrorAt time.Time
	errorCount   int
	alerted      bool
}

func NewPriceBase(chain string, log Logger, rdb *redis.Client, httpClient *http.Client, taker TakerLevelCfg) *PriceBase {
	p := &PriceBase{
		Chain:        chain,
		Log:          log,
		Redis:        rdb,
		HTTP:         httpClient,
		TakerLevel:   taker,
		errorTracker: map[string]*symbolError{},
	}
	if taker.StopPriceRanges.Dex.Period > 0 {
		p.klinePeriodMins = taker.StopPriceRanges.Dex.Period
	} else {
		p.klinePeriodMins = 5
	}
	return p
}

func (p *PriceBase) GetTokenPriceBinance(ctx context.Context, token string) (float64, error) {
	key := fmt.Sprintf("price_%s_usdt", token)
	now := time.Now().UnixMilli()

	if v, err := p.Redis.Get(ctx, key).Result(); err == nil {
		var point PricePoint
		if json.Unmarshal([]byte(v), &point) == nil && now-point.T < 10_000 {
			return point.P, nil
		}
	}

	url := fmt.Sprintf("https://api.binance.com/api/v3/depth?symbol=%sUSDT&limit=1", token)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	resp, err := p.HTTP.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	var depth struct {
		Bids [][]string `json:"bids"`
		Asks [][]string `json:"asks"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&depth); err != nil {
		return 0, err
	}
	if len(depth.Bids) == 0 || len(depth.Asks) == 0 {
		return 0, errors.New("empty depth")
	}
	bid := atof(depth.Bids[0][0])
	ask := atof(depth.Asks[0][0])
	mid := (bid + ask) / 2.0

	blob, _ := json.Marshal(PricePoint{P: mid, PB: bid, PS: ask, T: now, TS: now})
	_ = p.Redis.Set(ctx, key, string(blob), 0).Err()
	return mid, nil
}

func (p *PriceBase) HandleSymbolPriceUpdate(ctx context.Context, symbol string, mid, bid, ask float64) error {
	msg := fmt.Sprintf("%s: 买入价:%f, 卖出价:%f, 中间价:%f", symbol, bid, ask, mid)
	p.Log.Infof(msg)

	if mid == 0 {
		return errors.New("mid=0")
	}
	last, _ := p.GetLastPrice(ctx, symbol)
	now := time.Now().UnixMilli()
	point := PricePoint{T: now, TS: now, P: mid, PB: bid, PS: ask}

	if ask > 0 && bid > 0 {
		spread := abs(ask/bid - 1)
		if spread > 0.1 {
			p.Log.Errorf("%s 买卖价差过大: %.2f, %+v", symbol, spread, point)
			return errors.New("spread too large")
		}
	}

	if last != nil && last.P > 0 && abs(point.P/last.P-1) > 0.1 {
		p.Log.Errorf("%s 价格瞬时波动过大, last=%.6f, now=%.6f", symbol, last.P, point.P)
	}

	if err := p.storePrice(ctx, symbol, &point); err != nil {
		return err
	}
	p.update5mKline(ctx, symbol, point.P, point.T)
	return nil
}

func (p *PriceBase) GetLastPrice(ctx context.Context, symbol string) (*PricePoint, error) {
	key := fmt.Sprintf("price_%s", symbol)
	s, err := p.Redis.Get(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	var pt PricePoint
	if err := json.Unmarshal([]byte(s), &pt); err != nil {
		return nil, err
	}
	return &pt, nil
}

func (p *PriceBase) storePrice(ctx context.Context, symbol string, pt *PricePoint) error {
	key := fmt.Sprintf("price_%s", symbol)
	blob, _ := json.Marshal(pt)
	return p.Redis.Set(ctx, key, string(blob), 0).Err()
}

func (p *PriceBase) update5mKline(ctx context.Context, symbol string, price float64, tsMs int64) {
	key := fmt.Sprintf("kline_5m_%s", symbol)
	periodMs := p.klinePeriodMins * 60 * 1000
	bucket := (tsMs / periodMs) * periodMs

	var cur FiveMinCandle
	s, _ := p.Redis.Get(ctx, key).Result()
	if s == "" {
		cur = FiveMinCandle{T: bucket, O: price, H: price, L: price, C: price}
	} else {
		_ = json.Unmarshal([]byte(s), &cur)
		if cur.T != bucket {
			cur = FiveMinCandle{T: bucket, O: price, H: price, L: price, C: price}
		} else {
			if price > cur.H {
				cur.H = price
			}
			if price < cur.L {
				cur.L = price
			}
			cur.C = price
		}
	}
	blob, _ := json.Marshal(cur)
	_ = p.Redis.Set(ctx, key, string(blob), 0).Err()
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

func atof(s string) float64 {
	var f float64
	_ = json.Unmarshal([]byte(s), &f)
	return f
}
