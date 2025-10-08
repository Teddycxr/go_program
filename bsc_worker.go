package price

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

type BscPriceWorker struct {
	Symbol    string
	PoolInfo  PoolInfo
	EVM       *EvmPriceBase
	Base      *PriceBase
	StopCh    chan struct{}
	errorHits int
}

type PoolInfo struct {
	PoolType   string  `json:"pool_type"`
	Fee        float64 `json:"fee"`
	Pool       string  `json:"pool"`
	HashCode   string  `json:"hash_code"`
	Decimal    int     `json:"decimal"`
	QuoteMint  string  `json:"quoteMint"`
	Token0Addr string  `json:"token0_addr,omitempty"`
}

var (
	addrWBNB  = common.HexToAddress("0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c")
	addrBUSDT = common.HexToAddress("0x55d398326f99059fF775485246999027B3197955")
	addrUSD1  = common.HexToAddress("0x8d0D000Ee44948FC98c9B98A4FA4921476f08B0d")
)

func NewBscPriceWorker(sym string, info PoolInfo, evm *EvmPriceBase, base *PriceBase) *BscPriceWorker {
	return &BscPriceWorker{
		Symbol:   sym,
		PoolInfo: info,
		EVM:      evm,
		Base:     base,
		StopCh:   make(chan struct{}),
	}
}

func (w *BscPriceWorker) Run(ctx context.Context) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()

	for {
		select {
		case <-w.StopCh:
			return
		case <-tick.C:
			if err := w.once(ctx); err != nil {
				w.Base.Log.Errorf("fetch %s error: %v", w.Symbol, err)
				w.errorHits++
			} else {
				w.errorHits = 0
			}

			if w.errorHits > 60 {
				w.Base.Log.Errorf("采集 %s 价格异常已达 %d", w.Symbol, w.errorHits)
				time.Sleep(5 * time.Second)
				w.errorHits = 0
			}
		}
	}
}

func (w *BscPriceWorker) once(ctx context.Context) error {
	var token1Price float64
	var outToken common.Address

	switch w.PoolInfo.PoolType {
	case "v2":
		p, err := w.Base.GetTokenPriceBinance(ctx, "BNB")
		if err != nil || p == 0 {
			return errors.New("BNB price unavailable")
		}
		token1Price = p
	case "v3":
		quote := common.HexToAddress(w.PoolInfo.QuoteMint)
		switch {
		case quote == addrBUSDT:
			token1Price = 1
			outToken = addrBUSDT
		case quote == addrUSD1:
			p, err := w.Base.GetTokenPriceBinance(ctx, "USD1")
			if err != nil || p == 0 {
				return errors.New("USD1 price unavailable")
			}
			token1Price = p
			outToken = addrUSD1
		default:
			p, err := w.Base.GetTokenPriceBinance(ctx, "BNB")
			if err != nil || p == 0 {
				return errors.New("BNB price unavailable")
			}
			token1Price = p
			outToken = addrWBNB
		}
	default:
		return errors.New("unknown pool_type")
	}

	switch w.PoolInfo.PoolType {
	case "v2":
		token0Bal, token1Bal, wethKey, err := w.getV2Reserves(ctx)
		if err != nil {
			return err
		}
		effective := token1Price
		rawKey := common.HexToAddress(w.PoolInfo.HashCode)
		if rawKey == addrBUSDT {
			effective = 1
		} else if rawKey == addrUSD1 {
			effective = 1
		} else {
			_ = wethKey
		}

		mid, buy, sell, token0Recv, token1Recv := w.EVM.CalcPriceV2(V2Params{
			Token1Price: effective,
			Token0Bal:   token0Bal,
			Token1Bal:   token1Bal,
			FeeRate:     w.PoolInfo.Fee,
		}, 1000)
		return w.Base.HandleSymbolPriceUpdate(ctx, w.Symbol, mid, buy, sell)

	case "v3":
		token0 := common.HexToAddress(w.PoolInfo.HashCode)
		mid, buy, sell, _, _, err := w.EVM.CalcPriceV3(ctx, V3Params{
			Token1Price:   token1Price,
			Token0Addr:    token0,
			Token1Addr:    outToken,
			Token0Decimal: w.PoolInfo.Decimal,
			Token1Decimal: 18,
			Fee:           uint32(w.PoolInfo.Fee),
		}, 1000)
		if err != nil {
			return err
		}
		return w.Base.HandleSymbolPriceUpdate(ctx, w.Symbol, mid, buy, sell)
	}
	return nil
}

func (w *BscPriceWorker) getV2Reserves(ctx context.Context) (token0Bal, token1Bal float64, wethKey common.Address, err error) {
	return 0, 0, common.Address{}, errorsNew("getV2Reserves not implemented")
}

func errorsNew(s string) error { return errors.New(s) }

func LoadPoolInfoFromJSON(s string) (PoolInfo, error) {
	var p PoolInfo
	err := json.Unmarshal([]byte(s), &p)
	return p, err
}
