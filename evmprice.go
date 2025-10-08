package price

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

type EvmPriceBase struct {
	*PriceBase
	Eth    *ethclient.Client
	Quoter common.Address
}

type V2Params struct {
	Token1Price float64
	Token0Bal   float64
	Token1Bal   float64
	FeeRate     float64 // 0.003
}

type V3Params struct {
	Token1Price   float64
	Token0Addr    common.Address
	Token1Addr    common.Address
	Token0Decimal int
	Token1Decimal int
	Fee           uint32
}

func NewEvmPriceBase(pb *PriceBase, eth *ethclient.Client, quoter common.Address) *EvmPriceBase {
	return &EvmPriceBase{PriceBase: pb, Eth: eth, Quoter: quoter}
}

func (e *EvmPriceBase) v2SwapOut(x0, y0, delta, feeRate float64, isBuy bool) float64 {
	deltaEff := delta * (1 - feeRate)
	if isBuy {
		return x0 - x0*y0/(y0+deltaEff)
	}
	return y0 - x0*y0/(x0+deltaEff)
}

func (e *EvmPriceBase) CalcPriceV2(p V2Params, notionUSDT float64) (mid, buy, sell, token0Recv, token1Recv float64) {
	swapToken1 := notionUSDT / p.Token1Price
	token0Recv = e.v2SwapOut(p.Token0Bal, p.Token1Bal, swapToken1, p.FeeRate, true)
	buy = p.Token1Price * swapToken1 / token0Recv

	token1Recv = e.v2SwapOut(p.Token0Bal, p.Token1Bal, token0Recv, p.FeeRate, false)
	sell = p.Token1Price * token1Recv / token0Recv
	mid = (buy + sell) / 2.0
	return
}

func (e *EvmPriceBase) CalcPriceV3(ctx context.Context, p V3Params, notionUSDT float64) (mid, buy, sell, token0Recv, token1Recv float64, err error) {
	swapToken1 := notionUSDT / p.Token1Price

	amountIn := big.NewInt(0).Mul(big.NewInt(int64(swapToken1*1e9)), big.NewInt(0).Exp(big.NewInt(10), big.NewInt(int64(p.Token1Decimal-9)), nil))
	// quote token1 -> token0
	out0, err := e.quoteExactInputSingle(ctx, p.Token1Addr, p.Token0Addr, p.Fee, amountIn)
	if err != nil {
		return
	}
	token0Recv = toDecimal(out0, p.Token0Decimal)
	buy = p.Token1Price * swapToken1 / token0Recv

	// 再 quote token0 -> token1
	out1, err := e.quoteExactInputSingle(ctx, p.Token0Addr, p.Token1Addr, p.Fee, out0)
	if err != nil {
		return
	}
	token1Recv = toDecimal(out1, p.Token1Decimal)
	sell = p.Token1Price * token1Recv / token0Recv
	mid = (buy + sell) / 2.0
	return
}

func toDecimal(n *big.Int, dec int) float64 {
	den := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(dec)), nil)
	rat := new(big.Rat).SetFrac(n, den)
	f, _ := rat.Float64()
	return f
}

var quoterV2ABI = mustABI(`[{
  "inputs":[{"components":[
    {"internalType":"address","name":"tokenIn","type":"address"},
    {"internalType":"address","name":"tokenOut","type":"address"},
    {"internalType":"uint256","name":"amountIn","type":"uint256"},
    {"internalType":"uint24","name":"fee","type":"uint24"},
    {"internalType":"uint160","name":"sqrtPriceLimitX96","type":"uint160"}
  ],"internalType":"struct IQuoterV2.QuoteExactInputSingleParams","name":"params","type":"tuple"}],
  "name":"quoteExactInputSingle",
  "outputs":[
    {"internalType":"uint256","name":"amountOut","type":"uint256"},
    {"internalType":"uint160","name":"sqrtPriceX96After","type":"uint160"},
    {"internalType":"uint32","name":"initializedTicksCrossed","type":"uint32"},
    {"internalType":"uint256","name":"gasEstimate","type":"uint256"}
  ],
  "stateMutability":"nonpayable",
  "type":"function"
}]`)

func mustABI(s string) abi.ABI {
	a, err := abi.JSON(stringsNewReader(s))
	if err != nil {
		panic(err)
	}
	return a
}

func stringsNewReader(s string) *stringsReader {
	return &stringsReader{b: []byte(s)}
}

type stringsReader struct{ b []byte }

func (r *stringsReader) Read(p []byte) (int, error) {
	if len(r.b) == 0 {
		return 0, ioEOF{}
	}
	n := copy(p, r.b)
	r.b = r.b[n:]
	return n, nil
}

type ioEOF struct{}

func (ioEOF) Error() string { return "EOF" }

func (e *EvmPriceBase) quoteExactInputSingle(ctx context.Context, tokenIn, tokenOut common.Address, fee uint32, amountIn *big.Int) (*big.Int, error) {
	type params struct {
		TokenIn           common.Address
		TokenOut          common.Address
		AmountIn          *big.Int
		Fee               uint32
		SqrtPriceLimitX96 *big.Int
	}
	callData, err := quoterV2ABI.Pack("quoteExactInputSingle", params{
		TokenIn:           tokenIn,
		TokenOut:          tokenOut,
		AmountIn:          amountIn,
		Fee:               fee,
		SqrtPriceLimitX96: big.NewInt(0),
	})
	if err != nil {
		return nil, err
	}
	out, err := e.Eth.CallContract(ctx, ethereumCallMsg{To: e.Quoter, Data: callData}, nil)
	if err != nil {
		return nil, err
	}
	var decoded struct {
		AmountOut               *big.Int
		SqrtPriceX96After       *big.Int
		InitializedTicksCrossed uint32
		GasEstimate             *big.Int
	}
	if err := quoterV2ABI.UnpackIntoInterface(&decoded, "quoteExactInputSingle", out); err != nil {
		return nil, err
	}
	return decoded.AmountOut, nil
}

type ethereumCallMsg struct {
	To   common.Address
	Data []byte
}

func (m ethereumCallMsg) ToAddress() *common.Address { return &m.To }
func (m ethereumCallMsg) Gas() uint64                { return 0 }
func (m ethereumCallMsg) GasPrice() *big.Int         { return nil }
func (m ethereumCallMsg) Value() *big.Int            { return nil }
func (m ethereumCallMsg) DataBytes() []byte          { return m.Data }
