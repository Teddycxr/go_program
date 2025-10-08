package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/redis/go-redis/v9"

	"yourmod/price"
)

type stdLogger struct{}

func (stdLogger) Infof(f string, a ...any)  { logPrintf("[INFO] "+f, a...) }
func (stdLogger) Errorf(f string, a ...any) { logPrintf("[ERR ] "+f, a...) }
func logPrintf(format string, a ...any) {
	_, _ = os.Stdout.WriteString(time.Now().Format("15:04:05 ") + fmtSprintf(format, a...) + "\n")
}
func fmtSprintf(f string, a ...any) string { return fmt.Sprintf(f, a...) }

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rpcURL := os.Getenv("BSC_RPC")
	if rpcURL == "" {
		panic("BSC_RPC empty")
	}
	quoterAddr := common.HexToAddress("0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997")

	eth, err := ethclient.DialContext(ctx, rpcURL)
	if err != nil {
		panic(err)
	}
	rdb := redis.NewClient(&redis.Options{
		Addr: os.Getenv("REDIS_ADDR"),
	})
	httpClient := &http.Client{Timeout: 8 * time.Second}

	var taker price.TakerLevelCfg
	if v := os.Getenv("TAKER_LEVEL_JSON"); v != "" {
		_ = json.Unmarshal([]byte(v), &taker)
	} else {
		taker.StopPriceRanges.Dex.Period = 5
	}

	log := stdLogger{}
	base := price.NewPriceBase("bsc", log, rdb, httpClient, taker)
	evm := price.NewEvmPriceBase(base, eth, quoterAddr)

	var symbols []struct {
		Symbol string         `json:"symbol"`
		Info   price.PoolInfo `json:"info"`
	}
	_ = json.Unmarshal([]byte(os.Getenv("BSC_SYMBOLS_JSON")), &symbols)

	wg := &sync.WaitGroup{}
	stopAll := make(chan struct{})

	for _, it := range symbols {
		wg.Add(1)
		w := price.NewBscPriceWorker(it.Symbol, it.Info, evm, base)
		go func() {
			defer wg.Done()
			w.Run(ctx)
		}()
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	close(stopAll)
	cancel()
	wg.Wait()
}
