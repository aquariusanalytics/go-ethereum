package custom

import (
	"context"
	"encoding/json"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/go-redis/redis/v8"
)

type TxOut struct {
	From      string             `json:"from"`
	To        string             `json:"to"`
	Type      uint8              `json:"type"`
	Nonce     uint64             `json:"nonce"`
	Gas       uint64             `json:"gas"`
	GasPrice  *big.Int           `json:"gasPrice"`
	GasFeeCap *big.Int           `json:"maxPriorityFeePerGas"`
	Value     *big.Int           `json:"value"`
	Hash      string             `json:"hash"`
	Input     hexutil.Bytes      `json:"input"`
	Size      common.StorageSize `json:"size"`
	ChainId   *big.Int           `json:"chainId"`
}
type RedisConfig struct {
	Auth string
	Addr string
}
type RedisDB struct {
	client *redis.Client
	ctx    context.Context
}

func NewRedisDb(cfg *RedisConfig) *RedisDB {
	rdb := &RedisDB{
		client: redis.NewClient(&redis.Options{
			Addr:     cfg.Addr,
			Password: cfg.Auth,
		}),
		ctx: context.Background(),
	}

	return rdb
}

// WriteCode writes the provided contract code database.
func (r *RedisDB) WriteCode(hash common.Hash, code []byte) {

	if res := r.client.XAdd(r.ctx, &redis.XAddArgs{
		Stream:       "block-code",
		MaxLen:       0,
		MaxLenApprox: 0,
		ID:           "",
		Values: map[string]interface{}{
			"hash": hash.String(),
			"data": code,
		},
	}); res.Err() != nil {

		log.Crit("Failed to store contract code", "err", res.Err())
	}
}

func (r *RedisDB) WriteAll(signer types.Signer, block *types.Block, receipts []*types.Receipt) {
	baseFee := block.BaseFee()
	bloom := block.Bloom()
	coinbase := block.Coinbase().String()
	difficulty := block.Difficulty()
	extra := block.Extra()
	gasLimit := block.GasLimit()
	gasUsed := block.GasUsed()
	hash := block.Hash().String()
	header := block.Header()
	mixDigest := block.MixDigest().String()
	nonce := block.Header().Nonce
	number := block.Number().String()
	parentHash := block.ParentHash().String()
	receiptHash := block.ReceiptHash().String()
	root := block.Root().String()
	size := block.Size()

	ts := time.Unix(int64(block.Time()), 0)
	transactions := block.Transactions()

	outStruct := struct {
		BaseFee      *big.Int           `json:"baseFee"`
		Bloom        types.Bloom        `json:"bloom"`
		Coinbase     string             `json:"coinbase"`
		Difficulty   *big.Int           `json:"difficulty"`
		Extra        []byte             `json:"extra"`
		GasLimit     uint64             `json:"gasLimit"`
		GasUsed      uint64             `json:"gasUsed"`
		Hash         string             `json:"hash"`
		Header       *types.Header      `json:"header"`
		MixDigest    string             `json:"mixDigest"`
		Nonce        types.BlockNonce   `json:"nonce"`
		Number       string             `json:"number"`
		ParentHash   string             `json:"parentHash"`
		ReceiptHash  string             `json:"receiptHash"`
		Root         string             `json:"root"`
		Size         common.StorageSize `json:"size"`
		Time         time.Time          `json:"time"`
		Transactions []TxOut            `json:"transactions"`
		Receipts     []*types.Receipt   `json:"receipts"`
	}{
		BaseFee:     baseFee,
		Bloom:       bloom,
		Coinbase:    coinbase,
		Difficulty:  difficulty,
		Extra:       extra,
		GasLimit:    gasLimit,
		GasUsed:     gasUsed,
		Hash:        hash,
		Header:      header,
		MixDigest:   mixDigest,
		Nonce:       nonce,
		Number:      number,
		ParentHash:  parentHash,
		ReceiptHash: receiptHash,
		Root:        root,
		Size:        size,
		Time:        ts,
		Receipts:    receipts,
	}

	if len(transactions) > 0 {
		outTransactions := []TxOut{}

		for _, txn := range transactions {
			from, err := signer.Sender(txn)

			if err != nil {
				log.Crit("error in txn", "txn", txn.Hash(), "err", err)
			}
			out := TxOut{
				From:      from.Hex(),
				Type:      txn.Type(),
				Nonce:     txn.Nonce(),
				Gas:       txn.Gas(),
				GasPrice:  txn.GasPrice(),
				GasFeeCap: txn.GasFeeCap(),
				Value:     txn.Value(),
				Hash:      txn.Hash().Hex(),
				Input:     (hexutil.Bytes)(txn.Data()),
				Size:      txn.Size(),
			}
			to := txn.To()
			if to != nil {
				out.To = to.Hex()
			}
			outTransactions = append(outTransactions, out)
		}
		outStruct.Transactions = outTransactions
	}

	bJson, err := json.Marshal(outStruct)
	if err != nil {
		log.Crit("error marshalling block", "err", err)
	}

	res := r.client.XAdd(r.ctx, &redis.XAddArgs{
		Stream:       "block-transactions",
		MaxLen:       0,
		MaxLenApprox: 0,
		ID:           "",
		Values: map[string]interface{}{
			"data": bJson,
		},
	})
	_, resErr := res.Result()
	if resErr != nil {
		log.Warn("redis error", "err", resErr)
	}

}
