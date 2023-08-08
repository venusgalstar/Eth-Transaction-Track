# Indexer for Pulsechain especially erc20 transfer and swap on dex


Indexer is written in Python. It works as a service in background:

- Connects to Ethereum node (works well with Geth, Nethermind or other node, which provides http/ws/ipc API)
- Stores all erc20 transfer and swap info in SQLite database
- Provides data for API to get transactions by address with postgrest

## Stored information

Transfer table fields:

- 'blockNumber' block number on pulse chain ,
- 'fromAddress' EOA which occured erc20 transfer,
- 'toAddress' Receiver address,
- 'amount' Transfer Amount Value,
- 'name' Name of Token  which is transfered,
- 'symbol' Symbol of Token,
- 'address' Address of Token,
- 'decimal' Decimal value of Token,
- 'transactionHash' Hash of transaction which occured transfer

Swap table fields:

- 'blockNumber' block number on pulse chain ,
- 'fromAddress' EOA which occured erc20 transfer,
- 'pairAddress' Swapping Pair Contract Address,
- 'amountIn' input token amount,
- 'amountOut' output token amount,
- 'inTokenName' Name of InputToken ,
- 'inTokenSymbol' Symbol of InputToken,
- 'outTokenName' Name of OutputToken ,
- 'outTokenSymbol' Symbol of OutputToken,
- 'inTokenAddress' Address of InputToken,
- 'outTokenAddress' Address of OutputToken,
- 'inTokenDecimal' Decimal value of InputToken,
- 'outTokenDecimal' Decimal value of OutputToken,
- 'transactionHash' Hash of transaction which occured transfer

To improve syncing past transfer events, Indexer don't check every transaction but transfer event by dumping 100 blocks.


## Ethereum Indexer's API

# Ethereum Indexer Setup

## Prerequisites

- Ethereum node with RPC API enabled: Geth, Nethermind, etc.
- Python 3
- SQLite

## Installation

### Ethereum Node

Make sure your Ethereum node is installed and is fully synced. You can check its API and best block height with the command:

```
curl --data '{"method":"eth_blockNumber","params":[],"id":1,"jsonrpc":"2.0"}' -H "Content-Type: application/json" -X POST localhost:8545
```

### Python modules

Install Python 3. Install python modules:

```
apt install python3-pip
pip3 install web3
pip3 install psycopg2
```
### ERC20 transfer & swap indexer

`sync-transfer.py` is a script which makes erc20 transfer transaction logging
`sync-swap.py` is a script which makes erc20 swap transaction logging

- DB_NAME: SQL database name. Example: `transfer.db`.
- ETH_URL: Ethereum node url to reach the node. Supports websocket, http and ipc. See examples in `ethsync.py`.
- START_BLOCK: the first block to synchronize from. Default is 1.
- CONFIRMATIONS_BLOCK: the number of blocks to leave out of the synch from the end. I.e., last block is current `blockNumber - CONFIRMATIONS_BLOCK`. Default is 0.
- PERIOD: Number of seconds between to synchronization. Default is 20 sec.

Indexer can fetch transactions not from the beginning, but from special block number `START_BLOCK`. It will speed up indexing process and reduce database size.

At first start, Indexer will store transactions starting from the block you set. It will take a time. After that, it will check for new blocks every `PERIOD` seconds and update the index.

### Troubleshooting

To test connection from script, set a connection line in `test.py`, and run it. In case of success, it will print erc20 transfer and swap history of specific address
### Transaction API with Postgrest
  
### Make Indexer's API public

## Dockerized and docker compose

# API request examples