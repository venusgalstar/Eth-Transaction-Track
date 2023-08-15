# Indexer for Pulsechain especially erc20 transfer and swap on dex

# Contact me on t.me/@Capdev22, flexible chat for project.

Indexer is written in Python. It works as a service in background:

- Connects to Ethereum node (works well with Geth, Nethermind or other node, which provides http/ws/ipc API)
- Stores all erc20 transfer and swap info in SQLite database
- Provides data for API to get transactions by address

## Stored information

Transfer table fields for erc20:

- 'blockNumber' block number on pulse chain ,
- 'fromAddress' EOA which occured erc20 transfer,
- 'toAddress' Receiver address,
- 'amount' Transfer Amount Value,
- 'address' Address of Token,
- 'transactionHash' Hash of transaction which occured transfer
- 'logIndex' Index number of this transfer in transaction

Swap table fields for erc20:

- 'blockNumber' block number on pulse chain ,
- 'fromAddress' EOA which occured erc20 transfer,
- 'pairAddress' Swapping Pair Contract Address,
- 'amountIn' input token amount,
- 'amountOut' output token amount,
- 'inTokenAddress' Address of InputToken,
- 'outTokenAddress' Address of OutputToken,
- 'transactionHash' Hash of transaction which occured transfer
- 'logIndex' Index number of this transfer in transaction

Transactions table files for PLS, Native currency:

- 'blockNumber' block number on pulse chain ,
- 'fromAddress' EOA which occured erc20 transfer,
- 'gas' Give Gas from the user for transaction,
- 'gasPrice' Give Gasprice from the user for transaction,
- 'toAddress' Destination address of transfer,
- 'value' Transfer amount of PLS,
- 'gasUsed' Used gas for this transaction,
- 'transactionHash' Hash of transaction which occured transfer

Token table for token info:

- 'address' Token address ,
- 'name' Token name,
- 'symbol' Token symbol,
- 'decimal' Token decimal,

CSV for final result:

- Tx-ID, TransactionHash
- Sender, EOA who makes transaction
- Receiver, Account which receives transaction
- Type, Buy, Sell, Withdraw, Income
- Buy Amount, 
- Buy Currency,
- Sell Amount, 
- Sell Currency,
- Fee, Gas fee used for transaction
- Fee Currency, PLS
- Exchange, PulseChain
- Trade-Group,
- Comment,
- Date, Transaction Date

To improve syncing past transfer events, Indexer don't check every transaction but transfer event by dumping 1000 blocks.


## Ethereum Indexer's API

# Ethereum Indexer Setup

## Prerequisites

- Ethereum node with RPC API enabled: Geth, Nethermind, etc.
- Python 3
- SQLite

## Installation


### ERC20 transfer & swap indexer

`sync-transfer.py` is a script which makes erc20 transfer transaction logging
`sync-swap.py` is a script which makes erc20 swap transaction logging
`sync-pls.py` is a script which makes PLS transfer transaction logging
`combine.py` is a script which combines database from above and token info logging
`makecsv.py` is a script which makes csv file

- DB_NAME: SQL database name. Example: `transfer.db`.
- ETH_URL: Ethereum node url to reach the node. Supports websocket, http and ipc. See examples in `sync-pls.py`.
- START_BLOCK: the first block to synchronize from. Default is 1.
- CONFIRMATIONS_BLOCK: the number of blocks to leave out of the synch from the end. I.e., last block is current `blockNumber - CONFIRMATIONS_BLOCK`. Default is 0.

Indexer can fetch transactions not from the beginning, but from special block number `START_BLOCK`. It will speed up indexing process and reduce database size.

At first start, Indexer will store transactions starting from the block you set. It will take a time. After that, it will check for new blocks every `PERIOD` seconds and update the index.

All config infos are in env.py
You can change startBlockNumber and accounts.

- ./run.sh

Finally, you will get account.db which contains all info from this script.

### Troubleshooting

To test connection from script, set a connection line in `test.py`, and run it. In case of success, it will print erc20 transfer and swap history of specific address
### Transaction API with Postgrest
  
### Make Indexer's API public

## Dockerized and docker compose

# API request examples

