from web3 import Web3
from web3.middleware import geth_poa_middleware

confirmationBlocks = "1"

nodeUrl = "/media/blockchain/execution/geth/geth.ipc"

# Connect to Ethereum node
if nodeUrl.startswith("http"):
    web3 = Web3(Web3.HTTPProvider(nodeUrl))  # "http://publicnode:8545"
elif nodeUrl.startswith("ws"):
    web3 = Web3(Web3.WebsocketProvider(nodeUrl))  # "ws://publicnode:8546"
else:
    web3 = Web3(Web3.IPCProvider(nodeUrl))  # "/home/geth/.ethereum/geth.ipc"

web3.middleware_onion.inject(geth_poa_middleware, layer=0)

pollingPeriod = 5

startBlock = 17230000

accounts = [
    "0x1daD947dD181fAa6c751ec14e2683e0A8fE2bf8c",
    "0xc17b1e62eAEf2805F664ed44972FCc7E6647474A",
    "0xCD76BD589A81E978014F237C5063c80335490Ae0",
    "0x788425510Bf225b75580804E2441339E17e1a6a5",
]

CSVAccount = "0xCD76BD589A81E978014F237C5063c80335490Ae0"

dexRouter = [
    "0x98bf93ebf5c380C0e6Ae8e192A7e2AE08edAcc02",
]
    
WPLS = "0xA1077a294dDE1B09bB078844df40758a5D0f9a27".lower()

TOKEN_ABI = '[{	"inputs": [],	"name": "decimals",	"outputs": [{"internalType": "uint8","name": "","type": "uint8"}	],	"stateMutability": "view",	"type": "function"},{	"inputs": [],	"name": "name",	"outputs": [{"internalType": "string","name": "","type": "string"}	],	"stateMutability": "view",	"type": "function"},{	"inputs": [],	"name": "symbol",	"outputs": [{"internalType": "string","name": "","type": "string"}	],	"stateMutability": "view",	"type": "function"},{	"inputs": [],	"name": "totalSupply",	"outputs": [{"internalType": "uint256","name": "","type": "uint256"}	],	"stateMutability": "view",	"type": "function"}]'
PAIR_ABI ='[{"type": "function","stateMutability": "view","outputs": [{"type": "uint8","name": "","internalType": "uint8"}],"name": "decimals","inputs": [],"constant": true},{"type": "function","stateMutability": "view","outputs": [{"type": "address","name": "","internalType": "address"}],"name": "factory","inputs": [],"constant": true},{"type": "function","stateMutability": "view","outputs": [{"type": "uint112","name": "_reserve0","internalType": "uint112"},{"type": "uint112","name": "_reserve1","internalType": "uint112"},{"type": "uint32","name": "_blockTimestampLast","internalType": "uint32"}],"name": "getReserves","inputs": [],"constant": true},{"type": "function","stateMutability": "view","outputs": [{"type": "string","name": "","internalType": "string"}],"name": "name","inputs": [],"constant": true},{"type": "function","stateMutability": "view","outputs": [{"type": "string","name": "","internalType": "string"}],"name": "symbol","inputs": [],"constant": true},{"type": "function","stateMutability": "view","outputs": [{"type": "address","name": "","internalType": "address"}],"name": "token0","inputs": [],"constant": true},{"type": "function","stateMutability": "view","outputs": [{"type": "address","name": "","internalType": "address"}],"name": "token1","inputs": [],"constant": true}]'

topics = []

for account in accounts:
    topics.append(account.lower().replace("0x", "0x000000000000000000000000"))

for idx in range(len(accounts)):
    accounts[idx] = accounts[idx].lower()

for idx in range(len(dexRouter)):
    dexRouter[idx] = dexRouter[idx].lower()
