# swarm-driver
This is an implementation of docker storage driver on the swarm

In order to run this we need to clone 3 repos:

1. This repo with the driver implementation
```
git clone https://github.com/uncloud-registry/swarm-driver.git
```
2. The distribution repo
```
git clone https://github.com/uncloud-registry/distribution.git
```
3. The modified swarm client
```
git clone https://github.com/aloknerurkar/bee.git
```

```
NOTE: The driver and distribution repos should be in the same directory as we use local dependency in go
```

### Step 1
Build the modified bee client. This has a small fix which allows deferred uploads of SOCs.
```
cd bee
make binary
```

### Step 2
Create the config for the bee node and start it. Example testnet config:
```
api-addr: ':1633'
p2p-addr: ':1634'
password: aaa4eabb0813df71afa45d
data-dir: ~/.bee-testnet
cors-allowed-origins:
  - '*'
verbosity: '5'
bootnode: ["/dnsaddr/testnet.ethswarm.org"]
full-node: 'false'
swap-enable: 'true'
mainnet: false
blockchain-rpc-endpoint: <block chain RPC endpoint for sepolia/gnosis>
storage-incentives-enable: 'false'
use-postage-snapshot: false
network-id: 10
swap-initial-deposit: '1000000000000000000'
```

- #### Fund the account
  Check [bee docs](https://docs.ethswarm.org/) on how to fund the node on testnet/mainnet.

- #### Buy a postage stamp
  The registry has to be initialized with a postage stamp. You can buy once you have funds and the node is synced with the network.
  ```
  curl -X POST http://localhost:1633/stamps/1000000000/24
  ```

### Step 3
Build the distribution docker image. The current dockerfile is copied during the build. So there is a `config-testnet.yaml` file in the distribution repo which will be copied.

Example:
```
version: 0.1
log:
  level: debug
  fields:
    service: registry
    environment: development
storage:
    delete:
      enabled: true
    cache:
        blobdescriptor: inmemory
    swarm:
        inmemory: false
        encrypt: false
        privateKey: <HEX encoded private key for publishing updates for the registry SOCs>
        cache: true
        host: "127.0.0.1" <IP of the machine where bee node is runninng>
        port: 1633 <PORT no where the API is running on bee>
        batchID: <POSTAGE STAMP ID>
    maintenance:
        uploadpurging:
            enabled: false
    tag:
      concurrencylimit: 8
http:
    addr: :5000
    debug:
        addr: :5001
        prometheus:
            enabled: true
            path: /metrics
    headers:
        X-Content-Type-Options: [nosniff]
health:
  storagedriver:
    enabled: true
    interval: 10s
    threshold: 3

```

- #### Generating private key for SOC owner account
  ```
  openssl ecparam -genkey -name secp256k1 -out private_key.pem
  ```

- #### Get HEX encoding
  ```
  openssl ec -in private_key.pem -outform DER -out private_key.der
  xxd -p private_key.der | tr -d '\n'
  ```

  Check out the [demo here](https://drive.google.com/file/d/1aefZZZQSrdDzsdl1SoI3TvO9LgjXAHKM/view?usp=drive_link)
