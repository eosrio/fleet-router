#!/bin/bash
# Bootstrap script for fleet-router test chain
# Deploys eosio.boot + eosio.token, creates test accounts, and injects transactions
set -euo pipefail

NODEOS_URL="${NODEOS_URL:-http://nodeos-1:8888}"
WALLET_DIR="/tmp/wallet"
CONTRACTS_DIR="/opt/contracts"
NUM_ACCOUNTS=10
NUM_TRANSFERS="${NUM_TRANSFERS:-100}"

# Default eosio development key
EOSIO_PRIVKEY="5KQwrPbwdL6PhXujxW37FSSQZ1JiwsST4cqQzDeyXtP79zkvFD3"
EOSIO_PUBKEY="EOS6MRyAjQq8ud7hVNYcfnVPJqcVpscN5So8BhtHuGYqET5GDW5CV"

echo "=== Fleet Router Chain Bootstrap ==="
echo "Target: $NODEOS_URL"
echo "Transfers: $NUM_TRANSFERS"

# --- Step 0: Wait for nodeos to be ready ---
echo "[1/7] Waiting for nodeos..."
for i in $(seq 1 60); do
  if curl -sf "$NODEOS_URL/v1/chain/get_info" > /dev/null 2>&1; then
    echo "  nodeos is ready!"
    break
  fi
  if [ "$i" -eq 60 ]; then
    echo "ERROR: nodeos not reachable after 60s"
    exit 1
  fi
  sleep 1
done

# Show chain info
HEAD=$(curl -sf "$NODEOS_URL/v1/chain/get_info" | jq -r '.head_block_num')
echo "  Head block: $HEAD"

# --- Step 1: Create wallet ---
echo "[2/7] Creating wallet..."
mkdir -p "$WALLET_DIR"
# Start keosd in the background
keosd --wallet-dir "$WALLET_DIR" --http-server-address 127.0.0.1:8900 &
KEOSD_PID=$!
sleep 1

CLEOS="cleos -u $NODEOS_URL --wallet-url http://127.0.0.1:8900"

$CLEOS wallet create --to-console || true
$CLEOS wallet import --private-key "$EOSIO_PRIVKEY" || true

# --- Step 2: Activate PREACTIVATE_FEATURE ---
echo "[3/7] Activating PREACTIVATE_FEATURE..."
curl -sf "$NODEOS_URL/v1/producer/schedule_protocol_feature_activations" \
  -d '{"protocol_features_to_activate":["0ec7e080177b2c02b278d5088611686b49d739925a92d9bfcacd7fc6b74053bd"]}' \
  > /dev/null
sleep 1

# --- Step 3: Deploy eosio.boot ---
echo "[4/7] Deploying eosio.boot..."
$CLEOS set contract eosio "$CONTRACTS_DIR/eosio.boot" -p eosio@active
sleep 0.5

# --- Step 4: Activate remaining protocol features ---
echo "[5/7] Activating protocol features..."
# Core features needed for eosio.token and modern transactions
FEATURES=(
  "c3a6138c5061cf291310887c0b5c71fcaffeab90d5deb50d3b9e687cead45071"  # ONLY_BILL_FIRST_AUTHORIZER
  "4e7bf348da00a945489b2a681749eb56f5de00b900014e137ddae39f48f69d67"  # ACTION_RETURN_VALUE
  "f0af56d2c5a48d60a4a5b5c903edfb7db3a736a94ed589d0b797df33ff9d3e1d"  # GET_SENDER
  "2652f5f96006294109b3dd0bbde63693f55324af452b799ee137a81a905eed25"  # FORWARD_SETCODE
  "8ba52fe7a3956c5cd3a656a3174b931d3bb2abb45578befc59f283ecd816a405"  # ONLY_LINK_TO_EXISTING_PERMISSION
  "ad9e3d8f650687709fd68f4b90b41f7d825a365b02c23a636cef88ac2ac00c43"  # DISALLOW_EMPTY_PRODUCER_SCHEDULE
  "68dcaa34c0517d19666e6b33add67351d8c5f69e999ca1e37931bc410a297428"  # RESTRICT_ACTION_TO_SELF
  "e0fb64b1085cc5538970158d05a009c24e276fb94e1a0bf6a528b48fbc4ff526"  # FIX_LINKAUTH_RESTRICTION
  "ef43112c6543b88db2283a2e077278c315ae2c84719a8b25f25cc88565fbea99"  # REPLACE_DEFERRED
  "4a90c00d55454dc5b059055ca213579c6ea856967712a56017487886a4d4cc0f"  # NO_DUPLICATE_DEFERRED_ID
  "1a99a59d87e06e09ec5b028a9cbb7749b4a5ad8819004365d02dc4379a8b7241"  # RAM_RESTRICTIONS
  "299dcb6af692324b899b39f16d5a530a33062804e41f09dc97e9f156b4476707"  # WTMSIG_BLOCK_SIGNATURES
)

for feat in "${FEATURES[@]}"; do
  $CLEOS push action eosio activate "[\"$feat\"]" -p eosio@active 2>/dev/null || true
done
sleep 1

# --- Step 5: Deploy eosio.token ---
echo "[6/7] Deploying eosio.token..."
$CLEOS create account eosio eosio.token "$EOSIO_PUBKEY" "$EOSIO_PUBKEY"
$CLEOS set contract eosio.token "$CONTRACTS_DIR/eosio.token" -p eosio.token@active
sleep 0.5

# Create SYS token
$CLEOS push action eosio.token create '["eosio","1000000000.0000 SYS"]' -p eosio.token@active
$CLEOS push action eosio.token issue '["eosio","100000000.0000 SYS","initial"]' -p eosio@active
sleep 0.5

# --- Step 6: Create test accounts ---
ACCOUNTS=()
for i in $(seq 1 $NUM_ACCOUNTS); do
  ACCT=$(printf "testaccount%c" "$(echo $i | tr '0-9' 'a-j')")
  ACCOUNTS+=("$ACCT")
  $CLEOS create account eosio "$ACCT" "$EOSIO_PUBKEY" "$EOSIO_PUBKEY" 2>/dev/null || true
  # Fund each account
  $CLEOS push action eosio.token transfer \
    '["eosio","'"$ACCT"'","10000.0000 SYS","funding"]' \
    -p eosio@active 2>/dev/null || true
done

echo "  Created ${#ACCOUNTS[@]} accounts: ${ACCOUNTS[*]}"
sleep 1

# --- Step 7: Inject transactions ---
echo "[7/7] Injecting $NUM_TRANSFERS transfers..."
for i in $(seq 1 $NUM_TRANSFERS); do
  # Cycle through account pairs
  FROM_IDX=$(( (i - 1) % NUM_ACCOUNTS ))
  TO_IDX=$(( i % NUM_ACCOUNTS ))
  FROM=${ACCOUNTS[$FROM_IDX]}
  TO=${ACCOUNTS[$TO_IDX]}
  AMOUNT="$(( (RANDOM % 100) + 1 )).0000 SYS"
  MEMO="tx-$i"

  $CLEOS push action eosio.token transfer \
    '["'"$FROM"'","'"$TO"'","'"$AMOUNT"'","'"$MEMO"'"]' \
    -p "$FROM@active" 2>/dev/null || true

  # Print progress every 10 transfers
  if [ $((i % 10)) -eq 0 ]; then
    echo "  $i/$NUM_TRANSFERS transfers completed"
  fi
done

# Final chain state
HEAD=$(curl -sf "$NODEOS_URL/v1/chain/get_info" | jq -r '.head_block_num')
echo ""
echo "=== Bootstrap Complete ==="
echo "Head block: $HEAD"
echo "Accounts: ${#ACCOUNTS[@]}"
echo "Transactions injected: $NUM_TRANSFERS"

# Cleanup
kill $KEOSD_PID 2>/dev/null || true
