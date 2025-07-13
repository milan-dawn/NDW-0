
set -x

#nats stream add ACME_STREAM --subjects "ACME.Transactions.Confirms" --storage file --retention limits --max-bytes 250MB

nats stream add ACME_STREAM \
  --subjects "ACME.Transactions.Confirms" \
  --storage file \
  --retention limits \
  --max-bytes 262144000 \
  --discard old \
  --replicas 1 \
  --dupe-window 2m \
  --deny-delete \
  --allow-purge


