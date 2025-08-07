#!/bin/bash

GENESIS_HASH="0xdbacc01ae41b79388135ccd5d0ebe81eb0905260344256e6f4003bb8e75a91b5"
TELEMETRY_URL="wss://tc0.res.fm/feed"
OUTPUT_PATH=/tmp/res-likely-authors-csv

# Ensure CSV has header
[ ! -s ${OUTPUT_PATH} ] && echo '"timestamp","node_name","node_id","block_number","block_hash","propagation_time"' > ${OUTPUT_PATH}

while true; do
    echo "[$(date)] Starting telemetry monitoring..."

    echo "subscribe:${GENESIS_HASH}" | \
    websocat --no-close --buffer-size 524288 "${TELEMETRY_URL}" | \
    tee -a telemetry_raw.log | \
    jq -r '
      . as $array |

      # Collect node info
      (reduce (range(0; length; 2) | select($array[.] == 3)) as $idx (
        {};
        $array[$idx + 1] as $data |
        . + {($data[0] | tostring): {
          name: $data[1][0],
          node_id: $data[1][4]
        }}
      )) as $nodes |

      # Find low-propagation block imports
      range(0; length; 2) |
      select($array[.] == 6) |
      $array[. + 1] as $data |
      select($data[1][4] < 100) |
      [
        (now | floor),
        ($nodes[$data[0] | tostring].name // "unknown"),
        ($nodes[$data[0] | tostring].node_id // "unknown"),
        $data[1][0],
        $data[1][1],
        $data[1][4]
      ] |
      @csv
    ' | tee --append ${OUTPUT_PATH}

    echo "[$(date)] Connection lost. Reconnecting in 5 seconds..."
    sleep 5
done
