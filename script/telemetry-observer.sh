#!/bin/bash

GENESIS_HASH=0xdbacc01ae41b79388135ccd5d0ebe81eb0905260344256e6f4003bb8e75a91b5
TELEMETRY_URL=wss://tc0.res.fm/feed
OUTPUT_PATH=./data/res-likely-authors.csv
NODES_FILE=./data/telemetry-nodes.json
BLOCKS_FILE=./data/telemetry-blocks.json

# Initialize files
[ ! -f ${NODES_FILE} ] && echo '{}' > ${NODES_FILE}
[ ! -f ${BLOCKS_FILE} ] && echo '{}' > ${BLOCKS_FILE}

# Ensure CSV has header
[ ! -s ${OUTPUT_PATH} ] && echo '"timestamp","node_name","node_id","block_number","block_hash","propagation_time"' > ${OUTPUT_PATH}

while true; do
    echo "[$(date)] Starting telemetry monitoring..."
    echo "[DEBUG] Connecting to ${TELEMETRY_URL} with genesis hash ${GENESIS_HASH}" >&2

    echo "subscribe:${GENESIS_HASH}" | \
    websocat --no-close --buffer-size 524288 "${TELEMETRY_URL}" 2>&1 | \
    tee -a telemetry_raw.log | \
    while IFS= read -r line; do
        echo "[DEBUG] Received line: ${line:0:100}..." >&2
        # Check if line is valid JSON array
        if ! echo "$line" | jq -e '.' >/dev/null 2>&1; then
            echo "[DEBUG] Invalid JSON received: $line" >&2
            continue
        fi

        # Update nodes file with any new nodes
        echo "$line" | jq -c --slurpfile nodes ${NODES_FILE} '
          . as $array |
          ($nodes[0] // {}) as $current_nodes |

          # Extract new nodes and merge with existing
          if ($array | type) == "array" and ($array | length) > 0 then
            reduce (range(0; $array | length; 2) | select($array[.] == 3)) as $idx ($current_nodes;
              if $idx + 1 < ($array | length) then
                $array[$idx + 1] as $data |
                if ($data | type) == "array" and ($data | length) > 1 and
                   $data[0] != null and ($data[1] | type) == "array" and ($data[1] | length) >= 5 then
                  . + {($data[0] | tostring): {
                    name: $data[1][0],
                    node_id: $data[1][4]
                  }}
                else . end
              else . end
            )
          else
            $current_nodes
          end
        ' > ${NODES_FILE}.tmp && mv ${NODES_FILE}.tmp ${NODES_FILE} || echo "[DEBUG] Failed to update nodes file" >&2

        # Process block imports and generate output
        RESULT=$(echo "$line" | jq -c --slurpfile nodes ${NODES_FILE} --slurpfile blocks ${BLOCKS_FILE} '
          . as $array |
          ($nodes[0] // {}) as $node_map |
          ($blocks[0] // {}) as $current_blocks |

          # Process all block reports and track lowest propagation times
          (if ($array | type) == "array" and ($array | length) > 0 then
            reduce (range(0; $array | length; 2)) as $idx ({
              blocks: $current_blocks,
              outputs: []
            };
              if $idx + 1 < ($array | length) and $array[$idx] == 6 then
                $array[$idx + 1] as $data |
                if ($data | type) == "array" and ($data | length) >= 2 and
                    ($data[1] | type) == "array" and ($data[1] | length) >= 5 then
                  $data[0] as $node_idx |
                  $data[1][0] as $block_number |
                  $data[1][1] as $block_hash |
                  $data[1][4] as $propagation_time |

                  # Only process if all required values are valid
                  if ($node_idx != null and $block_hash != null and $block_number != null and
                      $propagation_time != null and
                      ($propagation_time | type) == "number" and
                      $propagation_time > 0) then

                    # Initialize block record if needed
                    .blocks[$block_hash] = (.blocks[$block_hash] // {
                      block_number: $block_number,
                      lowest_prop_time: 999999,
                      reporters: [],
                      first_seen: (now | floor),
                      report_count: 0,
                      output: false
                    }) |

                    # Update report count
                    .blocks[$block_hash].report_count += 1 |

                    # Check if this is a new lowest propagation time
                    if $propagation_time < .blocks[$block_hash].lowest_prop_time then
                      # New lowest - clear previous reporters and add this one
                      .blocks[$block_hash].lowest_prop_time = $propagation_time |
                      .blocks[$block_hash].reporters = [{
                        node_idx: $node_idx,
                        node_name: (if $node_idx != null then ($node_map[$node_idx | tostring].name // "unknown_node_\($node_idx)") else "unknown" end),
                        node_id: (if $node_idx != null then ($node_map[$node_idx | tostring].node_id // "unknown_id") else "unknown_id" end),
                        timestamp: (now | floor)
                      }]
                    elif $propagation_time == .blocks[$block_hash].lowest_prop_time then
                      # Equal to lowest - add to reporters list if not already present
                      $node_idx as $current_node |
                      if (.blocks[$block_hash].reporters | map(.node_idx // -1) | index($current_node) | not) then
                        .blocks[$block_hash].reporters += [{
                          node_idx: $node_idx,
                          node_name: (if $node_idx != null then ($node_map[$node_idx | tostring].name // "unknown_node_\($node_idx)") else "unknown" end),
                          node_id: (if $node_idx != null then ($node_map[$node_idx | tostring].node_id // "unknown_id") else "unknown_id" end),
                          timestamp: (now | floor)
                        }]
                      else . end
                    else
                      .
                    end
                  else
                    .
                  end
                else . end
              else . end
            )
          else
            {blocks: $current_blocks, outputs: []}
          end) |

          # Calculate max block number safely
          (.blocks as $updated_blocks |
           (if $updated_blocks == {} then 0
            else ($updated_blocks | to_entries | map(.value.block_number // 0) | max // 0)
            end) as $max_block |

          # Prepare CSV outputs for blocks that are ready and not yet output
          .outputs = [
            if $updated_blocks and $updated_blocks != {} then
              $updated_blocks | to_entries[] |
              select(
                .value.output == false and
                (
                  .value.report_count >= 3 or
                  ((now | floor) - .value.first_seen) > 3 or
                  (.value.block_number < $max_block - 1)
                )
              ) |
              .key as $block_hash |
              .value.reporters[] as $reporter |
              {
                block_hash: $block_hash,
                csv: [
                  $reporter.timestamp,
                  $reporter.node_name,
                  $reporter.node_id,
                  .value.block_number,
                  $block_hash,
                  .value.lowest_prop_time
                ] | @csv
              }
            else empty end
          ] |

          # Mark output blocks as output
          .outputs as $output_list |
          (if ($output_list | length) > 0 then
            reduce ($output_list | map(select(.block_hash != null) | .block_hash) | unique[]) as $hash (.blocks;
              if $hash != null and .[$hash] then .[$hash].output = true else . end
            )
          else .blocks end) as $marked_blocks |

          # Clean up old blocks from state (keep only recent 100)
          (
            if $marked_blocks == {} then {}
            else
              $marked_blocks | to_entries |
              map(select(.value.block_number != null)) |
              sort_by(.value.block_number) | reverse |
              .[0:100] |
              from_entries
            end
          ) as $cleaned_blocks |

          # Return result with proper structure
          {
            outputs: (if (.outputs | type) == "array" and (.outputs | length) > 0 then [.outputs[] | select(.csv != null) | .csv] else [] end),
            blocks: $cleaned_blocks
          })
        ')

        # Check if we got valid JSON result
        if [ -n "$RESULT" ]; then
            # Check for jq errors
            if echo "$RESULT" | grep -q "jq: error"; then
                echo "[DEBUG] JQ Error: $RESULT" >&2
                continue
            fi

            # Debug: show what we got
            echo "[DEBUG] Result type: $(echo "$RESULT" | jq -r 'type' 2>/dev/null || echo "unknown")" >&2

            # Extract and write outputs
            OUTPUTS=$(echo "$RESULT" | jq -r '.outputs[]? // empty' 2>/dev/null)
            if [ -n "$OUTPUTS" ]; then
                echo "[DEBUG] Writing outputs to CSV" >&2
                echo "$OUTPUTS" | while IFS= read -r csv_line; do
                    if [ -n "$csv_line" ]; then
                        echo "[DEBUG] CSV: $csv_line" >&2
                        echo "$csv_line" >> ${OUTPUT_PATH}
                    fi
                done
            else
                echo "[DEBUG] No outputs to write" >&2
            fi

            # Update blocks file
            echo "$RESULT" | jq -c '.blocks // {}' 2>/dev/null > ${BLOCKS_FILE}.tmp && mv ${BLOCKS_FILE}.tmp ${BLOCKS_FILE}

            # Debug: show block count and output count
            BLOCK_COUNT=$(echo "$RESULT" | jq -r '.blocks | length' 2>/dev/null || echo "0")
            OUTPUT_COUNT=$(echo "$RESULT" | jq -r '.outputs | length' 2>/dev/null || echo "0")
            echo "[$(date +%H:%M:%S)] Tracking $BLOCK_COUNT blocks, $OUTPUT_COUNT outputs ready" >&2
        else
            echo "[DEBUG] No result from jq processing" >&2
        fi
    done

    echo "[$(date)] Connection lost or error occurred. Reconnecting in 5 seconds..." >&2
    echo "[DEBUG] Last exit status: $?" >&2
    sleep 5
done
