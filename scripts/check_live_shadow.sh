#!/usr/bin/env bash
set -euo pipefail

if ! command -v jq >/dev/null 2>&1; then
  echo "ERROR: jq not found. Please install jq." >&2
  exit 1
fi

JSONL="${1:-}"
if [[ -z "${JSONL}" ]]; then
  echo "usage: $0 <live.jsonl>" >&2
  exit 1
fi
if [[ ! -f "${JSONL}" ]]; then
  echo "ERROR: file not found: ${JSONL}" >&2
  exit 1
fi

if ! jq -e . "${JSONL}" >/dev/null 2>&1; then
  echo "FAIL: invalid JSONL (jq parse error)"
  exit 1
fi

intent_count=$(jq -r 'select(.kind=="live_intent") | .kind' "${JSONL}" 2>/dev/null | wc -l | tr -d ' ')
place_count=$(jq -r 'select(.kind=="live_place") | .kind' "${JSONL}" 2>/dev/null | wc -l | tr -d ' ')
cancel_count=$(jq -r 'select(.kind=="live_cancel") | .kind' "${JSONL}" 2>/dev/null | wc -l | tr -d ' ')
user_ws_status_count=$(jq -r 'select(.kind=="user_ws_status") | .kind' "${JSONL}" 2>/dev/null | wc -l | tr -d ' ')
fill_count=$(jq -r 'select(.kind=="user_ws_fill") | .kind' "${JSONL}" 2>/dev/null | wc -l | tr -d ' ')
order_count=$(jq -r 'select(.kind=="user_ws_order") | .kind' "${JSONL}" 2>/dev/null | wc -l | tr -d ' ')
sim_fill_count=$(jq -r 'select(.kind=="user_ws_fill" and .data.raw_type=="sim_fill") | .kind' "${JSONL}" 2>/dev/null | wc -l | tr -d ' ')
sim_fill_enabled="${SIM_FILL:-0}"

fail=0

if [[ "${intent_count}" -gt 0 ]]; then
  echo "PASS: live_intent count=${intent_count}"
else
  echo "FAIL: live_intent count=${intent_count}"
  fail=$((fail+1))
fi

if [[ "${place_count}" -gt 0 ]]; then
  echo "PASS: live_place count=${place_count}"
else
  echo "FAIL: live_place count=${place_count}"
  fail=$((fail+1))
fi

echo "PASS: live_cancel count=${cancel_count}"

if [[ "${user_ws_status_count}" -gt 0 ]]; then
  echo "PASS: user_ws_status count=${user_ws_status_count}"
else
  echo "FAIL: user_ws_status count=${user_ws_status_count}"
  fail=$((fail+1))
fi

if [[ "${fill_count}" -ge 0 ]]; then
  echo "PASS: user_ws_fill count=${fill_count}"
fi

if [[ "${order_count}" -ge 0 ]]; then
  echo "PASS: user_ws_order count=${order_count}"
fi

if [[ "${sim_fill_enabled}" == "1" ]]; then
  eps_qty="${EPS_QTY:-0.01}"
  pair_cost_min_k="${PAIR_COST_MIN_K:-2}"
  pair_cost_fail=$(jq -sr --argjson k "${pair_cost_min_k}" '
    def both_legs_markets:
      [ .[] | select(.kind=="dryrun_snapshot") | .data
        | select((.qty_up|tonumber)>0 and (.qty_down|tonumber)>0)
        | .market_slug ] as $snap;
      [ .[] | select(.kind=="dryrun_snapshot") | .data
        | {m:.market_slug, up:.up_token_id, down:.down_token_id} ] as $map
      | [ .[] | select(.kind=="user_ws_fill") | .data
          | {m:.market_slug, token_id:.token_id} ] as $fills
      | [ $map[] | select(.up!=null and .down!=null)
          | . as $s
          | ($fills | map(select(.m==$s.m and (.token_id==$s.up or .token_id==$s.down))) | map(.token_id) | unique) as $ft
          | select(($ft|index($s.up)!=null) and ($ft|index($s.down)!=null))
          | $s.m ] as $fill;
      ($snap + $fill) | unique;
    def snaps_by_m($m):
      [ .[] | select(.kind=="dryrun_snapshot") | .data | select(.market_slug==$m) ]
      | sort_by(.ts_ms);
    def check_market($m):
      snaps_by_m($m) as $s
      | ($s|length) as $n
      | if $n==0 then empty
        else
          ( ($n*0.3)|ceil ) as $tail_n
          | ($s[($n-$tail_n):]) as $tail
          | ($tail | map(select(.pair_cost!=null)) | length) as $pc
          | if $pc < $k then {market:$m, tail_count:$tail_n, pair_cost_count:$pc, total:$n} else empty end
        end;
    both_legs_markets as $mkts
    | [ $mkts[] | check_market(.) ] | if length>0 then .[0] else empty end
  ' "${JSONL}" 2>/dev/null)
  if [[ -n "${pair_cost_fail}" ]]; then
    echo "FAIL: pair_cost missing in tail window: ${pair_cost_fail}"
    fail=$((fail+1))
  else
    required_markets=$(jq -sr '
      def both_legs_markets:
        [ .[] | select(.kind=="dryrun_snapshot") | .data
          | select((.qty_up|tonumber)>0 and (.qty_down|tonumber)>0)
          | .market_slug ] as $snap;
        [ .[] | select(.kind=="dryrun_snapshot") | .data
          | {m:.market_slug, up:.up_token_id, down:.down_token_id} ] as $map
        | [ .[] | select(.kind=="user_ws_fill") | .data
            | {m:.market_slug, token_id:.token_id} ] as $fills
        | [ $map[] | select(.up!=null and .down!=null)
            | . as $s
            | ($fills | map(select(.m==$s.m and (.token_id==$s.up or .token_id==$s.down))) | map(.token_id) | unique) as $ft
            | select(($ft|index($s.up)!=null) and ($ft|index($s.down)!=null))
            | $s.m ] as $fill;
        ($snap + $fill) | unique;
      both_legs_markets
    ' "${JSONL}" 2>/dev/null)
    if [[ -n "${required_markets}" && "${required_markets}" != "[]" ]]; then
      echo "PASS: pair_cost present in tail window when both legs filled"
    else
      echo "WARN: pair_cost not applicable (single-leg fills)"
    fi
  fi

  pairing_seq_window="${PAIRING_LEG_MAX_SEQ:-60}"
  pairing_violation=$(jq -sr --argjson n "${pairing_seq_window}" '
    [ .[] | select(.kind=="dryrun_snapshot") | .data ] as $snaps
    | [ .[] | select(.kind=="live_intent" or .kind=="live_place") | .data ] as $acts
    | [ $snaps[] | select(.round_state=="leg1_filled" and (.round_qty_target|tonumber)>0)
        | {m:.market_slug, seq:(.decision_seq|tonumber), leg1:(.round_leg1//null), up:.up_token_id, down:.down_token_id}
      ] as $starts
    | [ $starts[] as $s
        | ($acts | map(select(.market_slug==$s.m
            and (.decision_seq|tonumber) >= $s.seq
            and (.decision_seq|tonumber) <= ($s.seq + $n)
            and .token_id!=null
            and (
              ($s.leg1=="UP" and .token_id==$s.down) or
              ($s.leg1=="DOWN" and .token_id==$s.up)
            )
          )) | length) as $cnt
        | select($cnt==0)
        | {market_slug:$s.m, start_seq:$s.seq, leg1:$s.leg1, window:$n}
      ]
    | if length>0 then .[0] else empty end
  ' "${JSONL}" 2>/dev/null)
  if [[ -n "${pairing_violation}" ]]; then
    echo "FAIL: no pairing-leg action within seq window: ${pairing_violation}"
    fail=$((fail+1))
  else
    echo "PASS: pairing-leg action observed after leg1_filled"
  fi

  final_abs_net_violation=$(jq -sr --argjson eps "${eps_qty}" '
    def both_legs_markets:
      [ .[] | select(.kind=="dryrun_snapshot") | .data
        | {m:.market_slug, up:.up_token_id, down:.down_token_id} ] as $map
      | [ .[] | select(.kind=="user_ws_fill") | .data
          | {m:.market_slug, token_id:.token_id} ] as $fills
      | [ $map[] | select(.up!=null and .down!=null)
          | . as $s
          | ($fills | map(select(.m==$s.m and (.token_id==$s.up or .token_id==$s.down))) | map(.token_id) | unique) as $ft
          | select(($ft|index($s.up)!=null) and ($ft|index($s.down)!=null))
          | $s.m ] | unique;
    def final_snap($m):
      [ .[] | select(.kind=="dryrun_snapshot") | .data | select(.market_slug==$m) ]
      | sort_by(.ts_ms) | last;
    both_legs_markets as $mkts
    | [ $mkts[] | {market:., snap: final_snap(.)}
        | select(.snap!=null)
        | select((.snap.abs_net|tonumber) > $eps)
      ]
    | if length>0 then .[0] else empty end
  ' "${JSONL}" 2>/dev/null)
  if [[ -n "${final_abs_net_violation}" ]]; then
    echo "FAIL: final abs_net > EPS_QTY in SIM_FILL market: ${final_abs_net_violation}"
    fail=$((fail+1))
  else
    echo "PASS: final abs_net within EPS_QTY=${eps_qty} when both legs filled"
  fi

  tail_risk_violation=$(jq -sr '
    def idx:
      reduce [ .[] | select(.kind=="dryrun_snapshot") | .data ] as $s ({};
        .[($s.market_slug+"#"+($s.decision_seq|tostring))] = {tail_mode:$s.tail_mode, time_left:$s.time_left_secs});
    idx as $idx
    | [ .[] | select(.kind=="dryrun_gate_eval") | .data
        | . as $g
        | ($idx[$g.market_slug+"#"+($g.decision_seq|tostring)] // {}) as $s
        | select(($s.tail_mode=="freeze" or $s.tail_mode=="close") and ($g.allow==true) and ($g.would_increase_abs_net==true))
        | {market_slug:$g.market_slug, decision_seq:$g.decision_seq, tail_mode:$s.tail_mode, token_id:$g.token_id, side:$g.side, action_kind:$g.action_kind}
      ]
    | if length>0 then .[0] else empty end
  ' "${JSONL}" 2>/dev/null)
  if [[ -n "${tail_risk_violation}" ]]; then
    echo "FAIL: action increased abs_net after tail_freeze/close: ${tail_risk_violation}"
    fail=$((fail+1))
  else
    echo "PASS: no abs_net increase after tail_freeze/close"
  fi

  tail_close_violation=$(jq -sr '
    def abs(x): if x<0 then -x else x end;
    [ .[] | select(.kind=="dryrun_snapshot") | .data ] as $snaps
    | [ $snaps[] | select(.tail_mode=="close") | .market_slug ] | unique as $mkts
    | [ $mkts[] as $m
        | ($snaps | map(select(.market_slug==$m and .time_left_secs!=null and (.time_left_secs|tonumber)<=180))
           | map({ts:.ts_ms, abs_net:(.abs_net//0|tonumber)}) ) as $s
        | select(($s|length)>=2)
        | (reduce $s[] as $it ({"prev":null,"bad":null};
            if .bad!=null then .
            elif .prev==null then {"prev":$it,"bad":null}
            else
              (if ($it.abs_net > .prev.abs_net + 1e-9)
               then {"prev":$it,"bad":{market:$m, ts:$it.ts, prev:.prev, cur:$it}}
               else {"prev":$it,"bad":null} end)
            end) | .bad)
      ] | map(select(.!=null)) | if length>0 then .[0] else empty end
  ' "${JSONL}" 2>/dev/null)
  if [[ -n "${tail_close_violation}" ]]; then
    echo "FAIL: abs_net increased during tail_close: ${tail_close_violation}"
    fail=$((fail+1))
  else
    echo "PASS: abs_net non-increasing during tail_close"
  fi

  sim_fill_summary=$(jq -sr '
    def final_snap($m):
      [ .[] | select(.kind=="dryrun_snapshot") | .data | select(.market_slug==$m) ]
      | sort_by(.ts_ms) | last;
    [ .[] | select(.kind=="user_ws_fill") | .data ] as $fills
    | [ .[] | select(.kind=="dryrun_snapshot") | .data | .market_slug ] | unique as $mkts
    | [ $mkts[] as $m
        | ($fills | map(select(.market_slug==$m and .token_id!=null)) ) as $mf
        | (final_snap($m) // {}) as $snap
        | {
            market_slug:$m,
            fills_up: ($mf | map(select(.token_id==$snap.up_token_id)) | length),
            fills_down: ($mf | map(select(.token_id==$snap.down_token_id)) | length),
            final_qty_up: ($snap.qty_up // null),
            final_qty_down: ($snap.qty_down // null),
            final_pair_cost: ($snap.pair_cost // null),
            final_abs_net: ($snap.abs_net // null)
          }
      ]
    | map("\(.market_slug) fills_up=\(.fills_up) fills_down=\(.fills_down) final_qty_up=\(.final_qty_up) final_qty_down=\(.final_qty_down) final_pair_cost=\(.final_pair_cost) final_abs_net=\(.final_abs_net)") | .[]
  ' "${JSONL}" 2>/dev/null)
  if [[ -n "${sim_fill_summary}" ]]; then
    echo "INFO: sim_fill_summary:"
    while IFS= read -r line; do
      echo "INFO: ${line}"
    done <<< "${sim_fill_summary}"
  fi
fi

missing_fields=$(jq -sc '
  [ .[] | select(.kind=="live_intent" or .kind=="live_place")
    | .data as $d
    | select(($d.token_id==null) or ($d.side==null) or ($d.price==null) or ($d.size==null) or ($d.post_only==null) or ($d.tick_size==null))
  ]
  | if length>0 then .[0] else empty end
' "${JSONL}" 2>/dev/null)
if [[ -n "${missing_fields}" ]]; then
  echo "FAIL: live_intent/live_place missing required fields: ${missing_fields}"
  fail=$((fail+1))
else
  echo "PASS: live_intent/live_place required fields present"
fi

bad_post_only=$(jq -sc '
  [ .[] | select(.kind=="live_intent" or .kind=="live_place")
    | .data as $d
    | select($d.post_only != true)
  ]
  | if length>0 then .[0] else empty end
' "${JSONL}" 2>/dev/null)
if [[ -n "${bad_post_only}" ]]; then
  echo "FAIL: live_intent/live_place post_only not true: ${bad_post_only}"
  fail=$((fail+1))
else
  echo "PASS: live_intent/live_place post_only true"
fi

bad_size_price=$(jq -sc '
  [ .[] | select(.kind=="live_intent" or .kind=="live_place")
    | .data as $d
    | select(($d.size|tonumber?)<=0 or ($d.price|tonumber?)<=0)
  ]
  | if length>0 then .[0] else empty end
' "${JSONL}" 2>/dev/null)
if [[ -n "${bad_size_price}" ]]; then
  echo "FAIL: live_intent/live_place size/price invalid: ${bad_size_price}"
  fail=$((fail+1))
else
  echo "PASS: live_intent/live_place size/price positive"
fi

shadow_missing_ids=$(jq -sc '
  [ .[] | select(.kind=="live_place" or .kind=="live_cancel")
    | .data as $d
    | select(($d.client_order_id==null) or ($d.order_id_str==null) or ($d.would_send_hash==null))
  ]
  | if length>0 then .[0] else empty end
' "${JSONL}" 2>/dev/null)
if [[ -n "${shadow_missing_ids}" ]]; then
  echo "FAIL: live_place/live_cancel missing shadow receipt fields: ${shadow_missing_ids}"
  fail=$((fail+1))
else
  echo "PASS: live_place/live_cancel shadow receipt fields present"
fi

missing_decision_seq=$(jq -sc '
  [ .[] | select(.kind=="live_place" or .kind=="live_cancel" or .kind=="live_skip")
    | .data as $d
    | select($d.decision_seq==null)
  ]
  | if length>0 then .[0] else empty end
' "${JSONL}" 2>/dev/null)
if [[ -n "${missing_decision_seq}" ]]; then
  echo "FAIL: live_* missing decision_seq: ${missing_decision_seq}"
  fail=$((fail+1))
else
  echo "PASS: live_* decision_seq present"
fi

bad_one_tick=$(jq -sc '
  def num(x):
    if x==null then null
    elif (x|type)=="string" then (x|tonumber?)
    elif (x|type)=="number" then x
    else null end;
  [ .[] | select(.kind=="live_intent" or .kind=="live_place")
    | .data as $d
    | ($d.intent // {}) as $i
    | select($i.one_tick_improved==true)
    | (num($i.tick_size) // num($d.tick_size) // num($d.tick_size_current)) as $t
    | (num($d.price) // num($i.price)) as $p
    | (num($i.best_bid)) as $bid
    | (num($i.best_ask)) as $ask
    | (($d.side // $i.side // "") | ascii_downcase) as $side
    | select($t==null or $p==null or $bid==null or $ask==null or ($i.quote_policy != "one_tick")
        or (
          ($side=="buy" and ($p != ($bid + $t) or $p >= $ask)) or
          ($side=="sell" and ($p != ($ask - $t) or $p <= $bid))
        )
      )
    | {token_id:$d.token_id, side:$side, price:$p, tick_size:$t, best_bid:$bid, best_ask:$ask, quote_policy:$i.quote_policy}
  ]
  | if length>0 then .[0] else empty end
' "${JSONL}" 2>/dev/null)
if [[ -n "${bad_one_tick}" ]]; then
  echo "FAIL: one_tick_improved mismatch: ${bad_one_tick}"
  fail=$((fail+1))
else
  echo "PASS: one_tick_improved alignment"
fi

bad_fill_fields=$(jq -sc '
  [ .[] | select(.kind=="user_ws_fill")
    | .data as $d
    | select(($d.token_id==null) or ($d.side==null) or ($d.price==null) or ($d.size==null) or ($d.raw_type==null))
  ]
  | if length>0 then .[0] else empty end
' "${JSONL}" 2>/dev/null)
if [[ -n "${bad_fill_fields}" ]]; then
  echo "FAIL: user_ws_fill missing required fields: ${bad_fill_fields}"
  fail=$((fail+1))
else
  echo "PASS: user_ws_fill required fields present"
fi

bad_order_fields=$(jq -sc '
  [ .[] | select(.kind=="user_ws_order")
    | .data as $d
    | select(($d.raw_type==null))
  ]
  | if length>0 then .[0] else empty end
' "${JSONL}" 2>/dev/null)
if [[ -n "${bad_order_fields}" ]]; then
  echo "FAIL: user_ws_order missing required fields: ${bad_order_fields}"
  fail=$((fail+1))
else
  echo "PASS: user_ws_order required fields present"
fi

status_has_new_fields=$(jq -sr '
  [.[] | select(.kind=="user_ws_status") | .data.conn_id]
  | map(select(.!=null))
  | length
' "${JSONL}" 2>/dev/null | tr -d '[:space:]')
status_has_new_fields="${status_has_new_fields:-0}"
if [[ "${status_has_new_fields}" -gt 0 ]]; then
bad_status_fields=$(jq -sc '
  [ .[] | select(.kind=="user_ws_status")
    | .data as $d
    | select(($d.state==null) or ($d.conn_id==null) or ($d.attempt==null) or ($d.backoff_secs==null) or ($d.ts_ms==null))
  ]
  | if length>0 then .[0] else empty end
' "${JSONL}" 2>/dev/null)
if [[ -n "${bad_status_fields}" ]]; then
  echo "FAIL: user_ws_status missing required fields: ${bad_status_fields}"
  fail=$((fail+1))
else
  echo "PASS: user_ws_status required fields present"
fi
else
  echo "INFO: user_ws_status required fields skipped (no conn_id present)"
fi

bad_fill_size_price=$(jq -sc '
  [ .[] | select(.kind=="user_ws_fill")
    | .data as $d
    | select(($d.size|tonumber?)<=0 or ($d.price|tonumber?)<=0)
  ]
  | if length>0 then .[0] else empty end
' "${JSONL}" 2>/dev/null)
if [[ -n "${bad_fill_size_price}" ]]; then
  echo "FAIL: user_ws_fill size/price invalid: ${bad_fill_size_price}"
  fail=$((fail+1))
else
  echo "PASS: user_ws_fill size/price positive"
fi

if [[ "${sim_fill_enabled}" == "1" ]]; then
  pair_cost_missing=$(jq -sr '
    def snap_req:
      [ .[] | select(.kind=="dryrun_snapshot") | .data
        | select((.qty_up|tonumber)>0 and (.qty_down|tonumber)>0)
        | .market_slug ] as $snap;
    def fill_req:
      [ .[] | select(.kind=="dryrun_snapshot") | .data
        | {m:.market_slug, up:.up_token_id, down:.down_token_id} ] as $map
      | [ .[] | select(.kind=="user_ws_fill") | .data
          | {m:.market_slug, token_id:.token_id} ] as $fills
      | [ $map[] | select(.up!=null and .down!=null)
          | . as $s
          | ($fills | map(select(.m==$s.m and (.token_id==$s.up or .token_id==$s.down))) | map(.token_id) | unique) as $ft
          | select(($ft|index($s.up)!=null) and ($ft|index($s.down)!=null))
          | $s.m ] as $fill;
    ($snap + $fill) | unique as $req
    | [ .[] | select(.kind=="dryrun_snapshot") | .data ] as $snaps
    | [ $req[] | select(([$snaps[] | select(.market_slug==. ) | select(.pair_cost!=null)] | length) == 0) ] as $missing
    | if ($missing|length)>0 then $missing[0] else empty end
  ' "${JSONL}" 2>/dev/null)
  if [[ -n "${pair_cost_missing}" ]]; then
    echo "FAIL: pair_cost missing for market=${pair_cost_missing}"
    fail=$((fail+1))
  else
    required_markets=$(jq -sr '
      def snap_req:
        [ .[] | select(.kind=="dryrun_snapshot") | .data
          | select((.qty_up|tonumber)>0 and (.qty_down|tonumber)>0)
          | .market_slug ] as $snap;
      def fill_req:
        [ .[] | select(.kind=="dryrun_snapshot") | .data
          | {m:.market_slug, up:.up_token_id, down:.down_token_id} ] as $map
        | [ .[] | select(.kind=="user_ws_fill") | .data
            | {m:.market_slug, token_id:.token_id} ] as $fills
        | [ $map[] | select(.up!=null and .down!=null)
            | . as $s
            | ($fills | map(select(.m==$s.m and (.token_id==$s.up or .token_id==$s.down))) | map(.token_id) | unique) as $ft
            | select(($ft|index($s.up)!=null) and ($ft|index($s.down)!=null))
            | $s.m ] as $fill;
      ($snap + $fill) | unique
    ' "${JSONL}" 2>/dev/null)
    if [[ -n "${required_markets}" && "${required_markets}" != "[]" ]]; then
      echo "PASS: pair_cost present when both legs filled"
    else
      echo "WARN: pair_cost not applicable (single-leg fills)"
    fi
  fi

  tail_place_violation=$(jq -sr '
    def ts($o): ($o.data.ts_ms // $o.ts_ms);
    def freeze_map:
      reduce [ .[] | select(.kind=="dryrun_snapshot") | .data | select(.tail_mode=="freeze") | {m:.market_slug, ts:.ts_ms} ] as $it ({};
        if .[$it.m]==null or $it.ts < .[$it.m] then .[$it.m]=$it.ts else . end);
    freeze_map as $fm
    | [ .[] | select(.kind=="live_place")
        | {m:(.data.market_slug // .data.market // .market_slug), ts:(.data.ts_ms // .ts_ms), price:(.data.price), token_id:(.data.token_id), side:(.data.side)}
        | select($fm[.m]!=null and (.ts|tonumber) >= ($fm[.m]|tonumber))
      ]
    | if length>0 then .[0] else empty end
  ' "${JSONL}" 2>/dev/null)
  if [[ -n "${tail_place_violation}" ]]; then
    echo "FAIL: live_place occurs after tail_freeze (per-market): ${tail_place_violation}"
    fail=$((fail+1))
  else
    echo "PASS: no live_place after tail_freeze (per-market)"
  fi

  tail_net_violation=$(jq -sr '
    def abs(x): if x<0 then -x else x end;
    [ .[] | select(.kind=="dryrun_snapshot") | .data ] as $snaps
    | [ $snaps[] | select(.tail_mode=="freeze" or .tail_mode=="close") | .market_slug ] | unique as $mkts
    | [ $mkts[] as $m
        | ($snaps | map(select(.market_slug==$m and .time_left_secs!=null and (.time_left_secs|tonumber)<=300))
           | map({ts:.ts_ms, qty_up:(.qty_up//0|tonumber), qty_down:(.qty_down//0|tonumber)}) ) as $s
        | select(($s|length)>=2)
        | (reduce $s[] as $it ({"prev":null,"bad":null};
            if .bad!=null then .
            elif .prev==null then {"prev":$it,"bad":null}
            else
              (if (abs($it.qty_up-$it.qty_down) > abs(.prev.qty_up-.prev.qty_down) + 1e-9)
               then {"prev":$it,"bad":{market:$m, ts:$it.ts, prev:.prev, cur:$it}}
               else {"prev":$it,"bad":null} end)
            end) | .bad)
      ] | map(select(.!=null)) | if length>0 then .[0] else empty end
  ' "${JSONL}" 2>/dev/null)
  if [[ -n "${tail_net_violation}" ]]; then
    echo "FAIL: net leg increased during tail (per-market): ${tail_net_violation}"
    fail=$((fail+1))
  else
    echo "PASS: net leg non-increasing in tail (per-market)"
  fi
fi

skip_count=$(jq -sr '
  def num(x):
    if x==null then null
    elif (x|type)=="string" then (x|tonumber?)
    elif (x|type)=="number" then x
    else null end;
  def tick($d): num($d.tick_size) // num($d.intent.tick_size) // num($d.tick_size_current);
  def price($d): num($d.price) // num($d.intent.price);
  [.[] | select(.kind=="live_intent" or .kind=="live_place")
   | .data as $d
   | (tick($d)) as $t
   | (price($d)) as $p
   | select($t==null or $p==null)
  ] | length
' "${JSONL}" 2>/dev/null | tr -d '[:space:]')
skip_count="${skip_count:-0}"
if [[ "${skip_count}" != "0" ]]; then
  echo "INFO: tick_alignment_skipped=${skip_count}"
fi

misaligned=$(jq -sc '
  def num(x):
    if x==null then null
    elif (x|type)=="string" then (x|tonumber?)
    elif (x|type)=="number" then x
    else null end;
  def tick($d): num($d.tick_size) // num($d.intent.tick_size) // num($d.tick_size_current);
  def price($d): num($d.price) // num($d.intent.price);
  def aligned($p;$t):
    if ($p==null or $t==null or $t==0) then null
    else ( (($p/$t) - (($p/$t)|round)) | fabs ) < 1e-9 end;
  select(.kind=="live_intent" or .kind=="live_place")
  | .data as $d
  | (tick($d)) as $t
  | (price($d)) as $p
  | select($t!=null and $p!=null)
  | select(aligned($p;$t)==false)
  | {token_id:$d.token_id, side:$d.side, price:$p, tick_size:$t, ts_ms:$d.ts_ms, decision_seq:$d.decision_seq}
  ]
  | if length>0 then .[0] else empty end
' "${JSONL}" 2>/dev/null)
if [[ -n "${misaligned}" ]]; then
  echo "FAIL: live_intent/live_place tick misaligned: ${misaligned}"
  fail=$((fail+1))
else
  echo "PASS: live_intent/live_place tick alignment"
fi

stale_cancel_count=$(jq -r 'select(.kind=="live_cancel" and .data.reason=="stale_ttl") | .kind' "${JSONL}" 2>/dev/null | wc -l | tr -d ' ')
stale_place_count=$(jq -r 'select(.kind=="live_place" and (.data.reason=="stale_reprice" or .data.reason=="replace_price")) | .kind' "${JSONL}" 2>/dev/null | wc -l | tr -d ' ')
stale_total=$((stale_cancel_count + stale_place_count))
if [[ "${stale_total}" -gt 0 ]]; then
  echo "PASS: stale ttl events count=${stale_total}"
else
  echo "FAIL: stale ttl events count=${stale_total}"
  fail=$((fail+1))
fi

rapid_count=$(jq -sr '
  def tok($d): $d.token_id;
  def side($d): ($d.side | tostring | ascii_downcase);
  def ts($d; $root): $d.ts_ms // $root.ts_ms // 0;
  [.[] | select(.kind=="live_place" or .kind=="live_cancel")
   | . as $root
   | .data as $d
   | {kind:.kind, token_id:tok($d), side:side($d), ts_ms:ts($d; $root)}
   | select(.token_id!=null and .side!=null and .ts_ms!=null)
  ]
  | sort_by(.token_id, .side, .ts_ms)
  | [range(1; length) as $i
      | {prev:.[ $i-1 ], curr:.[ $i ]}
      | select(.prev.token_id == .curr.token_id and .prev.side == .curr.side)
      | select(.prev.kind=="live_cancel" and .curr.kind=="live_place")
      | select((.curr.ts_ms - .prev.ts_ms) < 1000)
    ]
  | length
' "${JSONL}" 2>/dev/null | tr -d '[:space:]')
rapid_count="${rapid_count:-0}"
if [[ "${rapid_count}" -gt 0 ]]; then
rapid_offender=$(jq -sc '
    def tok($d): $d.token_id;
    def side($d): ($d.side | tostring | ascii_downcase);
    def ts($d; $root): $d.ts_ms // $root.ts_ms // 0;
    def price($d): $d.price;
    def cid($d): $d.client_order_id // $d.order_id;
    [.[] | select(.kind=="live_place" or .kind=="live_cancel")
     | . as $root
     | .data as $d
     | {kind:.kind, token_id:tok($d), side:side($d), ts_ms:ts($d; $root),
        reason:$d.reason, price:price($d), client_order_id:cid($d)}
     | select(.token_id!=null and .side!=null and .ts_ms!=null)
    ]
    | sort_by(.token_id, .side, .ts_ms)
    | [range(1; length) as $i
        | {prev:.[ $i-1 ], curr:.[ $i ]}
        | select(.prev.token_id == .curr.token_id and .prev.side == .curr.side)
        | select(.prev.kind=="live_cancel" and .curr.kind=="live_place")
        | select((.curr.ts_ms - .prev.ts_ms) < 1000)
        | {token_id:.curr.token_id, side:.curr.side,
           cancel_ts_ms:.prev.ts_ms, place_ts_ms:.curr.ts_ms,
           dt_ms:(.curr.ts_ms - .prev.ts_ms),
           cancel_reason:.prev.reason, place_reason:.curr.reason,
           cancel_price:.prev.price, place_price:.curr.price,
           cancel_client_order_id:.prev.client_order_id,
           place_client_order_id:.curr.client_order_id,
           prev_kind:.prev.kind, curr_kind:.curr.kind}
      ]
    | .[0] // {}
  ' "${JSONL}" 2>/dev/null)
  if [[ -z "${rapid_offender}" || "${rapid_offender}" == "null" || "${rapid_offender}" == "{}" ]]; then
    echo "FAIL: rapid cancel/place within 1s: (no sample; count=${rapid_count})"
  else
    echo "FAIL: rapid cancel/place within 1s: ${rapid_offender}"
  fi
  fail=$((fail+1))
else
  echo "PASS: no rapid cancel/place bursts"
fi

rapid_dt_stats=$(jq -sr '
  def tok($d): $d.token_id;
  def side($d): ($d.side | tostring | ascii_downcase);
  def ts($d; $root): $d.ts_ms // $root.ts_ms // 0;
  [.[] | select(.kind=="live_place" or .kind=="live_cancel")
   | . as $root
   | .data as $d
   | {kind:.kind, token_id:tok($d), side:side($d), ts_ms:ts($d; $root)}
   | select(.token_id!=null and .side!=null and .ts_ms!=null)
  ]
  | sort_by(.token_id, .side, .ts_ms)
  | [range(1; length) as $i
      | {prev:.[ $i-1 ], curr:.[ $i ]}
      | select(.prev.token_id == .curr.token_id and .prev.side == .curr.side)
      | select(.prev.kind=="live_cancel" and .curr.kind=="live_place")
      | (.curr.ts_ms - .prev.ts_ms)
    ]
  | (if length==0 then "" else
      (sort) as $s
      | {min:$s[0], p50:$s[(length/2|floor)], p95:$s[(length*0.95|floor)], max:$s[-1]}
      | "min=" + (.min|tostring) + " p50=" + (.p50|tostring) + " p95=" + (.p95|tostring) + " max=" + (.max|tostring)
    end)
' "${JSONL}" 2>/dev/null)
if [[ -n "${rapid_dt_stats}" ]]; then
  echo "INFO: rapid_dt_ms ${rapid_dt_stats}"
fi

live_skip_count=$(jq -r 'select(.kind=="live_skip") | .kind' "${JSONL}" 2>/dev/null | wc -l | tr -d ' ')
echo "PASS: live_skip count=${live_skip_count}"

# Tail checks: per-market invariants

tail_gate_violation=$(jq -sc '
  . as $all
  | [ $all[] | select(.kind=="dryrun_snapshot")
      | {seq:.data.decision_seq, tail:.data.tail_mode, market:.data.market_slug} ] as $snaps
  | [ $all[] | select(.kind=="dryrun_gate_eval") | .data as $d
      | ($snaps[]? | select(.seq==$d.decision_seq) | .tail) as $tm
      | select($tm=="freeze" or $tm=="close")
      | select($d.allow==true and $d.would_increase_abs_net==true)
      | {decision_seq:$d.decision_seq, market_slug:$d.market_slug, tail_mode:$tm, token_id:$d.token_id, side:$d.side, delta_abs_net:$d.delta_abs_net}
    ]
  | if length>0 then .[0] else empty end
' "${JSONL}" 2>/dev/null)
if [[ -n "${tail_gate_violation}" ]]; then
  echo "FAIL: tail allowed abs_net increase: ${tail_gate_violation}"
  fail=$((fail+1))
else
  echo "PASS: tail gates prevent abs_net increase"
fi

tail_abs_violation=$(jq -sc '
  def abs(x): if x<0 then -x else x end;
  . as $all
  | [ $all[] | select(.kind=="dryrun_snapshot") | .data ] as $snaps
  | [ $snaps[] | select(.tail_mode=="freeze" or .tail_mode=="close") | .market_slug ] | unique as $mkts
  | [ $mkts[] as $m
      | ($snaps | map(select(.market_slug==$m and .time_left_secs!=null and (.time_left_secs|tonumber)<=300))
         | map({ts:.ts_ms, abs_net:(.abs_net//0|tonumber)}) ) as $s
      | select(($s|length)>=2)
      | (reduce $s[] as $it ({"prev":null,"bad":null};
          if .bad!=null then .
          elif .prev==null then {"prev":$it,"bad":null}
          else
            (if ($it.abs_net > (.prev.abs_net) + 1e-9)
             then {"prev":$it,"bad":{market:$m, ts:$it.ts, prev:.prev, cur:$it}}
             else {"prev":$it,"bad":null} end)
          end) | .bad)
    ] | map(select(.!=null)) | if length>0 then .[0] else empty end
' "${JSONL}" 2>/dev/null)
if [[ -n "${tail_abs_violation}" ]]; then
  echo "FAIL: abs_net increased during tail (per-market): ${tail_abs_violation}"
  fail=$((fail+1))
else
  echo "PASS: abs_net non-increasing in tail (per-market)"
fi

final_abs_net_violation=$(jq -sc '
  . as $all
  | [ $all[] | select(.kind=="dryrun_snapshot") | .data ] as $snaps
  | [ $snaps[] | select(.tail_mode=="close") | .market_slug ] | unique as $mkts
  | [ $mkts[] as $m
      | ($snaps | map(select(.market_slug==$m and .tail_mode=="close"))) as $tail
      | select(($tail|length)>0)
      | ($tail | map(select(.have_quotes==true)) | length) as $hq
      | if $hq==0 then empty else
          ( $tail | last ) as $last
          | select(($last.abs_net//0|tonumber) > 1e-9)
          | {market:$m, abs_net:$last.abs_net, ts:$last.ts_ms}
        end
    ] | if length>0 then .[0] else empty end
' "${JSONL}" 2>/dev/null)
if [[ -n "${final_abs_net_violation}" ]]; then
  echo "FAIL: tail_close final abs_net not zero: ${final_abs_net_violation}"
  fail=$((fail+1))
else
  echo "PASS: tail_close final abs_net zero when quotes available"
fi

missing_tail_skip=$(jq -sc '
  . as $all
  | [ $all[] | select(.kind=="dryrun_snapshot")
      | {seq:.data.decision_seq, tail_mode:.data.tail_mode}
    ] as $snaps
  | [ $snaps[] | select(.tail_mode!="none") | . as $s
      | select(([ $all[] | select(.kind=="live_skip" and .data.decision_seq==$s.seq) ] | length) == 0)
      | $s
    ]
  | if length>0 then .[0] else empty end
' "${JSONL}" 2>/dev/null)
if [[ -n "${missing_tail_skip}" ]]; then
  echo "FAIL: tail mode missing live_skip: ${missing_tail_skip}"
  fail=$((fail+1))
else
  echo "PASS: tail mode has live_skip"
fi

tail_cancel_all=$(jq -sr '
  . as $all
  | [ $all[] | select(.kind=="dryrun_snapshot" and (.data.tail_mode=="freeze" or .data.tail_mode=="close")) ] as $tails
  | [ $all[] | select(.kind=="live_cancel") | select(.data.reason=="cancel_all") ] as $cancels
  | if ($tails|length) > 0 and ($cancels|length) == 0 then 1 else 0 end
' "${JSONL}" 2>/dev/null | tr -d '[:space:]')
tail_cancel_all="${tail_cancel_all:-0}"
if [[ "${tail_cancel_all}" -gt 0 ]]; then
  echo "FAIL: tail mode entered but no cancel_all observed"
  fail=$((fail+1))
else
  echo "PASS: tail cancel_all observed when needed"
fi
fill_skip_count=$(jq -sr '
  def num(x):
    if x==null then null
    elif (x|type)=="string" then (x|tonumber?)
    elif (x|type)=="number" then x
    else null end;
  def tick($d): num($d.tick_size) // num($d.intent.tick_size) // num($d.tick_size_current);
  def price($d): num($d.price) // num($d.intent.price);
  [.[] | select(.kind=="user_ws_fill" or .kind=="user_ws_order")
   | .data as $d
   | (tick($d)) as $t
   | (price($d)) as $p
   | select($t==null or $p==null)
  ] | length
' "${JSONL}" 2>/dev/null | tr -d '[:space:]')
fill_skip_count="${fill_skip_count:-0}"
if [[ "${fill_skip_count}" != "0" ]]; then
  echo "INFO: user_ws_tick_alignment_skipped=${fill_skip_count}"
fi

fill_misaligned=$(jq -sc '
  def num(x):
    if x==null then null
    elif (x|type)=="string" then (x|tonumber?)
    elif (x|type)=="number" then x
    else null end;
  def tick($d): num($d.tick_size) // num($d.intent.tick_size) // num($d.tick_size_current);
  def price($d): num($d.price) // num($d.intent.price);
  def aligned($p;$t):
    if ($p==null or $t==null or $t==0) then null
    else ( (($p/$t) - (($p/$t)|round)) | fabs ) < 1e-9 end;
  select(.kind=="user_ws_fill" or .kind=="user_ws_order")
  | .data as $d
  | (tick($d)) as $t
  | (price($d)) as $p
  | select($t!=null and $p!=null)
  | select(aligned($p;$t)==false)
  | {token_id:$d.token_id, side:$d.side, price:$p, tick_size:$t, ts_ms:$d.ts_ms, decision_seq:$d.decision_seq}
  ]
  | if length>0 then .[0] else empty end
' "${JSONL}" 2>/dev/null)
if [[ -n "${fill_misaligned}" ]]; then
  echo "FAIL: user_ws_fill/order tick misaligned: ${fill_misaligned}"
  fail=$((fail+1))
else
  echo "PASS: user_ws_fill/order tick alignment"
fi

# user ws stability checks
user_ws_error_count=$(jq -r '
  select(.kind=="user_ws_status" or .kind=="user_ws_error")
  | .data as $d
  | select($d.state=="error" or $d.state=="disconnected")
  | .kind
' "${JSONL}" 2>/dev/null | wc -l | tr -d ' ')
user_ws_error_count="${user_ws_error_count:-0}"
if [[ "${user_ws_error_count}" -le 50 ]]; then
  echo "PASS: user_ws_error count=${user_ws_error_count}"
else
  echo "FAIL: user_ws_error count=${user_ws_error_count}"
  fail=$((fail+1))
fi

min_error_dt_ms=$(jq -sr '
  [.[] | select(.kind=="user_ws_status" or .kind=="user_ws_error")
   | .data as $d
   | select($d.state=="error" or $d.state=="disconnected")
   | ($d.ts_ms // .ts_ms)
  ]
  | sort
  | [range(1; length) as $i | (.[ $i ] - .[ $i-1 ])]
  | (if length==0 then "" else (sort | .[0]) end)
' "${JSONL}" 2>/dev/null | tr -d '[:space:]')
if [[ -n "${min_error_dt_ms}" ]]; then
  if [[ "${min_error_dt_ms}" -lt 1000 ]]; then
    echo "FAIL: user_ws_error min_dt_ms=${min_error_dt_ms} (<1000)"
    fail=$((fail+1))
  else
    echo "PASS: user_ws_error min_dt_ms=${min_error_dt_ms}"
  fi
else
  echo "PASS: user_ws_error min_dt_ms=NA"
fi

reset_no_handshake_run=$(jq -sr '
  [.[] | select(.kind=="user_ws_status" or .kind=="user_ws_error")
   | .data as $d
   | select(($d.state=="error" or $d.state=="disconnected") and ($d.err_kind=="reset_no_handshake"))
   | {ts: ($d.ts_ms // .ts_ms), backoff: ($d.backoff_secs // 0)}
  ]
  | sort_by(.ts)
  | reduce .[] as $e ({run:0, max_run:0};
      if ($e.backoff <= 1) then .run += 1 else .run = 0 end
      | if .run > .max_run then .max_run = .run else . end)
  | .max_run
' "${JSONL}" 2>/dev/null | tr -d '[:space:]')
reset_no_handshake_run="${reset_no_handshake_run:-0}"
if [[ "${reset_no_handshake_run}" -ge 5 ]]; then
  echo "FAIL: user_ws reset_no_handshake backoff not increasing (max_run=${reset_no_handshake_run})"
  fail=$((fail+1))
else
  echo "PASS: user_ws reset_no_handshake backoff increasing"
fi

err_kind_top=$(jq -sr '
  [.[] | select(.kind=="user_ws_status" or .kind=="user_ws_error") | .data.err_kind]
  | map(select(.!=null))
  | group_by(.)
  | map({k:.[0], c:length})
  | sort_by(-.c)
  | .[0:3]
  | map("\(.k)=\(.c)")
  | join(" ")
' "${JSONL}" 2>/dev/null)
if [[ -n "${err_kind_top}" ]]; then
  echo "INFO: user_ws_err_kind_top ${err_kind_top}"
fi

since_stats=$(jq -sr '
  [.[] | select(.kind=="user_ws_status" or .kind=="user_ws_error") | .data.since_connected_ms]
  | map(select(.!=null))
  | sort
  | if length==0 then "" else
      (.[0] as $min | .[(length/2|floor)] as $p50 | .[-1] as $max
      | "min=\($min) p50=\($p50) max=\($max)") end
' "${JSONL}" 2>/dev/null)

recv_first_msg_count=$(jq -r '
  select(.kind=="user_ws_status")
  | .data as $d
  | select($d.state=="recv_first_msg")
  | .kind
' "${JSONL}" 2>/dev/null | wc -l | tr -d ' ')
recv_first_msg_count="${recv_first_msg_count:-0}"
if [[ "${recv_first_msg_count}" -gt 0 ]]; then
  echo "PASS: user_ws recv_first_msg count=${recv_first_msg_count}"
else
  echo "FAIL: user_ws recv_first_msg count=${recv_first_msg_count}"
  fail=$((fail+1))
fi

server_error_count=$(jq -r '
  select(.kind=="user_ws_error" or .kind=="user_ws_status")
  | .data as $d
  | select($d.err_kind=="server_error")
  | .kind
' "${JSONL}" 2>/dev/null | wc -l | tr -d ' ')
server_error_count="${server_error_count:-0}"
if [[ "${server_error_count}" -gt 0 ]]; then
  echo "FAIL: user_ws server_error count=${server_error_count}"
  fail=$((fail+1))
else
  echo "PASS: user_ws server_error count=${server_error_count}"
fi

executed_place_cancel_count=$(jq -sr '
  [ .[] | select(
      (.kind=="live_place" and (.data.order_id!=null or .data.order_id_str!=null))
      or (.kind=="live_cancel" and (.data.order_id!=null or .data.order_id_str!=null))
    )
  ] | length
' "${JSONL}" 2>/dev/null | tr -d '[:space:]')
executed_place_cancel_count="${executed_place_cancel_count:-0}"
if [[ "${executed_place_cancel_count}" -gt 0 ]]; then
  if [[ "${order_count}" -gt 0 ]]; then
    echo "PASS: user_ws_order present when executed place/cancel > 0"
  else
    echo "FAIL: user_ws_order count=${order_count} with executed place/cancel > 0"
    fail=$((fail+1))
  fi
fi

# Place must see an open user_ws_order within 5s (shadow consistency)
place_missing_open=$(jq -sc '
  def side($d): ($d.side // $d.payload.side // $d.intent.side // "") | tostring | ascii_downcase;
  def tok($d): $d.token_id // $d.payload.token_id // $d.intent.token_id;
  def ts($d; $root): $d.ts_ms // $root.ts_ms // 0;
  def oid($d): $d.order_id // $d.order_id_str // $d.client_order_id;
  def is_open($s):
    ($s | tostring | ascii_downcase) as $x
    | ($x=="open" or $x=="accepted" or $x=="working" or $x=="live" or $x=="placed" or $x=="new");
  [ .[] | select(.kind=="user_ws_order") | .data
    | {order_id: oid(.), status: (.status // ""), ts_ms: (.ts_ms // 0)}
    | select(.order_id!=null)
  ] as $orders
  | [ .[] | select(.kind=="live_place") | . as $root | .data as $d
      | {order_id: oid($d), ts_ms: ts($d;$root), token_id: tok($d), side: side($d), price: $d.price} as $p
      | select(.order_id!=null and .ts_ms!=null)
      | select(([$orders[] | select(.order_id==$p.order_id and is_open(.status) and (.ts_ms >= $p.ts_ms) and (.ts_ms - $p.ts_ms) <= 5000)] | length) == 0)
    ]
  | if length>0 then .[0] else empty end
' "${JSONL}" 2>/dev/null)
if [[ -n "${place_missing_open}" ]]; then
  echo "FAIL: live_place missing user_ws_order open within 5s: ${place_missing_open}"
  fail=$((fail+1))
else
  echo "PASS: live_place user_ws_order open within 5s"
fi

# Cancel must see a canceled user_ws_order within 5s
cancel_missing_close=$(jq -sc '
  def side($d): ($d.side // $d.payload.side // $d.intent.side // "") | tostring | ascii_downcase;
  def tok($d): $d.token_id // $d.payload.token_id // $d.intent.token_id;
  def ts($d; $root): $d.ts_ms // $root.ts_ms // 0;
  def oid($d): $d.order_id // $d.order_id_str // $d.client_order_id;
  def is_close($s):
    ($s | tostring | ascii_downcase) as $x
    | ($x=="canceled" or $x=="cancelled" or $x=="filled" or $x=="rejected" or $x=="expired");
  [ .[] | select(.kind=="user_ws_order") | .data
    | {order_id: oid(.), status: (.status // ""), ts_ms: (.ts_ms // 0)}
    | select(.order_id!=null)
  ] as $orders
  | [ .[] | select(.kind=="live_cancel") | . as $root | .data as $d
      | {order_id: oid($d), ts_ms: ts($d;$root), token_id: tok($d), side: side($d), price: $d.price} as $p
      | select(.order_id!=null and .ts_ms!=null)
      | select(([$orders[] | select(.order_id==$p.order_id and is_close(.status) and (.ts_ms >= $p.ts_ms) and (.ts_ms - $p.ts_ms) <= 5000)] | length) == 0)
    ]
  | if length>0 then .[0] else empty end
' "${JSONL}" 2>/dev/null)
if [[ -n "${cancel_missing_close}" ]]; then
  echo "FAIL: live_cancel missing user_ws_order close within 5s: ${cancel_missing_close}"
  fail=$((fail+1))
else
  echo "PASS: live_cancel user_ws_order close within 5s"
fi

# Invariant A: max one active order per (token_id, side) from user_ws_order stream
max_active_orders=$(jq -sr '
  def key($d):
    ($d.token_id // "") + "|" + (($d.side // "") | tostring | ascii_downcase);
  def is_open($s):
    ($s | tostring | ascii_downcase) as $x
    | ($x=="open" or $x=="accepted" or $x=="working" or $x=="new");
  def is_close($s):
    ($s | tostring | ascii_downcase) as $x
    | ($x=="canceled" or $x=="cancelled" or $x=="filled" or $x=="rejected" or $x=="expired");
  [ .[] | select(.kind=="user_ws_order") | .data
    | {key:key(.), status:(.status // "")}
    | select(.key != "|")
  ]
  | reduce .[] as $e ({counts:{}, max:0};
      ($e.key) as $k
      | if $k=="" then .
        else
          (if is_open($e.status) then
             .counts[$k] = ((.counts[$k] // 0) + 1)
           elif is_close($e.status) then
             .counts[$k] = ((.counts[$k] // 0) - 1)
           else . end)
          | .counts[$k] = ((.counts[$k] // 0) | if . < 0 then 0 else . end)
          | .max = ([.max, (.counts[$k] // 0)] | max)
        end
    )
  | .max
' "${JSONL}" 2>/dev/null | tr -d '[:space:]')
max_active_orders="${max_active_orders:-0}"
if [[ "${max_active_orders}" -le 1 ]]; then
  echo "PASS: max active orders per key=${max_active_orders}"
else
  echo "FAIL: max active orders per key=${max_active_orders} (>1)"
  fail=$((fail+1))
fi

# Invariant B: replace place must have a cancel within 2s for same token/side
replace_missing_cancel=$(jq -sc '
  def side($d):
    ($d.side // $d.payload.side // $d.intent.side // "") | tostring | ascii_downcase;
  def tok($d): $d.token_id // $d.payload.token_id // $d.intent.token_id;
  def ts($d; $root): $d.ts_ms // $root.ts_ms // 0;
  def price($d): $d.price // $d.payload.price // $d.intent.price;
  def match_cancel($p; $c):
    ($c.token_id==$p.token_id) and ($c.side==$p.side)
    and (($p.ts_ms - $c.ts_ms) >= 0) and (($p.ts_ms - $c.ts_ms) <= 2000);
  [ .[] | select(.kind=="live_cancel") | . as $root | .data as $d
    | {token_id: tok($d), side: side($d), ts_ms: ts($d;$root), price: price($d)}
    | select(.token_id!=null and .side!="" and .ts_ms!=null)
  ] as $cancels
  | [ .[] | select(.kind=="live_place") | . as $root | .data as $d
      | select($d.prev_order_id!=null)
      | {token_id: tok($d), side: side($d), ts_ms: ts($d;$root), prev_order_id: $d.prev_order_id, place_price: price($d), reason: $d.reason} as $p
      | select(.token_id!=null and .side!="" and .ts_ms!=null)
      | select(([$cancels[] | select(match_cancel($p; .))] | length) == 0)
    ]
  | if length>0 then .[0] else empty end
' "${JSONL}" 2>/dev/null)
if [[ -n "${replace_missing_cancel}" ]]; then
  echo "FAIL: replace missing cancel within 2s: ${replace_missing_cancel}"
  fail=$((fail+1))
else
  echo "PASS: replace cancel within 2s"
fi
if [[ -n "${since_stats}" ]]; then
  echo "INFO: user_ws_since_connected_ms ${since_stats}"
fi

if [[ "${fail}" -gt 0 ]]; then
  echo "SUMMARY: FAIL (${fail} failed)"
  exit 1
fi

echo "SUMMARY: PASS"
