# Gateway Layer Design — B1 Transaction Fetch

A design exploration for an HA, fault-tolerant, paginated gateway layer over a bulkheaded service architecture using CockroachDB.

---

## 1. Starting Architecture

The system has an `app domain` containing:

- **UI** → **gateways** → **router** as the entry chain
- Three **bulkheads**, each containing the same internal pattern: `envoy → service → txn/data`
- Two logical **branches** (B1, B2) of execution, with events distributed across bulkheads:
  - **Bulkhead 1** — handles `e1, e2` for branch B1; `e3` for branch B2
  - **Bulkhead 2** — handles `e3, d4, e5` for branch B1
  - **Bulkhead 3** — handles `e1, e2` for branch B2

Bulkheads provide failure isolation: one compartment failing doesn't cascade.

The `e1`/`e2`/`e3`/`d4` labels likely refer to event types or saga steps in a distributed workflow.

---

## 2. Problem Statement

UI requests: *"fetch all txn data for branch B1"*.

Branch B1's data is split across **Bulkhead 1** (e1, e2) and **Bulkhead 2** (e3, d4, e5) — two independent failure domains.

Design goals:

- High availability (HA)
- Fault tolerance
- Resilience to bulkhead failure
- Pagination support for the UI

---

## 3. First Design — Cursor-Based Scatter-Gather

### Gateway layer structure

- Two **stateless** gateway nodes behind a load balancer (active/active)
- LB does health checks; either node serves any request
- Shared **Redis** stores cursor state and a short-TTL response cache
- Each gateway node has independent **circuit breakers** per bulkhead

### Request flow

1. UI sends `GET /txn?branch=B1&page_token=T&limit=50`
2. Gateway resolves cursor `T` from Redis → `(BH1_offset, BH2_offset)`
3. Cache check; return early on hit
4. Scatter-gather to BH1 and BH2 in parallel (timeout = 500ms)
5. Merge results sorted by `txn_time`, advance cursor, write `T+1` to Redis
6. Return response with `data`, `next_page_token`, `partial` flag, `sources`

### Fault tolerance — the partial result pattern

- Both bulkheads respond → full page
- One down → return partial page with `"partial": true` and `"sources": ["BH1"]`
- Both down → cache fallback or 503 with `Retry-After`

### Pagination — compound cursors over offsets

Cursor `T` encodes `{ bh1_offset, bh2_offset, sort_key }` (opaque, in Redis):

- Each bulkhead queried independently with its own offset
- Merge is deterministic and stable
- Failed bulkhead recovery: cursor remembers what was already fetched

### Resilience checklist

- 500ms scatter-gather timeout
- 30s cache TTL on read-heavy pages
- Redis HA via replica + sentinel or cluster
- Bulkhead health endpoint polled on startup

---

## 4. Drawbacks of First Design

1. **Read skew across pages** — page 1 at t0, page 2 at t0+30s; new txns inserted between cause duplicates or gaps. No consistent snapshot.
2. **Partial result resumability is fragile** — if BH2 fails on page 1, cursor has no BH2 offset. On retry, BH2 restarts from beginning → possible duplicates.
3. **Merge-sort needs over-fetch but didn't compute it** — to return 50 sorted, may need 50+50 from each bulkhead. No over-fetch factor → wrong order at boundaries.
4. **Redis cursor store is a SPOF for navigation** — Redis down = pagination dead. Cursor should be self-contained.
5. **Unbounded deep pagination** — user can paginate forever, holding open snapshots. Cockroach GC pressure, memory leak for stale cursors.
6. **No schema-level idempotency for retries** — retried calls can return slightly different data due to ongoing writes.

---

## 5. Redesign — MVCC Snapshot + Bounded Pagination

### Constraints introduced

- `page_size = 10`
- `over_fetch_factor = 2` → each bulkhead fetches 20, gateway merges and trims to 10
- `max_pages = 2` → after page 2, snapshot invalidated; UI must hard-reset
- Use CockroachDB MVCC via `AS OF SYSTEM TIME`

### Core insight

Every CockroachDB row has `crdb_internal_mvcc_timestamp`. Queries with `AS OF SYSTEM TIME 'snap_ts'` read a consistent point-in-time view. Both bulkhead queries read at the same logical timestamp — solves drawbacks 1, 2, 6 simultaneously.

### Flow

1. UI sends `GET /txn?branch=B1&cursor=C&size=10`
2. Decode cursor: `{ snap_ts, bh1_after, bh2_after, page_num, hmac }`
   - Verify HMAC
   - Check `page_num ≤ 2`
   - Check `snap_ts` age
3. **Page 1 path**: pin snapshot `snap_ts = cluster_logical_timestamp()`, embed in cursor
4. **Page > 2 OR snap_ts > 30s old**: return `410 Gone` + `reset_required: true`
5. Scatter — parallel AOST queries with `LIMIT 20` each:
   ```sql
   SELECT … AS OF SYSTEM TIME snap_ts
   WHERE id > bh_after
   ORDER BY (txn_time, id)
   LIMIT 20
   ```
6. K-way merge → trim to 10. Set `bh_after` to highest `(txn_time, id)` of rows **actually returned** from that bulkhead, not the highest fetched.
7. Build next cursor + response. If `page_num == 2`: `next_cursor = null`, hint reset.

### Why over-fetch = 2

- To return 10 globally-sorted rows from 2 bulkheads, fetching 10 from each is unsafe — worst case all 10 winners come from one bulkhead.
- Over-fetch = 2 gives the gateway a buffer for skewed distributions across bulkheads without immediate re-querying.
- Trade-off: 2× DB load and network per page.
- Correctness preserved by advancing `bh_after` only past returned rows, not over-fetched ones.

### Why hard reset after page 2

1. **MVCC GC** — holding AOST against an old snapshot blocks GC for that range; bloats LSM. 2-page cap means snapshot lives ~30s tops.
2. **Snapshot staleness UX** — after 20 frozen rows, anything new is invisible. Forced reset gives a fresh snapshot.
3. **Bounded resource cost** — cursor state, retry budget, circuit breaker windows all capped.

### New cursor structure

```
cursor = base64(hmac_signed({
  snap_ts:   "1735000000.0000000001",
  bh1_after: { txn_time, id },
  bh2_after: { txn_time, id },
  page_num:  1,
  branch:    "B1",
  exp:       <issue_ts + 30s>
}))
```

Self-contained. Redis no longer required for navigation — only optional response cache. HMAC prevents tampering. `exp` enforces freshness independent of page count.

---

## 6. Drawbacks of MVCC Redesign

1. **Partial failure now corrupts the snapshot contract** — bulkhead pattern collides with snapshot pattern. Either fail whole page (lose graceful degradation) or return BH1-only and risk later "time travel" rows from BH2.
2. **`snap_ts` can be GC'd mid-pagination** — page 1 succeeds, user idles, page 2 fails: *"batch timestamp must be after replica GC threshold"*.
3. **Follower reads break with very recent writes** — `follower_read_timestamp()` is ~4.8s in the past. A txn committed 2s ago appears as a "ghost write" → user thinks app dropped their write.
4. **Over-fetch wastes 50% of DB work on average** — 10 of 20 fetched rows discarded per bulkhead. 2× index reads, network, tail latency. Insufficient for highly uneven distributions (e.g. 90/10).
5. **Hard reset at page 2 is hostile UX** — "all txns last quarter" forces resume-from-here logic anyway. Also: 20-row ceiling makes export/CSV impossible through this API.
6. **Cursor signing key rotation is painful** — HMAC rotation invalidates in-flight cursors. Requires kid-tagged keys, overlap windows.
7. **Tie-breaking on `(txn_time, id)` is dataset-specific** — cross-bulkhead ID collisions break monotonicity. Need composite key `(bh_id, txn_time, id)`.
8. **Observability gap** — `410 Gone` from snapshot expiry vs end-of-pagination conflated. Need `reset_reason`: `"expired"` vs `"limit"` vs `"completed"`.
9. **AOST + hot ranges = follower lag amplification** — bursty writes cause lag spikes; page 1 fails unpredictably.

### Two most dangerous, expanded

**Drawback 1 — partial-failure regression.** First design degraded gracefully ("show BH1, flag partial"). MVCC version cannot, because both must read at `snap_ts`. If BH1-only returned on page 1 and BH2 recovers by page 2, BH2 rows at `snap_ts` may have timestamps interleaved with already-shown BH1 rows — looks like time travel.

**Drawback 3 — follower-read ghost writes.** User commits at t=0, requests list at t=2; follower reads at t≈−4.8s miss the brand-new txn. User retries write (creates duplicate) or panics. Mitigation: route reads to leaseholder when session has recent writes from this user.

---

## 7. Redesign Direction Chosen — Fix Partial Failure with Snapshots

### The core tension

- **Bulkhead pattern**: one component's failure must not fail the whole request
- **MVCC snapshot**: all reads happen at the same logical instant

Honor snapshot → fail whole page (bulkhead broken). Honor bulkhead → return BH1-only, BH2 rows later look out-of-order (snapshot broken). Neither acceptable.

### The fix — explicit gaps with snapshot continuation

Introduce a **third state**: explicit gaps in the result stream. Snapshot held open across pages. BH2 retried on subsequent pages within the same snapshot window. Failure is *represented*, not hidden.

### Per-bulkhead state in the cursor

```
cursor = base64(hmac_signed({
  snap_ts: "1735000000.0000000001",
  exp: <issue_ts + 30s>,
  page_num: 2,
  bulkheads: {
    BH1: { state: "ACTIVE",      after: { txn_time, id }, retries: 0 },
    BH2: { state: "GAP_PENDING", last_known_watermark, retries: 1, gap_count: 3 }
  }
}))
```

States: `ACTIVE`, `GAP_PENDING`, `COMPLETE`. Each bulkhead has its own lifecycle within a session.

### The watermark — the trick that makes it correct

On each page, compute:

```
watermark = min(last_seen_ts of each non-COMPLETE bulkhead)
```

Only emit rows with `txn_time ≤ watermark`. Others held back for next page. Guarantees no later page can produce a row earlier than what was shown.

When BH2 fails: BH2's watermark is unknown → could be anything ≥ `snap_ts_start` → cannot safely emit any BH1 row past the lowest possible BH2 timestamp → emit zero rows, mark `gap_pending`.

### The watermark probe — making it not-zero

Even on partial failure, ask BH2 for a cheap point query:

```sql
SELECT min(txn_time)
FROM ... AS OF SYSTEM TIME snap_ts
WHERE id > bh2_after
```

If BH2 can answer this much, you get a watermark and can emit BH1 rows up to it. Two-phase recovery: cheap watermark probe, expensive page fetch.

### Flow

1. Decode cursor with per-bulkhead state
2. Scatter to `ACTIVE` and `GAP_PENDING` bulkheads only (skip `COMPLETE`)
3. Classify each bulkhead independently:
   - success → `ACTIVE`
   - rows exhausted → `COMPLETE`
   - timeout / 5xx → `GAP_PENDING` (snapshot held; retry next page)
4. K-way merge with watermark — emit only rows ≤ watermark
5. Build response based on overall state:
   - all `ACTIVE` → emit data + `next_cursor`
   - any `GAP_PENDING` → emit data so far + `gap_marker[BH2]` + `retry_after`
   - all `COMPLETE` → `next_cursor = null`, end of stream
6. Escape hatch: gap stays unrecovered after K retries → finalize with explicit "data missing" range, mark BH2 `COMPLETE`

### What the UI sees

```json
{
  "data": [10 transactions],
  "next_cursor": "...",
  "partial": true,
  "gaps": [
    { "bulkhead": "BH2", "estimated_missing": 3, "retry_after_ms": 2000 }
  ],
  "watermark": "2024-01-15T10:23:00Z",
  "page_num": 2
}
```

UI renders: 10 rows, then a "3 transactions from one source temporarily unavailable" banner with Retry. On retry, gateway re-attempts BH2 within same snapshot. Missing rows fill in *above* the watermark on subsequent pages — never out of order.

### Why both patterns are honored

- **Bulkhead**: BH2 failure doesn't block BH1; rows flow up to watermark
- **Snapshot**: every shown row came from `snap_ts`; no row appears out of temporal order
- **Bulkhead again** (escape hatch): if BH2 stays dead K retries, accept permanent data loss for this snapshot rather than blocking forever

### Costs

- Watermark probe = per-bulkhead extra round-trip per page
- Cursor grows ~200B → ~400B
- UI complexity: gap states, retry button behavior — real product work

---

## 8. Drawbacks of Gap-Aware Design

1. **Watermark stall — slowest bulkhead caps progress.** If BH2's watermark is `t=10` and BH1 has 50 rows past `t=10`, user gets ~1 row per page until BH2 catches up. Looks like rate-limiting; pagination effectively broken from user perspective.
2. **Retry storms within snapshot window.** User mashes retry, multiple tabs open, polling refresh — all retries hit BH2 with same `snap_ts` (no caching benefit). Circuit breaker may stay open, perpetually `GAP_PENDING`.
3. **Probe + fetch = 2× request multiplier.** Every page now does watermark probe THEN main fetch. 2 RTTs per bulkhead per page, no batching across pages. Latency budget shrinks; tail latency dominated by slowest probe.
4. **UI complexity grew significantly.** Must render gap markers, retry button, partial state, watermark indicator. Virtual scrolling now interleaves real rows with placeholder gaps. Accessibility / screen reader semantics non-trivial.
5. **Gap finalization is data loss without escalation.** After K retries, marking BH2 `COMPLETE` silently drops rows. No audit trail, no compliance signal, no operator alert. User sees "complete" page that's actually permanently incomplete.
6. **Testing matrix exploded.** 3 states per bulkhead × N bulkheads × M pages = combinatorial explosion. Flaky bulkheads (success-fail-success) hard to reproduce. Need deterministic chaos harness, fault injection at envoy layer.
7. **Cursor leaks operational state to client.** `retries`, `gap_count`, `watermark` visible if HMAC reversed — cursor is signed, not encrypted. Attacker can craft cursors with low retry counts to evade rate-limiting. Need AES-GCM encryption on cursor body, not just HMAC.
8. **Watermark assumes monotonic `txn_time` across bulkheads.** If BH2 timestamps drift (clock skew, replay) watermark is wrong. CockroachDB's HLC bounds skew but cross-region writes can surprise. `MIN(txn_time)` is a moving target during burst writes.
9. **Retry budget shared with snapshot lifetime.** K=3 retries × 5s timeout each can exhaust 30s `snap_ts` window before page 2 even completes.

### Most damaging

**#1 (watermark stall)** is a correctness-vs-progress bind: the very mechanism that prevents time-travel rows is what causes the stall.

**#5 (silent data loss)** is a compliance/audit problem disguised as a UX choice. Without escalation hooks (operator alerts, audit log entries, user-visible confirmation of permanent loss), this design can lose customer data invisibly.

---

## 9. Watermark Probe — Mechanics, Cost, Failure Modes

### Phases per page

**Phase 1 — probe (cheap, parallel, 100ms timeout)**

Each bulkhead runs:
```sql
SELECT MIN(txn_time)
FROM txn
AS OF SYSTEM TIME snap_ts
WHERE id > bh_after AND branch = 'B1'
```

Index-only scan on `(branch, id)`. Single key lookup ≈ 1ms when warm.

**Phase 2 — compute global watermark**

```
watermark = min(probe_result of each non-COMPLETE bulkhead)
```

- Any probe failed → conservative watermark = `bh_after` of failed BH (no progress this page)
- All probes returned NULL → that BH is `COMPLETE` for this snapshot

**Phase 3 — bounded fetch (expensive, parallel, 500ms timeout)**

```sql
SELECT *
FROM txn
AS OF SYSTEM TIME snap_ts
WHERE id > bh_after AND txn_time <= watermark
ORDER BY (txn_time, id)
LIMIT 20
```

Watermark cap means no row can land outside the safe window.

**Phase 4 — merge, trim to 10, advance `bh_after`**

Advance only past returned rows. Over-fetched rows discarded.

### Cost model

In steady state: 4 round-trips per page (probe ×2 + fetch ×2 in parallel pairs). Wall-clock dominated by parallelism:
- Worst case: probe_RTT + fetch_RTT ≈ 100ms + 500ms = **600ms**
- Warm case: ~10ms + 50ms = **60ms**

Real amplification but not catastrophic. The 2× DB query count per page is the more durable concern — multiplied by user concurrency, this matters at scale.

### Probe failure modes

- **Probe times out** → conservative watermark = `bh_after` → no progress this page; user sees zero-row page
- **Probe succeeds but fetch fails** → had a watermark, partial result still possible (fall back to gap-marker path)
- **Probe returns lower value than expected** → watermark regresses → re-emit risk on retries
- **Probe returns NULL but bulkhead has uncommitted rows below `snap_ts`** → false `COMPLETE` classification; rows permanently invisible in this session
- **Probe latency > budget** (clock skew, slow follower replica) → probe becomes dominant page latency, defeats the "cheap" rationale

### The probe-regression failure (subtle)

If a write commits at `txn_time = t=8` after `snap_ts = t=10`, AOST query won't see it (correct). But if a *concurrent* operation also inserts a row at `t=11` and the probe runs between those events, watermark values can disagree across retries within the same snapshot.

CockroachDB's HLC mostly prevents this, but the abstraction is leaky under high write contention. Mitigation: cache probe result per `(snap_ts, bh_after)` for 1-2 seconds within the gateway — successive retries get the same answer.

---

## 10. Both Bulkheads Fail Simultaneously

The gap-marker pattern was designed for *partial* failure. When both fail, there's no anchor for a watermark — nothing to "gap" against. Don't pretend graceful recovery when no data source is available.

### Decision tree

**Page 1 (no cursor)** → return `503` + `Retry-After`. No useful state to preserve.

**Page N with valid cursor** → try response cache keyed by `(snap_ts, page_n)`:
- **Cache hit** → return cached page with `"stale_cache": true` marker. Do **not** advance cursor; user must retry to make real progress.
- **Cache miss** → return `503` with `"degraded_mode": "no_data_available"`. Operator alert; SLO breach event.

**Snapshot expired during outage** → `410 Gone` + `reset_required: true`. Don't try to extend; force fresh snapshot on recovery.

### Why cache fallback is safe under MVCC

The cache is keyed by `snap_ts` plus pagination position. The data inside is provably immutable for that snapshot (AOST guarantee). Serving stale cache during dual outage doesn't violate the snapshot contract — the user gets exactly what they would have gotten if the bulkheads had answered.

The only catch: cursor must **not advance** on a cache-served page. Otherwise the user could paginate through cache-only data, never getting fresh reads on recovery. The `"stale_cache": true` flag tells the UI: "this is what we saved earlier, retry to confirm."

### Circuit breaker coordination

If both BHs are downstream of a shared dependency (CockroachDB cluster, shared network path), they fail and recover together. Naïve per-bulkhead retry policy causes synchronized retry storms on recovery.

Pattern:
- Both BH circuit breakers trip → gateway short-circuits new requests for the same branch
- Half-open after backoff (e.g. 5s)
- Single **canary probe** before reopening (one cheap probe to one bulkhead)
- Only when canary succeeds do other requests start flowing
- Prevents thundering herd

### Snapshot preservation across outage

If both BHs down longer than `snap_ts` age budget (30s), the snapshot is dead anyway — CockroachDB GC will reclaim it. On recovery, force fresh snapshot — no resume from old cursor.

Response should include explicit `"snapshot_age_ms"` and `"snapshot_remaining_ms"` so the UI can show honest status: *"refreshing; previous results no longer available."*

### What this design loses

This is the limit of graceful degradation. With both data sources gone and no valid cache, there is no honest answer except "retry later." Hiding this from users (showing empty results, fake placeholders, "no data found") would be worse than `503` because it conflates outage with valid empty state. **Loud failure is the correct UX here.**

---

## 11. Open Questions / Next Steps

- [ ] Encrypt cursor body (AES-GCM) instead of just HMAC signing — addresses drawback #7
- [ ] Operator alerting + audit log entries for finalized gaps — addresses drawback #5
- [ ] Adaptive over-fetch factor based on observed return-rate ratios
- [ ] Cache the probe result per `(snap_ts, bh_after)` for 1-2s — mitigates probe regression
- [ ] Define `reset_reason` taxonomy: `"expired" | "limit" | "completed" | "outage"` — addresses earlier observability gap
- [ ] Snapshot extend endpoint vs hard reset — alternative to forced reset for power users
- [ ] Separate read paths (leaseholder for fresh, follower for historical)
- [ ] Stall mitigation for drawback #1 — adaptive watermark, optimistic emission with rollback?
