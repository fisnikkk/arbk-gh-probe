// Farm orchestrator — runs on your local machine.
//
// Per iteration:
//   1. Read state.json to get next_start_id
//   2. Trigger the farm-batch workflow with start_id
//   3. Wait for it to finish (gh run watch)
//   4. Download all artifacts into work/batch-<start>/
//   5. Run merge.js on that directory to upsert into PostgreSQL
//   6. Update state.json (advance next_start_id, record stats)
//   7. Loop (or stop if next_start_id > MAX_ID, or if --max-batches limit reached)
//
// Safe to Ctrl+C at any time; state is persisted after each successful batch.
// Resume: just run the script again.
//
// Prerequisites:
//   - gh CLI authenticated
//   - PostgreSQL running locally with arbk DB
//   - The workflow file is already pushed to the repo
//
// Usage:
//   node loop.js                          → run until MAX_ID, default batches
//   node loop.js --max-batches 5          → run N batches then stop
//   node loop.js --start 110350           → override starting ID (writes to state)
//   node loop.js --once                   → run exactly one batch and stop

const fs = require("fs");
const path = require("path");
const { execSync, spawnSync } = require("child_process");

const REPO = "fisnikkk/arbk-gh-probe";
const WORKFLOW = "farm-batch.yml";
const BATCH_SIZE = 60; // 20 runners × 3 IDs each
const MAX_ID = parseInt(process.env.FARM_MAX_ID || "290000", 10);
const STATE_FILE = path.resolve(__dirname, "state.json");
const WORK_DIR = path.resolve(__dirname, "work");

fs.mkdirSync(WORK_DIR, { recursive: true });

function loadState() {
  if (!fs.existsSync(STATE_FILE)) {
    return {
      nextStartId: 110350,
      totalInserted: 0,
      totalAttempted: 0,
      retryQueue: [],
      batches: [],
      startedAt: new Date().toISOString(),
    };
  }
  const s = JSON.parse(fs.readFileSync(STATE_FILE, "utf8"));
  s.retryQueue = s.retryQueue || [];
  return s;
}

function claimNextRange(state) {
  // Retries take priority — they're batches that failed earlier with 0 artifacts
  if (state.retryQueue.length > 0) {
    const r = state.retryQueue.shift();
    return { startId: r.startId, endId: r.endId, isRetry: true };
  }
  if (state.nextStartId >= MAX_ID) return null;
  const startId = state.nextStartId;
  const endId = Math.min(startId + BATCH_SIZE - 1, MAX_ID);
  return { startId, endId, isRetry: false };
}

function saveState(state) {
  state.lastUpdated = new Date().toISOString();
  fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
}

function sh(cmd, opts = {}) {
  // Default 20s timeout so a hung gh CLI doesn't stall the whole loop.
  // Callers that need longer (download of many artifacts) can override.
  const timeoutMs = opts.timeoutMs != null ? opts.timeoutMs : 20000;
  return execSync(cmd, {
    stdio: ["ignore", "pipe", "pipe"],
    encoding: "utf8",
    timeout: timeoutMs,
    killSignal: "SIGKILL",
    ...opts,
  });
}

function sleep(ms) {
  // Busy-wait via execSync with a clean child — blocks cleanly, unlike
  // a pure setTimeout (since this is called from a sync flow).
  const end = Date.now() + ms;
  while (Date.now() < end) {
    try { execSync(`node -e "setTimeout(()=>{}, ${Math.min(ms, 1000)})"`, { timeout: ms + 5000 }); break; }
    catch { /* shouldn't happen but be safe */ break; }
  }
}

function triggerBatch(startId) {
  console.log(`[${new Date().toISOString().slice(11, 19)}] triggering batch starting at ${startId}...`);
  sh(`gh workflow run ${WORKFLOW} --repo ${REPO} -f start_id=${startId}`, { timeoutMs: 30000 });
  for (let i = 0; i < 10; i++) {
    try {
      const out = sh(
        `gh run list --repo ${REPO} --workflow=${WORKFLOW} --limit 1 --json databaseId,status,createdAt`,
        { timeoutMs: 15000 }
      );
      const runs = JSON.parse(out);
      if (runs.length > 0) {
        const created = new Date(runs[0].createdAt).getTime();
        if (Date.now() - created < 120000) {
          return runs[0].databaseId;
        }
      }
    } catch {}
    sleep(1500);
  }
  throw new Error("couldn't find the new run after trigger");
}

function waitForRun(runId) {
  console.log(`  waiting for run ${runId} to finish...`);
  // Poll with gh api (lower-level, fewer moving parts than gh run view).
  const maxWaitSec = 600; // 10 min max per batch
  const startedAt = Date.now();
  let errorCount = 0;
  while (Date.now() - startedAt < maxWaitSec * 1000) {
    try {
      const out = sh(
        `gh api repos/${REPO}/actions/runs/${runId} --jq "{status,conclusion}"`,
        { timeoutMs: 15000 }
      );
      const r = JSON.parse(out);
      if (r.status === "completed") {
        return r.conclusion || "unknown";
      }
      errorCount = 0; // reset on good poll
    } catch (e) {
      errorCount++;
      if (errorCount <= 3 || errorCount % 10 === 0) {
        console.log(`  (poll error ${errorCount}: ${e.message.slice(0, 60)})`);
      }
      // If we've hit 20 consecutive poll errors, give up on this run
      if (errorCount >= 20) {
        console.log(`  too many poll errors — giving up on ${runId}`);
        return "poll-exhausted";
      }
    }
    sleep(5000);
  }
  return "timeout";
}

function downloadArtifacts(runId, dir) {
  fs.mkdirSync(dir, { recursive: true });
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      sh(`gh run download ${runId} --repo ${REPO} --dir "${dir}"`, { timeoutMs: 120000 });
      break;
    } catch (e) {
      console.log(`  download attempt ${attempt}/3 failed: ${e.message.slice(0, 80)}`);
      if (attempt < 3) sleep(attempt * 10000); // 10s, 20s
    }
  }
  const subdirs = fs.existsSync(dir)
    ? fs.readdirSync(dir, { withFileTypes: true }).filter(e => e.isDirectory())
    : [];
  return subdirs.length;
}

function mergeBatch(dir) {
  // Run merge.js and parse its output for the insertion count
  const mergePath = path.resolve(__dirname, "merge.js");
  const out = sh(`node "${mergePath}" "${dir}"`);
  console.log(out.trim().split("\n").map(l => "    " + l).join("\n"));
  // Parse "inserted N new" from the output
  const m = out.match(/inserted (\d+) new/);
  const nRecords = out.match(/(\d+) unique records/);
  return {
    inserted: m ? parseInt(m[1], 10) : 0,
    attempted: nRecords ? parseInt(nRecords[1], 10) : 0,
  };
}

async function main() {
  const args = process.argv.slice(2);
  const maxBatchesIdx = args.indexOf("--max-batches");
  const maxBatches = maxBatchesIdx >= 0 ? parseInt(args[maxBatchesIdx + 1], 10) : Infinity;
  const once = args.includes("--once");
  const startIdx = args.indexOf("--start");

  let state = loadState();
  if (startIdx >= 0) {
    state.nextStartId = parseInt(args[startIdx + 1], 10);
    saveState(state);
    console.log(`override: next_start_id = ${state.nextStartId}`);
  }

  console.log("═══════════════════════════════════════════════════════════════");
  console.log("  ARBK FARM LOOP");
  console.log("═══════════════════════════════════════════════════════════════");
  console.log(`  repo:         ${REPO}`);
  console.log(`  workflow:     ${WORKFLOW}`);
  console.log(`  batch size:   ${BATCH_SIZE} IDs (20 runners × 3 IDs)`);
  console.log(`  next_start:   ${state.nextStartId}`);
  console.log(`  max_id:       ${MAX_ID}`);
  console.log(`  total so far: ${state.totalInserted} inserted`);
  console.log(`  batches:      ${state.batches.length} completed`);
  console.log(`  retry queue:  ${state.retryQueue.length} pending`);
  console.log("");

  let batchesRun = 0;
  const loopStart = Date.now();
  let runningInserted = 0;

  while (batchesRun < maxBatches) {
    const claim = claimNextRange(state);
    if (!claim) break;
    const { startId: batchStartId, endId: batchEndId, isRetry } = claim;
    batchesRun++;

    const tag = isRetry ? "RETRY" : "new";
    console.log(`\n── Batch ${batchesRun} [${tag}] — IDs ${batchStartId}..${batchEndId} ──`);

    let runId;
    try {
      runId = triggerBatch(batchStartId);
      console.log(`  run ${runId}`);
    } catch (e) {
      console.log(`  💥 trigger failed: ${e.message.slice(0, 150)}`);
      // put the range back in the retry queue so we don't lose it
      state.retryQueue.push({ startId: batchStartId, endId: batchEndId, lastErr: "trigger: " + e.message.slice(0, 80) });
      if (!isRetry) state.nextStartId = batchEndId + 1;
      saveState(state);
      console.log(`  sleeping 60s before next batch`);
      sleep(60000);
      continue;
    }

    const conclusion = waitForRun(runId);
    console.log(`  run finished: ${conclusion}`);

    const batchDir = path.join(WORK_DIR, `batch-${batchStartId}`);
    // Clear any previous attempt directory so we don't count stale files
    if (fs.existsSync(batchDir)) fs.rmSync(batchDir, { recursive: true, force: true });
    const artifactCount = downloadArtifacts(runId, batchDir);
    console.log(`  downloaded ${artifactCount} artifacts`);

    // CRITICAL: if nothing was downloaded, DON'T advance state.nextStartId.
    // Push the range into the retry queue to re-attempt later.
    if (artifactCount === 0) {
      console.log(`  ⚠️  no artifacts downloaded — queuing for retry`);
      state.retryQueue.push({
        startId: batchStartId,
        endId: batchEndId,
        runId,
        lastErr: "download returned 0 artifacts",
      });
      if (!isRetry) state.nextStartId = batchEndId + 1;
      saveState(state);
      continue;
    }

    let insertResult = { inserted: 0, attempted: 0 };
    try {
      insertResult = mergeBatch(batchDir);
    } catch (e) {
      console.log(`  💥 merge failed: ${e.message.slice(0, 150)}`);
    }

    // Success — update state and advance cursor (if not retry)
    if (!isRetry) state.nextStartId = batchEndId + 1;
    state.totalInserted += insertResult.inserted;
    state.totalAttempted += insertResult.attempted;
    state.batches.push({
      startId: batchStartId,
      endId: batchEndId,
      runId,
      status: conclusion,
      artifactCount,
      recordsAttempted: insertResult.attempted,
      recordsInserted: insertResult.inserted,
      isRetry,
      at: new Date().toISOString(),
    });
    if (state.batches.length > 50) state.batches = state.batches.slice(-50);
    saveState(state);

    runningInserted += insertResult.inserted;
    const elapsedMin = (Date.now() - loopStart) / 60000;
    const ratePerHr = (runningInserted / elapsedMin) * 60;
    console.log(
      `  ✅ batch ${batchesRun} done: ${insertResult.inserted} new → total session: ${runningInserted} | rate: ${ratePerHr.toFixed(0)} new/hr | retry queue: ${state.retryQueue.length}`
    );

    if (once) break;
  }

  console.log(`\n═══════════════════════════════════════════════════════════════`);
  console.log(`  loop finished: ${batchesRun} batches, ${runningInserted} new records`);
  console.log(`  next_start_id now ${state.nextStartId}`);
  console.log(`═══════════════════════════════════════════════════════════════`);
}

main().catch((e) => {
  console.error("FATAL:", e);
  process.exit(1);
});
