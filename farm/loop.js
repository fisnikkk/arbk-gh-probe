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
      batches: [],
      startedAt: new Date().toISOString(),
    };
  }
  return JSON.parse(fs.readFileSync(STATE_FILE, "utf8"));
}

function saveState(state) {
  state.lastUpdated = new Date().toISOString();
  fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
}

function sh(cmd, opts = {}) {
  return execSync(cmd, { stdio: ["ignore", "pipe", "pipe"], encoding: "utf8", ...opts });
}

function shLive(cmd, opts = {}) {
  return execSync(cmd, { stdio: "inherit", ...opts });
}

function triggerBatch(startId) {
  console.log(`[${new Date().toISOString().slice(11, 19)}] triggering batch starting at ${startId}...`);
  sh(`gh workflow run ${WORKFLOW} --repo ${REPO} -f start_id=${startId}`);
  // GitHub takes a moment to register the run. Poll briefly for the new run ID.
  for (let i = 0; i < 10; i++) {
    try {
      const out = sh(
        `gh run list --repo ${REPO} --workflow=${WORKFLOW} --limit 1 --json databaseId,status,createdAt`
      );
      const runs = JSON.parse(out);
      if (runs.length > 0) {
        // Check the run was created very recently (within last 60s)
        const created = new Date(runs[0].createdAt).getTime();
        if (Date.now() - created < 60000) {
          return runs[0].databaseId;
        }
      }
    } catch {}
    execSync(`node -e "setTimeout(()=>{}, 1500)"`);
  }
  throw new Error("couldn't find the new run after trigger");
}

function waitForRun(runId) {
  console.log(`  waiting for run ${runId} to finish...`);
  // gh run watch blocks until completion
  try {
    sh(`gh run watch ${runId} --repo ${REPO} --exit-status`);
    return "success";
  } catch (e) {
    // Run finished with non-zero exit — could be partial success (some matrix jobs failed)
    // That's OK, we'll still download what artifacts exist
    return "partial";
  }
}

function downloadArtifacts(runId, dir) {
  fs.mkdirSync(dir, { recursive: true });
  try {
    sh(`gh run download ${runId} --repo ${REPO} --dir "${dir}"`);
  } catch (e) {
    console.log(`  warning: artifact download had errors: ${e.message.slice(0, 100)}`);
  }
  // Count artifacts
  const subdirs = fs.readdirSync(dir, { withFileTypes: true }).filter(e => e.isDirectory());
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
  console.log("");

  let batchesRun = 0;
  const loopStart = Date.now();
  let runningInserted = 0;

  while (state.nextStartId < MAX_ID && batchesRun < maxBatches) {
    const batchStartId = state.nextStartId;
    const batchEndId = Math.min(batchStartId + BATCH_SIZE - 1, MAX_ID);
    batchesRun++;

    console.log(`\n── Batch ${batchesRun} — IDs ${batchStartId}..${batchEndId} ──`);

    let runId;
    try {
      runId = triggerBatch(batchStartId);
      console.log(`  run ${runId}`);
    } catch (e) {
      console.log(`  💥 trigger failed: ${e.message}`);
      console.log(`  sleeping 60s before retry`);
      execSync(`node -e "setTimeout(()=>{}, 60000)"`);
      continue;
    }

    const status = waitForRun(runId);
    console.log(`  run finished: ${status}`);

    const batchDir = path.join(WORK_DIR, `batch-${batchStartId}`);
    const artifactCount = downloadArtifacts(runId, batchDir);
    console.log(`  downloaded ${artifactCount} artifacts`);

    let insertResult = { inserted: 0, attempted: 0 };
    try {
      insertResult = mergeBatch(batchDir);
    } catch (e) {
      console.log(`  💥 merge failed: ${e.message}`);
    }

    // Update state
    state.nextStartId = batchEndId + 1;
    state.totalInserted += insertResult.inserted;
    state.totalAttempted += insertResult.attempted;
    state.batches.push({
      startId: batchStartId,
      endId: batchEndId,
      runId,
      status,
      artifactCount,
      recordsAttempted: insertResult.attempted,
      recordsInserted: insertResult.inserted,
      at: new Date().toISOString(),
    });
    // Keep only the last 50 batch entries in state to avoid bloat
    if (state.batches.length > 50) state.batches = state.batches.slice(-50);
    saveState(state);

    runningInserted += insertResult.inserted;
    const elapsedMin = (Date.now() - loopStart) / 60000;
    const ratePerHr = (runningInserted / elapsedMin) * 60;
    console.log(
      `  ✅ batch ${batchesRun} done: ${insertResult.inserted} new → total session: ${runningInserted} | rate: ${ratePerHr.toFixed(0)} new/hr`
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
