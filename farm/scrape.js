// Farm scraper — runs on a single GitHub Actions runner.
//
// Reads START_ID and END_ID from env, walks the ID range, solves a fresh Turnstile
// token for each call via the locally-running cf-clearance-scraper, hits ARBK's
// TeDhenatBiznesit, saves any returned business record to /tmp/out/records.jsonl.
// Writes a summary to /tmp/out/summary.json.
//
// Safe to fail — the summary file captures stats regardless, and partial JSONL
// is still valid and mergeable.

const fs = require("fs");
const path = require("path");
const axios = require("axios");
const CryptoJS = require("crypto-js");

const BASE_URL = "https://arbk.rks-gov.net";
const AES_KEY = "8056483646328769";
const SITE_KEY = "0x4AAAAAACZtiLmEmN3oaQNR";
const TOKEN_URL = "http://localhost:3000/cf-clearance-scraper";

const START_ID = parseInt(process.env.START_ID || "110200", 10);
const END_ID = parseInt(process.env.END_ID || "110219", 10);
const MAX_ERRORS = parseInt(process.env.MAX_ERRORS || "20", 10);
const DELAY_MS = parseInt(process.env.DELAY_MS || "0", 10);
const OUT_DIR = process.env.OUT_DIR || "/tmp/out";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

fs.mkdirSync(OUT_DIR, { recursive: true });
const RECORDS_FILE = path.join(OUT_DIR, "records.jsonl");
const SUMMARY_FILE = path.join(OUT_DIR, "summary.json");

const stats = {
  startId: START_ID,
  endId: END_ID,
  startedAt: new Date().toISOString(),
  attempted: 0,
  solveOk: 0,
  solveFail: 0,
  fetch200: 0,
  fetch401: 0,
  fetch500: 0,
  fetchErr: 0,
  recordsSaved: 0,
  consecErrors: 0,
  abortedEarly: false,
  firstFewErrors: [],
};

async function getToken() {
  const { data } = await axios.post(
    TOKEN_URL,
    { url: `${BASE_URL}/TableSearch`, siteKey: SITE_KEY, mode: "turnstile-min" },
    { timeout: 90000, validateStatus: () => true }
  );
  if (!data?.token) throw new Error("solver returned no token: " + JSON.stringify(data).slice(0, 150));
  return data.token;
}

async function getDate() {
  const { data } = await axios.get(`${BASE_URL}/api/api/Home/GetDate`, { timeout: 10000 });
  return data;
}

function makeKeyHeader(dateStr) {
  const k = CryptoJS.enc.Utf8.parse(AES_KEY);
  return CryptoJS.AES.encrypt(dateStr, k, {
    iv: k, mode: CryptoJS.mode.CBC, padding: CryptoJS.pad.Pkcs7,
  }).toString();
}

async function fetchBusiness(id, token, key) {
  const url = `${BASE_URL}/api/api/Services/TeDhenatBiznesit?nRegjistriId=${id}&Gjuha=en&token=${encodeURIComponent(token)}`;
  const r = await axios.get(url, { headers: { key }, timeout: 20000, validateStatus: () => true });
  return { status: r.status, data: r.data };
}

function writeSummary() {
  stats.finishedAt = new Date().toISOString();
  fs.writeFileSync(SUMMARY_FILE, JSON.stringify(stats, null, 2));
}

async function main() {
  console.log(`=== Farm scrape: IDs ${START_ID}..${END_ID} ===`);

  // Refresh key header periodically (the AES encrypt is fast but we fetch date from API)
  let cachedKey = null;
  let cachedKeyUntil = 0;
  async function getKey() {
    const now = Date.now();
    if (cachedKey && now < cachedKeyUntil) return cachedKey;
    cachedKey = makeKeyHeader(await getDate());
    cachedKeyUntil = now + 25000;
    return cachedKey;
  }

  try {
    for (let id = START_ID; id <= END_ID; id++) {
      stats.attempted++;
      try {
        const token = await getToken();
        stats.solveOk++;
        const key = await getKey();
        const r = await fetchBusiness(id, token, key);

        if (r.status === 200 && Array.isArray(r.data) && r.data[0]?.teDhenatBiznesit?.nRegjistriID) {
          const biz = r.data[0].teDhenatBiznesit;
          fs.appendFileSync(RECORDS_FILE, JSON.stringify(biz) + "\n");
          stats.fetch200++;
          stats.recordsSaved++;
          stats.consecErrors = 0;
          console.log(`  ✓ ${id}: ${biz.EmriBiznesit}`);
        } else if (r.status === 500) {
          // ID doesn't exist in the registry
          stats.fetch500++;
          stats.consecErrors = 0;
          console.log(`  - ${id}: (empty)`);
        } else if (r.status === 401) {
          stats.fetch401++;
          stats.consecErrors++;
          if (stats.firstFewErrors.length < 5) stats.firstFewErrors.push(`${id}: 401 ${JSON.stringify(r.data).slice(0, 100)}`);
          console.log(`  ! ${id}: 401`);
        } else {
          stats.fetchErr++;
          stats.consecErrors++;
          if (stats.firstFewErrors.length < 5) stats.firstFewErrors.push(`${id}: ${r.status}`);
          console.log(`  ? ${id}: ${r.status}`);
        }
      } catch (e) {
        stats.solveFail++;
        stats.consecErrors++;
        if (stats.firstFewErrors.length < 5) stats.firstFewErrors.push(`${id}: ${e.message.slice(0, 100)}`);
        console.log(`  💥 ${id}: ${e.message.slice(0, 80)}`);
      }

      if (stats.consecErrors >= MAX_ERRORS) {
        console.log(`\n  ❌ aborting early: ${stats.consecErrors} consecutive errors`);
        stats.abortedEarly = true;
        break;
      }

      if (DELAY_MS > 0 && id < END_ID) await sleep(DELAY_MS);
    }
  } finally {
    writeSummary();
  }

  console.log("\n=== SUMMARY ===");
  console.log(JSON.stringify(stats, null, 2));
  console.log(`\nrecords saved to ${RECORDS_FILE}`);
  console.log(`summary saved to ${SUMMARY_FILE}`);
}

main().catch((e) => {
  console.error("FATAL:", e.message);
  stats.abortedEarly = true;
  stats.firstFewErrors.push("FATAL: " + e.message);
  writeSummary();
  process.exit(1);
});
