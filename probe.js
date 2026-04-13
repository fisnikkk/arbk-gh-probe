// End-to-end probe: solve one Turnstile token locally, call ARBK with it, report.
// Runs after the token server has already been started in the workflow.

const axios = require("/tmp/cf/node_modules/axios");
const CryptoJS = require("/tmp/cf/node_modules/crypto-js");

const BASE_URL = "https://arbk.rks-gov.net";
const AES_KEY = "8056483646328769";
const SITE_KEY = "0x4AAAAAACZtiLmEmN3oaQNR";

async function getDate() {
  const { data } = await axios.get(`${BASE_URL}/api/api/Home/GetDate`, { timeout: 15000 });
  return data;
}

function makeKeyHeader(dateStr) {
  const k = CryptoJS.enc.Utf8.parse(AES_KEY);
  return CryptoJS.AES.encrypt(dateStr, k, {
    iv: k, mode: CryptoJS.mode.CBC, padding: CryptoJS.pad.Pkcs7,
  }).toString();
}

async function getToken() {
  const t0 = Date.now();
  const { data } = await axios.post(
    "http://localhost:3000/cf-clearance-scraper",
    { url: `${BASE_URL}/TableSearch`, siteKey: SITE_KEY, mode: "turnstile-min" },
    { timeout: 120000 }
  );
  console.log(`solve: ${Date.now() - t0}ms -> ${data.code} ${data.token ? data.token.slice(0, 25) + "..." : "(no token) " + JSON.stringify(data).slice(0, 150)}`);
  if (!data.token) throw new Error("no token from solver: " + JSON.stringify(data).slice(0, 200));
  return data.token;
}

async function callArbk(id, token, key) {
  const url = `${BASE_URL}/api/api/Services/TeDhenatBiznesit?nRegjistriId=${id}&Gjuha=en&token=${encodeURIComponent(token)}`;
  const t0 = Date.now();
  const r = await axios.get(url, {
    headers: { key },
    timeout: 20000,
    validateStatus: () => true,
  });
  const ms = Date.now() - t0;
  const name = r.data?.[0]?.teDhenatBiznesit?.EmriBiznesit;
  return { status: r.status, ms, name, body: r.data };
}

async function main() {
  console.log("=== ARBK GitHub Actions probe ===");

  // Runner IP (for the record)
  try {
    const ip = (await axios.get("https://api.ipify.org", { timeout: 5000 })).data;
    console.log("runner public IP:", ip);
  } catch (e) {
    console.log("couldn't determine runner IP:", e.message);
  }

  // 1. Basic reachability: GetDate is public, no auth needed
  console.log("\n--- 1. reachability check (GetDate, no auth) ---");
  try {
    const d = await getDate();
    console.log("GetDate ok:", d);
  } catch (e) {
    console.log("GetDate FAILED:", e.message);
    console.log("  -> ARBK is not reachable from this runner at all. Aborting.");
    process.exit(1);
  }

  // 2. Solve a token
  console.log("\n--- 2. solve a Turnstile token ---");
  let token;
  try {
    token = await getToken();
  } catch (e) {
    console.log("token solve FAILED:", e.message);
    process.exit(2);
  }

  // 3. Call ARBK with the token for a known existing ID
  console.log("\n--- 3. call TeDhenatBiznesit?nRegjistriId=1 with solved token ---");
  const key = makeKeyHeader(await getDate());
  const r = await callArbk(1, token, key);
  console.log(`status=${r.status} ms=${r.ms}`);
  if (r.status === 200 && r.name) {
    console.log(`\n✅ SUCCESS: ARBK accepted the token and returned business data`);
    console.log(`   ID 1 = "${r.name}"`);
    console.log(`   GitHub Actions runner IP is NOT blocked by ARBK`);
    console.log(`   The distributed solver-farm approach is VIABLE`);
    process.exit(0);
  } else {
    console.log(`\n❌ FAILED: status=${r.status}`);
    console.log(`   body: ${JSON.stringify(r.body).slice(0, 500)}`);
    if (r.status === 401) {
      console.log(`   -> Runner IP is blocked OR token was rejected. Distributed farm may not work.`);
    }
    process.exit(3);
  }
}

main().catch((e) => { console.error("FATAL:", e.message); process.exit(99); });
