// Merge downloaded artifact JSONL files into the main arbk PostgreSQL.
//
// Usage:
//   node merge.js <dir>
//     <dir> = path to a directory containing one or more subdirs with records.jsonl
//            (e.g., a directory you ran `gh run download <run-id>` into)
//
// Uses Prisma from C:/Users/PC/Desktop/arbk-scraper/node_modules — no separate install needed.
// Uses createMany with skipDuplicates so it's safe to run multiple times on the same data.
// The original enumerator can keep running in parallel; P2002 races are handled.

const fs = require("fs");
const path = require("path");
const { PrismaClient, Prisma } = require("C:/Users/PC/Desktop/arbk-scraper/node_modules/@prisma/client");

const prisma = new PrismaClient({
  datasources: {
    db: {
      url: "postgresql://postgres:postgres@localhost:5432/arbk?schema=public&connection_limit=5",
    },
  },
});

function mapBusiness(d) {
  return {
    nRegjistriID: d.nRegjistriID,
    nui: d.NUI || "",
    numriBiznesit: d.NumriBiznesit || "",
    numriFiskal: d.NumriFiskal || null,
    llojiBiznesit: d.LlojiBiznesit || null,
    emriBiznesit: d.EmriBiznesit || "",
    emriTregtar: d.EmriTregtar || null,
    komuna: d.Komuna || null,
    vendi: d.Vendi || null,
    adresa: d.Adresa || null,
    latitude: d.Latitude || null,
    longitude: d.Longtitude || null,
    telefoni: d.Telefoni || null,
    email: d.Email || null,
    webFaqja: d.WebFaqja || null,
    numriPunetoreve: d.NumriPunetoreve || null,
    kapitali: d.Kapitali || null,
    dataRegjistrimit: d.DataRegjistrimit || null,
    dataShuarjesBiznesit: d.DataShuarjesBiznesit || null,
    statusiARBK: d.StatusiARBK || null,
  };
}

function findRecordFiles(root) {
  const out = [];
  if (!fs.existsSync(root)) return out;
  const entries = fs.readdirSync(root, { withFileTypes: true });
  for (const e of entries) {
    const p = path.join(root, e.name);
    if (e.isDirectory()) out.push(...findRecordFiles(p));
    else if (e.name === "records.jsonl") out.push(p);
  }
  return out;
}

async function main() {
  const root = process.argv[2];
  if (!root) {
    console.error("usage: node merge.js <directory>");
    process.exit(1);
  }

  const files = findRecordFiles(root);
  if (files.length === 0) {
    console.log(`no records.jsonl files under ${root}`);
    return;
  }
  console.log(`found ${files.length} records.jsonl files under ${root}`);

  // Gather all unique records
  const seen = new Set();
  const records = [];
  let malformed = 0;
  for (const f of files) {
    const lines = fs.readFileSync(f, "utf8").trim().split("\n").filter(Boolean);
    for (const line of lines) {
      try {
        const d = JSON.parse(line);
        if (!d.nRegjistriID) continue;
        if (seen.has(d.nRegjistriID)) continue;
        seen.add(d.nRegjistriID);
        records.push(mapBusiness(d));
      } catch {
        malformed++;
      }
    }
  }
  console.log(`  ${records.length} unique records, ${malformed} malformed`);

  if (records.length === 0) {
    console.log("nothing to insert");
    return;
  }

  // Bulk insert with skipDuplicates (idempotent; races with original enumerator are safe)
  const t0 = Date.now();
  const result = await prisma.business.createMany({
    data: records,
    skipDuplicates: true,
  });
  const ms = Date.now() - t0;
  console.log(`  inserted ${result.count} new / skipped ${records.length - result.count} existing (${ms}ms)`);
  await prisma.$disconnect();
}

main().catch((e) => {
  console.error("FATAL:", e.message);
  process.exit(1);
});
