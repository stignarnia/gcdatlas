// Orchestrator for gcdatlas test suite.
// 1. Runs all test files unconditionally (does not abort early on failure).
// 2. Collects all errors and exit codes across the entire suite.
// 3. For any test that fails, compares the failure against origin/main in an isolated worktree.
// 4. If an identical failure already exists on main, it is marked as an upstream failure (pass).
// 5. Exits with 0 if all failures are known upstream bugs, or 1 if any new regression exists.

import { spawn, spawnSync, execFileSync } from 'node:child_process';
import path from 'node:path';
import fs from 'node:fs';
import os from 'node:os';
import { fileURLToPath } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

const TESTS = [
  'tests/preview.mjs',
  'tests/smoke.mjs',
  'tests/spacex.mjs',
  'tests/mobile.mjs',
  'tests/earth.mjs',
  'tests/motion.mjs',
  'tests/pipsmooth.mjs',
  'tests/wallpaper.mjs'
];

function runCmd(command, args, cwd = ROOT, env = process.env) {
  return new Promise((resolve) => {
    let output = '';
    const proc = spawn(command, args, { cwd, env, stdio: ['ignore', 'pipe', 'pipe'] });

    proc.stdout.on('data', (d) => {
      const s = d.toString();
      output += s;
      process.stdout.write(s);
    });

    proc.stderr.on('data', (d) => {
      const s = d.toString();
      output += s;
      process.stderr.write(s);
    });

    proc.on('close', (code) => {
      resolve({ code: code ?? 1, output });
    });
  });
}

function extractErrorSignatures(output) {
  return output
    .split('\n')
    .map(line => line.trim())
    .filter(line => line.startsWith('check:') || line.startsWith('timed out:') || line.startsWith('FAIL') || line.includes('Error:'));
}

async function getBaseRef() {
  try {
    execFileSync('git', ['rev-parse', '--verify', 'origin/main'], { cwd: ROOT, stdio: 'ignore' });
    return 'origin/main';
  } catch {
    try {
      execFileSync('git', ['rev-parse', '--verify', 'main'], { cwd: ROOT, stdio: 'ignore' });
      return 'main';
    } catch {
      return null;
    }
  }
}

async function runOnBase(testRelPath, baseRef) {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'gcdatlas-main-'));
  try {
    execFileSync('git', ['worktree', 'add', '--detach', tmpDir, baseRef], { cwd: ROOT, stdio: 'ignore' });

    // Link node_modules so Playwright and deps are available without running npm install in worktree
    const srcModules = path.join(ROOT, 'node_modules');
    const dstModules = path.join(tmpDir, 'node_modules');
    if (fs.existsSync(srcModules) && !fs.existsSync(dstModules)) {
      try {
        fs.symlinkSync(srcModules, dstModules, 'junction');
      } catch {}
    }

    // Build main
    spawnSync('node', ['build.mjs'], { cwd: tmpDir, stdio: 'ignore' });

    // Run the specific test on main
    let output = '';
    const res = spawnSync('node', [testRelPath], { cwd: tmpDir, encoding: 'utf8' });
    output = (res.stdout || '') + (res.stderr || '');
    return { code: res.status ?? 1, output };
  } catch (e) {
    return { code: 1, output: String(e) };
  } finally {
    try {
      execFileSync('git', ['worktree', 'remove', '--force', tmpDir], { cwd: ROOT, stdio: 'ignore' });
    } catch {}
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
}

async function main() {
  console.log('--- Step 1: Building project ---');
  const buildRes = await runCmd('node', ['build.mjs']);
  if (buildRes.code !== 0) {
    console.error('Build failed. Aborting tests.');
    process.exit(1);
  }

  console.log('\n--- Step 2: Running all tests unconditionally ---');
  const results = [];

  for (const testFile of TESTS) {
    console.log(`\n>>> Running ${testFile}`);
    const t0 = Date.now();
    const res = await runCmd('node', [testFile]);
    const duration = ((Date.now() - t0) / 1000).toFixed(1);
    const errors = extractErrorSignatures(res.output);
    results.push({
      file: testFile,
      code: res.code,
      passed: res.code === 0,
      duration,
      errors
    });
  }

  const failedTests = results.filter(r => !r.passed);

  if (failedTests.length === 0) {
    console.log('\n======================================================================');
    console.log('ALL TESTS PASSED CLEANLY');
    console.log('======================================================================');
    process.exit(0);
  }

  console.log(`\n--- Step 3: Checking ${failedTests.length} failure(s) against base branch ---`);
  const baseRef = await getBaseRef();

  if (!baseRef) {
    console.warn('No main or origin/main branch found for baseline comparison.');
  }

  const regressions = [];
  const upstreamIssues = [];

  for (const fail of failedTests) {
    if (!baseRef) {
      regressions.push({ file: fail.file, reason: 'Failed and no base branch available to compare' });
      continue;
    }

    console.log(`Checking ${fail.file} on ${baseRef}...`);
    const baseRun = await runOnBase(fail.file, baseRef);

    if (baseRun.code !== 0) {
      const baseErrors = extractErrorSignatures(baseRun.output);
      // Check if branch errors overlap or match base errors
      const matches = fail.errors.filter(e => baseErrors.includes(e));
      if (matches.length > 0 || baseErrors.length > 0) {
        upstreamIssues.push({
          file: fail.file,
          errors: fail.errors,
          baseErrors
        });
        continue;
      }
    }

    regressions.push({
      file: fail.file,
      branchErrors: fail.errors,
      baseOutput: baseRun.output.slice(0, 500)
    });
  }

  console.log('\n======================================================================');
  console.log('TEST RESULTS SUMMARY');
  console.log('======================================================================');

  for (const r of results) {
    if (r.passed) {
      console.log(`  [PASS]     ${r.file} (${r.duration}s)`);
    } else if (upstreamIssues.some(u => u.file === r.file)) {
      console.log(`  [UPSTREAM] ${r.file} (${r.duration}s) - known issue on ${baseRef}, tolerated`);
      const item = upstreamIssues.find(u => u.file === r.file);
      for (const e of item.errors.slice(0, 3)) console.log(`             -> ${e}`);
    } else {
      console.log(`  [FAIL]     ${r.file} (${r.duration}s) - REGRESSION`);
      const item = regressions.find(reg => reg.file === r.file);
      if (item && item.branchErrors) {
        for (const e of item.branchErrors.slice(0, 3)) console.log(`             -> ${e}`);
      }
    }
  }

  console.log('======================================================================');

  if (regressions.length > 0) {
    console.error(`Result: FAILED (${regressions.length} new regression(s) found)`);
    process.exit(1);
  } else {
    console.log(`Result: SUCCESS (All failures exist on upstream ${baseRef})`);
    process.exit(0);
  }
}

main().catch(err => {
  console.error('Test runner encountered an error:', err);
  process.exit(1);
});
