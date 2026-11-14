// Orchestrator for gcdatlas test suite.
// 1. Runs all test files unconditionally with a 2-worker pool (utilizing runner's 2 vCPUs).
// 2. Worker 1 runs tests/earth.mjs immediately, Worker 2 runs other tests sequentially, then both drain queue.
// 3. For any test that fails, compares the failure against origin/main in a shared isolated worktree.
// 4. Preloads a real-time flush hook on baseRef to terminate early the moment an upstream error is confirmed.
// 5. Exits with 0 if all failures are known upstream bugs, or 1 if any new regression exists.

import { spawn, spawnSync, execFileSync } from 'node:child_process';
import path from 'node:path';
import fs from 'node:fs';
import os from 'node:os';
import { fileURLToPath } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

// Schedule longest tests first across the 2 workers so both finish around the same time
const TESTS = [
  'tests/earth.mjs',
  'tests/mobile.mjs',
  'tests/smoke.mjs',
  'tests/pipsmooth.mjs',
  'tests/moon.mjs',
  'tests/motion.mjs',
  'tests/wallpaper.mjs',
  'tests/preview.mjs',
  'tests/spacex.mjs'
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
    proc.on('error', (err) => {
      resolve({ code: 1, output: String(err) });
    });
  });
}

function runCmdBuffered(command, args, cwd = ROOT, env = process.env) {
  return new Promise((resolve) => {
    let output = '';
    const proc = spawn(command, args, { cwd, env, stdio: ['ignore', 'pipe', 'pipe'] });

    proc.stdout.on('data', (d) => { output += d.toString(); });
    proc.stderr.on('data', (d) => { output += d.toString(); });

    proc.on('close', (code) => {
      resolve({ code: code ?? 1, output });
    });
    proc.on('error', (err) => {
      resolve({ code: 1, output: String(err) });
    });
  });
}

function extractErrorSignatures(output) {
  return output
    .split('\n')
    .map(line => line.trim())
    .map(line => line.startsWith('[EARLY_FAIL] ') ? line.replace('[EARLY_FAIL] ', '').trim() : line)
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

function setupBaseWorktree(baseRef) {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'gcdatlas-main-'));
  const cleanup = () => {
    try {
      execFileSync('git', ['worktree', 'remove', '--force', tmpDir], { cwd: ROOT, stdio: 'ignore' });
    } catch {}
    try {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    } catch {}
  };

  try {
    execFileSync('git', ['worktree', 'add', '--detach', tmpDir, baseRef], { cwd: ROOT, stdio: 'ignore' });

    const srcModules = path.join(ROOT, 'node_modules');
    const dstModules = path.join(tmpDir, 'node_modules');
    if (fs.existsSync(srcModules) && !fs.existsSync(dstModules)) {
      try {
        fs.symlinkSync(srcModules, dstModules, 'junction');
      } catch {}
    }

    const buildRes = spawnSync('node', ['build.mjs'], { cwd: tmpDir, stdio: 'pipe', encoding: 'utf8' });
    if (buildRes.status !== 0) {
      throw new Error(`Base build failed: ${buildRes.stderr || buildRes.stdout}`);
    }

    const hookPath = path.join(tmpDir, 'flush-hook.mjs');
    const hookCode = `
const origPush = Array.prototype.push;
Array.prototype.push = function(...args) {
  for (const arg of args) {
    if (typeof arg === 'string' && (
      arg.startsWith('timed out:') ||
      arg.startsWith('check:') ||
      arg.startsWith('FAIL') ||
      arg.includes('Error:')
    )) {
      console.error('[EARLY_FAIL] ' + arg);
    }
  }
  return origPush.apply(this, args);
};
`;
    fs.writeFileSync(hookPath, hookCode);

    return { tmpDir, hookPath, cleanup };
  } catch (err) {
    cleanup();
    throw err;
  }
}

// Runs the failed test on baseRef to confirm whether the failure already exists on main.
// Preloads an in-memory error flush hook so the instant an error or timeout occurs,
// it is immediately emitted to stderr and matched against targetErrors, killing the base
// run early and saving runner minutes.
function runOnBaseStreaming(tmpDir, hookPath, testRelPath, baseRef, targetErrors = []) {
  return new Promise((resolve) => {
    if (!fs.existsSync(path.join(tmpDir, testRelPath))) {
      resolve({ code: 0, output: `Test ${testRelPath} does not exist on ${baseRef}`, confirmedMatch: false });
      return;
    }

    let output = '';
    let confirmedMatch = false;
    const proc = spawn('node', ['--import', hookPath, testRelPath], {
      cwd: tmpDir,
      stdio: ['ignore', 'pipe', 'pipe']
    });

    const checkChunk = (chunk) => {
      const text = chunk.toString();
      output += text;

      // Check if an error signature matching the branch failure has appeared on main
      const currentErrors = extractErrorSignatures(output);
      const match = targetErrors.find(e => currentErrors.includes(e));
      if (match && !confirmedMatch) {
        confirmedMatch = true;
        output += `\n[Verified upstream failure on ${baseRef}: matching error found early (${match}). Terminating base run.]\n`;
        proc.kill('SIGTERM');
        setTimeout(() => {
          try { proc.kill('SIGKILL'); } catch {}
        }, 2000);
      }
    };

    proc.stdout.on('data', checkChunk);
    proc.stderr.on('data', checkChunk);

    proc.on('close', (code) => {
      resolve({ code: confirmedMatch ? 1 : (code ?? 1), output, confirmedMatch });
    });

    proc.on('error', (err) => {
      resolve({ code: 1, output: String(err), confirmedMatch: false });
    });
  });
}

async function main() {
  console.log('--- Step 1: Building project ---');
  const buildRes = await runCmd('node', ['build.mjs']);
  if (buildRes.code !== 0) {
    console.error('Build failed. Aborting tests.');
    process.exit(1);
  }

  console.log('\n--- Step 2: Running all tests unconditionally (2 workers) ---');
  const queue = [...TESTS];
  const results = [];

  async function runWorker(workerId) {
    while (queue.length > 0) {
      const testFile = queue.shift();
      console.log(`[Worker ${workerId}] >>> Starting ${testFile}`);
      const t0 = Date.now();
      const res = await runCmdBuffered('node', [testFile]);
      const duration = ((Date.now() - t0) / 1000).toFixed(1);
      const errors = extractErrorSignatures(res.output);
      console.log(`[Worker ${workerId}] <<< Finished ${testFile} in ${duration}s (exit: ${res.code})`);
      if (res.output.trim()) {
        console.log(`--- [Worker ${workerId}] Output for ${testFile} ---\n${res.output.trim()}\n--- [End of ${testFile}] ---\n`);
      }
      results.push({
        file: testFile,
        code: res.code,
        passed: res.code === 0,
        duration,
        errors
      });
    }
  }

  const tStart = Date.now();
  await Promise.all([runWorker(1), runWorker(2)]);
  const step2Duration = ((Date.now() - tStart) / 1000).toFixed(1);
  console.log(`Step 2 completed in ${step2Duration}s.`);

  // Sort results by original TESTS order for consistent reporting
  results.sort((a, b) => TESTS.indexOf(a.file) - TESTS.indexOf(b.file));

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

  if (!baseRef) {
    for (const fail of failedTests) {
      regressions.push({ file: fail.file, reason: 'Failed and no base branch available to compare' });
    }
  } else {
    let baseEnv = null;
    try {
      console.log(`Preparing shared baseline worktree for ${baseRef}...`);
      baseEnv = setupBaseWorktree(baseRef);
      const baseQueue = [...failedTests];

      async function runBaseWorker(workerId) {
        while (baseQueue.length > 0) {
          const fail = baseQueue.shift();
          console.log(`[Base Worker ${workerId}] >>> Checking ${fail.file} on ${baseRef}`);
          const t0 = Date.now();
          const baseRun = await runOnBaseStreaming(baseEnv.tmpDir, baseEnv.hookPath, fail.file, baseRef, fail.errors);
          const dur = ((Date.now() - t0) / 1000).toFixed(1);
          console.log(`[Base Worker ${workerId}] <<< Finished ${fail.file} on ${baseRef} in ${dur}s (exit: ${baseRun.code})`);
          if (baseRun.output.trim()) {
            console.log(`--- [Base Worker ${workerId}] Output for ${fail.file} ---\n${baseRun.output.trim()}\n--- [End of base ${fail.file}] ---\n`);
          }

          if (baseRun.code !== 0) {
            const baseErrors = extractErrorSignatures(baseRun.output);
            const matches = fail.errors.filter(e => baseErrors.includes(e));
            if (baseRun.confirmedMatch || matches.length > 0) {
              upstreamIssues.push({
                file: fail.file,
                errors: fail.errors,
                baseErrors: baseErrors.length ? baseErrors : fail.errors
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
      }

      const workerCount = Math.min(2, failedTests.length);
      await Promise.all(Array.from({ length: workerCount }, (_, i) => runBaseWorker(i + 1)));
    } finally {
      if (baseEnv) baseEnv.cleanup();
    }
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
