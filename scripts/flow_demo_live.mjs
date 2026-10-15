// Drives ONE persistent headless Chromium session against /flow, triggers real cluster actions,
// and captures screenshots at timed offsets. Usage: node scripts/flow_demo_live.mjs <outdir>
import { spawn } from 'node:child_process';
import { writeFileSync, mkdirSync } from 'node:fs';

const OUT = process.argv[2] || '/tmp/flow_shots';
mkdirSync(OUT, { recursive: true });
const BASE = 'http://localhost:8000';
const PORT = 9333;
const sleep = ms => new Promise(r => setTimeout(r, ms));

const chrome = spawn('chromium', ['--headless=new', '--no-sandbox', '--disable-gpu', '--hide-scrollbars',
  `--remote-debugging-port=${PORT}`, '--window-size=1500,950', '--user-data-dir=/tmp/flow-cdp-profile', 'about:blank'],
  { stdio: 'ignore' });

async function wsUrl() {
  for (let i = 0; i < 40; i++) {
    try {
      const tabs = await (await fetch(`http://127.0.0.1:${PORT}/json`)).json();
      const t = tabs.find(x => x.type === 'page');
      if (t) return t.webSocketDebuggerUrl;
    } catch (_) {}
    await sleep(250);
  }
  throw new Error('chromium debugger not reachable');
}

const ws = new WebSocket(await wsUrl());
await new Promise(r => (ws.onopen = r));
let id = 0; const pending = new Map();
ws.onmessage = m => { const d = JSON.parse(m.data); if (d.id && pending.has(d.id)) { pending.get(d.id)(d.result); pending.delete(d.id); } };
const send = (method, params = {}) => new Promise(r => { const i = ++id; pending.set(i, r); ws.send(JSON.stringify({ id: i, method, params })); });
const shot = async name => {
  const r = await send('Page.captureScreenshot', { format: 'png' });
  writeFileSync(`${OUT}/flow_${name}.png`, Buffer.from(r.data, 'base64')); console.log('shot', name);
};
const post = (path) => fetch(BASE + path, { method: 'POST' }).then(r => r.text()).catch(() => '');

await send('Page.enable');
await send('Emulation.setDeviceMetricsOverride', { width: 1500, height: 950, deviceScaleFactor: 1, mobile: false });

await post('/dashboard/reset'); await sleep(2500);
await send('Page.navigate', { url: `${BASE}/flow` }); await sleep(2500);
await shot('1_idle');

await post('/dashboard/submit-job?dataset=large&num_mappers=3&num_reducers=3');
await sleep(6000); await shot('2_running');

// Preempt Node 1 (SIGTERM). The call blocks until the drain ends, so fire and forget.
post('/dashboard/simulate-preemption?grace_seconds=15&auto_spawn=true&target_node=spot_mapreduce-worker-1');
await sleep(900);  await shot('3a_preempt_drain');
await sleep(2600); await shot('3b_preempt_gone');
await sleep(3500); await shot('3c_preempt_respawn');
await sleep(3500); await shot('3d_preempt_online');

// Hard-crash Node 2 (SIGKILL)
post('/dashboard/simulate-crash?auto_spawn=true&target_node=spot_mapreduce-worker-2');
await sleep(1200); await shot('4a_crash_killed');
await sleep(3500); await shot('4b_crash_respawn');

// wait for completion
for (let i = 0; i < 60; i++) {
  const st = await (await fetch(`${BASE}/dashboard/state`)).json();
  if (st.active_job && st.active_job.state === 'COMPLETED') break;
  await sleep(2000);
}
await sleep(1500); await shot('5_done');

ws.close(); chrome.kill();
process.exit(0);
