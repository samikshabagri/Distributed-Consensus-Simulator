#ifndef EMBEDDED_FRONTEND_H
#define EMBEDDED_FRONTEND_H

#include <string>

inline std::string getEmbeddedFrontend(const std::string& name) {
    if (name == "index.html") return R"__ASSET_END__(<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Distributed Consensus Simulator</title>
  <meta name="description" content="Live Raft consensus cluster, consistent hashing follower read router, and auto-scaling load balancer console.">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Familjen+Grotesk:wght@500;600&family=IBM+Plex+Sans:wght@400;500&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
  <link rel="stylesheet" href="/styles.css">
</head>
<body>
  <div class="container">
    <header class="topbar">
      <div>
        <h1>Distributed Consensus Simulator</h1>
        <p class="topbar-sub">Raft leader election, heartbeats, consistent-hash follower reads, and load-driven node scaling.</p>
      </div>
      <div class="legend" aria-label="Node state legend">
        <span class="legend-item"><span class="swatch swatch-leader" aria-hidden="true"></span>Leader (writes)</span>
        <span class="legend-item"><span class="swatch swatch-follower" aria-hidden="true"></span>Follower (reads)</span>
        <span class="legend-item"><span class="swatch swatch-scaled" aria-hidden="true"></span>Auto-scaled follower</span>
        <span class="legend-item"><span class="swatch swatch-crashed" aria-hidden="true"></span>Crashed</span>
      </div>
    </header>

    <section class="metrics-strip" aria-label="Cluster summary">
      <div class="metric-cell">
        <div class="stat-label">Raft leader</div>
        <div class="stat-value" id="statLeader">Loading</div>
        <div class="stat-sub" id="statTerm">Waiting for election</div>
      </div>
      <div class="metric-cell">
        <div class="stat-label">Cluster servers</div>
        <div class="stat-value" id="statNodes">-</div>
        <div class="stat-sub" id="statScaled">Checking active nodes</div>
      </div>
      <div class="metric-cell">
        <div class="stat-label">Load against capacity</div>
        <div class="stat-value" id="statLoad">0</div>
        <div class="stat-sub" id="statThreshold">Scale threshold: -</div>
      </div>
      <div class="metric-cell">
        <div class="stat-label">Messages processed</div>
        <div class="stat-value" id="statTotal">0</div>
        <div class="stat-sub" id="statSplit">Writes: 0, reads: 0</div>
      </div>
    </section>

    <main>
      <div class="grid-main">
        <section class="sheet" aria-labelledby="clusterHeading">
          <div class="sheet-head">
            <h2 id="clusterHeading">Cluster topology and routing</h2>
            <div class="sheet-actions">
              <button type="button" id="btnMotion" class="btn btn-sm btn-secondary" onclick="toggleMotion()">Pause motion</button>
              <span class="badge" id="clusterBadge">5 servers</span>
            </div>
          </div>
          <div class="map-frame" id="mapFrame">
            <svg id="clusterSvg" class="topology-diagram" viewBox="0 0 680 250" role="img" aria-label="Raft cluster topology and Load Balancer routing diagram">
              <title>Raft cluster topology and Load Balancer routing diagram</title>
              <g id="clusterStaticLayer"></g>
              <g id="clusterAnimLayer"></g>
            </svg>
          </div>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Server</th>
                  <th>Role</th>
                  <th class="num">Term</th>
                  <th class="num">Applied</th>
                  <th class="num">Reads</th>
                  <th class="num">Writes</th>
                  <th>Action</th>
                </tr>
              </thead>
              <tbody id="nodeList">
                <tr><td colspan="7" class="empty-note">Connecting to backend clusterâ€¦</td></tr>
              </tbody>
            </table>
          </div>
        </section>

        <section class="sheet" aria-labelledby="lbHeading">
          <div class="sheet-head">
            <h2 id="lbHeading">Load balancer and auto-scaling</h2>
            <span class="badge" id="burstBadge">Continuous 200 to 300/s</span>
          </div>
          <div class="btn-row">
            <button type="button" id="btnBurst3k" class="btn btn-primary" onclick="triggerBurst(3000)">Send 3,000 messages</button>
            <button type="button" id="btnBurst5k" class="btn btn-secondary" onclick="triggerBurst(5000)">Send 5,000 messages</button>
          </div>
          <div class="meter-track" role="progressbar" aria-label="Current load versus auto-scale threshold">
            <div class="meter-fill" id="loadBar"></div>
          </div>
          <p class="stat-sub" id="burstText">Continuous traffic flows at 200 to 300 messages/sec. Exceeding capacity spawns a Follower.</p>
          <div class="chart-frame">
            <svg id="loadChartSvg" class="load-chart" viewBox="0 0 460 190" role="img" aria-label="Real-time message load and auto-scale threshold chart">
              <title>Message load over time chart</title>
            </svg>
          </div>
        </section>
      </div>

      <div class="grid-split">
        <section class="sheet" aria-labelledby="ringHeading">
          <div class="sheet-head">
            <h2 id="ringHeading">Consistent hash ring</h2>
            <span class="badge badge-follower" id="ringBadge">Awaiting read</span>
          </div>
          <div class="map-frame">
            <svg id="ringSvg" class="ring-diagram" viewBox="0 0 440 230" role="img" aria-label="Consistent hash ring selecting follower read replicas">
              <title>Consistent hash ring diagram</title>
            </svg>
          </div>
        </section>

        <section class="sheet" aria-labelledby="opsHeading">
          <div class="sheet-head">
            <h2 id="opsHeading">Client operations</h2>
            <span class="badge">Writes to Leader, reads to Follower</span>
          </div>

          <div class="op-block">
            <h3 class="op-title">Write transaction (routed to Leader)</h3>
            <div class="form-grid">
              <div class="field-group">
                <label class="field-label" for="wType">Operation</label>
                <select id="wType" class="field-control" onchange="toggleToAcc()">
                  <option value="DEPOSIT">Deposit</option>
                  <option value="WITHDRAW">Withdraw</option>
                  <option value="TRANSFER">Transfer</option>
                </select>
              </div>
              <div class="field-group">
                <label class="field-label" for="wAcc">Account</label>
                <input id="wAcc" class="field-control field-num" type="text" value="ACC_101" placeholder="ACC_101">
              </div>
              <div class="field-group" id="wToGroup" hidden>
                <label class="field-label" for="wTo">To account</label>
                <input id="wTo" class="field-control field-num" type="text" value="ACC_202" placeholder="ACC_202">
              </div>
              <div class="field-group">
                <label class="field-label" for="wAmt">Amount</label>
                <input id="wAmt" class="field-control field-num" type="number" min="1" step="1" value="100" placeholder="100">
              </div>
              <button type="button" id="btnWrite" class="btn btn-secondary" onclick="doWrite()">Commit write</button>
            </div>
            <div class="result-line" id="writeResult" aria-live="polite">Submit a transaction to replicate through the Raft Leader.</div>
          </div>

          <div class="op-block">
            <h3 class="op-title">Read balance (routed via Consistent Hashing)</h3>
            <div class="form-grid">
              <div class="field-group">
                <label class="field-label" for="rAcc">Account</label>
                <input id="rAcc" class="field-control field-num" type="text" value="ACC_101" placeholder="ACC_101">
              </div>
              <button type="button" id="btnRead" class="btn btn-secondary" onclick="doRead()">Read from follower</button>
            </div>
            <div class="result-line" id="readResult" aria-live="polite">Reads select a healthy Follower from the hash ring.</div>
          </div>
        </section>
      </div>

      <div class="grid-split">
        <section class="sheet" aria-labelledby="ledgerHeading">
          <div class="sheet-head">
            <h2 id="ledgerHeading">Replicated bank ledger</h2>
            <span class="badge">Deterministic state machine</span>
          </div>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Account</th>
                  <th class="num">Balance</th>
                </tr>
              </thead>
              <tbody id="ledgerBody">
                <tr><td colspan="2" class="empty-note">Loading ledger balancesâ€¦</td></tr>
              </tbody>
            </table>
          </div>
        </section>

        <section class="sheet" aria-labelledby="logsHeading">
          <div class="sheet-head">
            <h2 id="logsHeading">System event log</h2>
            <span class="badge">Elections, routing, scaling</span>
          </div>
          <div class="log-box" id="logBox" role="log" aria-live="polite">
            <div class="empty-note">Waiting for cluster eventsâ€¦</div>
          </div>
        </section>
      </div>
    </main>
  </div>

  <script>
    const prefersReduced = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    let motionEnabled = !prefersReduced;
    let prevStats = {};
    let latestNodeCoords = {};
    let latestLeaderId = -1;
    let activePulses = [];
    let lastHeartbeatTs = 0;

    if (!motionEnabled) {
      document.getElementById('mapFrame').classList.add('is-paused');
      document.getElementById('btnMotion').textContent = 'Resume motion';
    }

    function toggleMotion() {
      motionEnabled = !motionEnabled;
      document.getElementById('mapFrame').classList.toggle('is-paused', !motionEnabled);
      document.getElementById('btnMotion').textContent = motionEnabled ? 'Pause motion' : 'Resume motion';
      if (!motionEnabled) {
        activePulses = [];
        document.getElementById('clusterAnimLayer').innerHTML = '';
      }
    }

    function toggleToAcc() {
      const isTransfer = document.getElementById('wType').value === 'TRANSFER';
      document.getElementById('wToGroup').hidden = !isTransfer;
    }

    function escapeHtml(text) {
      return String(text)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
    }

    async function doWrite() {
      const type = document.getElementById('wType').value;
      const accInput = document.getElementById('wAcc');
      const amtInput = document.getElementById('wAmt');
      const acc = accInput.value.trim();
      const to = document.getElementById('wTo').value.trim();
      const amt = Number(amtInput.value);
      const box = document.getElementById('writeResult');
      const btn = document.getElementById('btnWrite');

      accInput.setAttribute('aria-invalid', acc ? 'false' : 'true');
      amtInput.setAttribute('aria-invalid', amt > 0 ? 'false' : 'true');
      if (!acc || !(amt > 0)) {
        box.className = 'result-line is-error';
        box.textContent = 'Enter a valid account ID and a positive amount.';
        return;
      }

      btn.disabled = true;
      btn.textContent = 'Committingâ€¦';
      try {
        const url = `/write?type=${encodeURIComponent(type)}&acc=${encodeURIComponent(acc)}&to=${encodeURIComponent(to)}&amt=${encodeURIComponent(amt)}`;
        const res = await fetch(url).then(r => r.json());
        if (res.ok) {
          box.className = 'result-line is-ok';
          box.innerHTML = `Committed on Leader <strong class="num">Node ${res.leaderId}</strong> and replicated across the cluster.`;
          if (res.leaderId >= 0) spawnPulse('lb', res.leaderId, 'var(--color-accent)', 4.8);
        } else {
          box.className = 'result-line is-error';
          box.textContent = 'Write rejected: no active Raft Leader is available.';
        }
        refresh();
      } catch (e) {
        box.className = 'result-line is-error';
        box.textContent = 'Request failed: backend server is unreachable.';
      } finally {
        btn.disabled = false;
        btn.textContent = 'Commit write';
      }
    }

    async function doRead() {
      const accInput = document.getElementById('rAcc');
      const acc = accInput.value.trim();
      const box = document.getElementById('readResult');
      const btn = document.getElementById('btnRead');

      accInput.setAttribute('aria-invalid', acc ? 'false' : 'true');
      if (!acc) {
        box.className = 'result-line is-error';
        box.textContent = 'Enter an account ID to read.';
        return;
      }

      btn.disabled = true;
      btn.textContent = 'Routingâ€¦';
      try {
        const res = await fetch(`/read?acc=${encodeURIComponent(acc)}`).then(r => r.json());
        if (res.ok) {
          box.className = 'result-line is-ok';
          box.innerHTML = `Consistent Hash routed <strong class="num">${escapeHtml(res.account)}</strong> to Follower <strong class="num">Node ${res.followerId}</strong>: balance <strong class="num">$${res.balance.toFixed(2)}</strong>`;
          spawnPulse('lb', res.followerId, 'var(--color-water)', 4.5);
        } else {
          box.className = 'result-line is-error';
          box.textContent = 'No healthy follower is available to serve the read.';
        }
        refresh();
      } catch (e) {
        box.className = 'result-line is-error';
        box.textContent = 'Request failed: backend server is unreachable.';
      } finally {
        btn.disabled = false;
        btn.textContent = 'Read from follower';
      }
    }

    async function triggerBurst(n) {
      try {
        await fetch(`/burst?n=${n}`);
        refresh();
      } catch (e) {}
    }

    async function toggleNode(id, crashed) {
      try {
        await fetch(`/${crashed ? 'recover' : 'crash'}?id=${id}`);
        refresh();
      } catch (e) {}
    }

    function spawnPulse(fromKey, toKey, color, radius, curveYOffset = 0, delayMs = 0) {
      if (!motionEnabled) return;
      if (activePulses.length > 64) activePulses.shift();
      activePulses.push({
        fromKey,
        toKey,
        color,
        radius,
        curveYOffset,
        start: performance.now() + delayMs,
        duration: 520
      });
    }

    function drawCluster(data) {
      const layer = document.getElementById('clusterStaticLayer');
      const nodes = data.nodes || [];
      const w = 680;
      const lbX = w / 2;
      const lbY = 36;
      const coords = { lb: { x: lbX, y: 60 } };

      let out = `
        <defs>
          <marker id="arrLeader" markerWidth="6" markerHeight="6" refX="5" refY="3" orient="auto">
            <path d="M0,0 L6,3 L0,6 Z" fill="var(--color-accent)"/>
          </marker>
          <marker id="arrFollower" markerWidth="6" markerHeight="6" refX="5" refY="3" orient="auto">
            <path d="M0,0 L6,3 L0,6 Z" fill="var(--color-water)"/>
          </marker>
          <marker id="arrScaled" markerWidth="6" markerHeight="6" refX="5" refY="3" orient="auto">
            <path d="M0,0 L6,3 L0,6 Z" fill="var(--color-green)"/>
          </marker>
        </defs>
        <path d="M-20,210 Q180,168 340,198 T700,180" fill="none" stroke="var(--color-contour-soft)" stroke-width="1"/>
        <path d="M-20,140 Q220,104 410,136 T700,118" fill="none" stroke="var(--color-contour-soft)" stroke-width="1"/>
        <rect x="${lbX - 135}" y="12" width="270" height="48" rx="6" fill="var(--color-paper-2)" stroke="var(--color-ink)" stroke-width="1.2"/>
        <text x="${lbX}" y="31" text-anchor="middle" fill="var(--color-ink)" font-family="var(--font-body)" font-size="12" font-weight="600">Load balancer and hash ring</text>
        <text x="${lbX}" y="49" text-anchor="middle" fill="var(--color-muted)" font-family="var(--font-outlier)" font-size="11">Load ${data.lb.currentLoad} / Cap ${data.lb.threshold}</text>
      `;

      const n = nodes.length;
      const margin = 52;
      const span = w - margin * 2;
      const yPos = 152;
      const busY = 204;

      // Shared cluster peer mesh bus showing servers are connected
      if (n > 1) {
        out += `<line x1="${margin}" y1="${busY}" x2="${w - margin}" y2="${busY}" stroke="var(--color-ink-2)" stroke-width="1.2" opacity="0.45"/>`;
      }

      const leaderNode = nodes.find(nd => nd.state === 'LEADER' && !nd.crashed);
      const leaderIdx = leaderNode ? nodes.indexOf(leaderNode) : -1;
      const leaderX = leaderIdx >= 0 ? (n <= 1 ? w / 2 : margin + (span * leaderIdx) / (n - 1)) : -1;

      nodes.forEach((node, idx) => {
        const x = n <= 1 ? w / 2 : margin + (span * idx) / (n - 1);
        coords[node.id] = { x, y: yPos };
        const isLeader = node.state === 'LEADER';
        const isCand = node.state === 'Candidate';
        const stroke = node.crashed
          ? 'var(--color-error)'
          : isLeader
          ? 'var(--color-accent)'
          : isCand
          ? 'var(--color-candidate)'
          : node.autoScaled
          ? 'var(--color-green)'
          : 'var(--color-water)';
        const fill = node.crashed
          ? 'var(--color-error-soft)'
          : isLeader
          ? 'var(--color-accent-soft)'
          : node.autoScaled
          ? 'var(--color-green-soft)'
          : 'var(--color-paper-2)';

        // Bus tap from server to cluster interconnect
        out += `<line x1="${x}" y1="${yPos + 28}" x2="${x}" y2="${busY}" stroke="${node.crashed ? 'var(--color-error)' : 'var(--color-ink-2)'}" stroke-width="1" ${node.crashed ? 'stroke-dasharray="2 3"' : ''} opacity="0.55"/>`;

        // Heartbeat peer arc from Leader to each healthy Follower
        if (!node.crashed && !isLeader && leaderX >= 0) {
          const midX = (leaderX + x) / 2;
          const ctrlY = busY + 26;
          out += `<path d="M${leaderX},${yPos + 28} Q${midX},${ctrlY} ${x},${yPos + 28}" fill="none" stroke="var(--color-contour)" stroke-width="1.1" stroke-dasharray="3 3" opacity="0.7"/>`;
        }

        // Load balancer routing lines
        if (!node.crashed) {
          if (isLeader) {
            out += `<line x1="${lbX}" y1="60" x2="${x}" y2="${yPos - 31}" stroke="var(--color-accent)" stroke-width="2" marker-end="url(#arrLeader)"/>`;
          } else if (node.autoScaled) {
            out += `<line x1="${lbX}" y1="60" x2="${x}" y2="${yPos - 31}" stroke="var(--color-green)" stroke-width="1.5" stroke-dasharray="4 3" marker-end="url(#arrScaled)"/>`;
          } else {
            out += `<line x1="${lbX}" y1="60" x2="${x}" y2="${yPos - 31}" stroke="var(--color-water)" stroke-width="1.3" stroke-dasharray="4 3" marker-end="url(#arrFollower)"/>`;
          }
        }

        const r = n > 7 ? 22 : 25;
        if (isLeader && !node.crashed) {
          out += `<polygon class="hb-ring" points="${x},${yPos - r - 6} ${x + r + 6},${yPos} ${x},${yPos + r + 6} ${x - r - 6},${yPos}" fill="none" stroke="var(--color-accent)" stroke-width="1.5"/>`;
          out += `<polygon points="${x},${yPos - r - 4} ${x + r + 4},${yPos} ${x},${yPos + r + 4} ${x - r - 4},${yPos}" fill="${fill}" stroke="${stroke}" stroke-width="2.2"/>`;
        } else {
          out += `<circle cx="${x}" cy="${yPos}" r="${r}" fill="${fill}" stroke="${stroke}" stroke-width="${node.autoScaled ? 2.2 : 1.6}"/>`;
        }

        const roleLabel = node.crashed ? 'Down' : isLeader ? 'Leader' : isCand ? 'Cand' : node.autoScaled ? 'New' : 'Follow';
        out += `
          <text x="${x}" y="${yPos - 2}" text-anchor="middle" fill="var(--color-ink)" font-family="var(--font-outlier)" font-size="12" font-weight="500">N${node.id}</text>
          <text x="${x}" y="${yPos + 12}" text-anchor="middle" fill="var(--color-ink-2)" font-family="var(--font-body)" font-size="10" font-weight="500">${roleLabel}</text>
          <text x="${x}" y="${busY + 26}" text-anchor="middle" fill="var(--color-muted)" font-family="var(--font-outlier)" font-size="10">R${node.readsHandled} W${node.writesHandled}</text>
        `;
      });

      layer.innerHTML = out;
      latestNodeCoords = coords;
      latestLeaderId = data.leaderId;
    }

    function stepAnimations(now) {
      const animLayer = document.getElementById('clusterAnimLayer');
      if (!motionEnabled) {
        requestAnimationFrame(stepAnimations);
        return;
      }

      // Emit periodic Raft heartbeat pulses from Leader to Followers
      if (latestLeaderId >= 0 && now - lastHeartbeatTs > 750) {
        lastHeartbeatTs = now;
        const ids = Object.keys(latestNodeCoords).filter(k => k !== 'lb' && Number(k) !== latestLeaderId);
        ids.forEach((fid, i) => {
          spawnPulse(latestLeaderId, Number(fid), 'var(--color-contour)', 3.2, 42, i * 40);
        });
      }

      const nextPulses = [];
      let animSvg = '';

      for (const p of activePulses) {
        const a = latestNodeCoords[p.fromKey];
        const b = latestNodeCoords[p.toKey];
        if (!a || !b) continue;

        if (now < p.start) {
          nextPulses.push(p);
          continue;
        }
        const t = (now - p.start) / p.duration;
        if (t >= 1) continue;

        nextPulses.push(p);

        let px, py;
        if (p.curveYOffset > 0) {
          // Quadratic Bezier along the peer heartbeat arc below the bus
          const mx = (a.x + b.x) / 2;
          const cy = 204 + 26;
          const sy = a.y + 28;
          const ey = b.y + 28;
          const u = 1 - t;
          px = u * u * a.x + 2 * u * t * mx + t * t * b.x;
          py = u * u * sy + 2 * u * t * cy + t * t * ey;
        } else {
          const sy = p.fromKey === 'lb' ? a.y : a.y;
          const ey = b.y - 28;
          px = a.x + (b.x - a.x) * t;
          py = sy + (ey - sy) * t;
        }

        const op = t < 0.15 ? t / 0.15 : t > 0.85 ? (1 - t) / 0.15 : 1;
        animSvg += `<circle cx="${px.toFixed(1)}" cy="${py.toFixed(1)}" r="${p.radius}" fill="${p.color}" opacity="${op.toFixed(2)}"/>`;

        // Flash subtle arrival ring when pulse reaches destination node
        if (t > 0.84) {
          const dst = latestNodeCoords[p.toKey];
          animSvg += `<circle cx="${dst.x}" cy="${dst.y}" r="${26 + (t - 0.84) * 55}" fill="none" stroke="${p.color}" stroke-width="1.5" opacity="${((1 - t) * 5).toFixed(2)}"/>`;
        }
      }

      activePulses = nextPulses;
      animLayer.innerHTML = animSvg;
      requestAnimationFrame(stepAnimations);
    }
    requestAnimationFrame(stepAnimations);

    function drawRing(data) {
      const svg = document.getElementById('ringSvg');
      const cx = 132;
      const cy = 115;
      const R = 82;
      const followers = (data.nodes || []).filter(n => !n.crashed && n.state !== 'LEADER');

      let out = `
        <title>Consistent hash ring diagram</title>
        <circle cx="${cx}" cy="${cy}" r="${R}" fill="none" stroke="var(--color-rule)" stroke-width="5"/>
        <text x="${cx}" y="${cy - 4}" text-anchor="middle" fill="var(--color-muted)" font-family="var(--font-body)" font-size="11">Active ring</text>
        <text x="${cx}" y="${cy + 14}" text-anchor="middle" fill="var(--color-ink)" font-family="var(--font-outlier)" font-size="12" font-weight="500">${followers.length} followers</text>
      `;

      const posMap = {};
      followers.forEach((f, i) => {
        const angle = (2 * Math.PI * i) / Math.max(1, followers.length) - Math.PI / 2;
        const x = cx + R * Math.cos(angle);
        const y = cy + R * Math.sin(angle);
        posMap[f.id] = { x, y };
        const stroke = f.autoScaled ? 'var(--color-green)' : 'var(--color-water)';
        const fill = f.autoScaled ? 'var(--color-green-soft)' : 'var(--color-paper-2)';
        const isHit = f.id === data.lb.lastReadNode;
        if (isHit && motionEnabled) {
          out += `<circle class="hb-ring" cx="${x}" cy="${y}" r="18" fill="none" stroke="var(--color-accent)" stroke-width="1.4"/>`;
        }
        out += `
          <circle cx="${x}" cy="${y}" r="${isHit ? 17 : 14}" fill="${isHit ? 'var(--color-accent-soft)' : fill}" stroke="${isHit ? 'var(--color-accent)' : stroke}" stroke-width="${isHit ? 2.5 : 1.6}"/>
          <text x="${x}" y="${y + 4}" text-anchor="middle" fill="var(--color-ink)" font-family="var(--font-outlier)" font-size="11" font-weight="500">N${f.id}</text>
        `;
      });

      const lastNode = data.lb.lastReadNode;
      if (lastNode >= 0 && posMap[lastNode]) {
        const p = posMap[lastNode];
        out += `<line x1="${cx}" y1="${cy + 22}" x2="${p.x}" y2="${p.y}" stroke="var(--color-accent)" stroke-width="1.8" stroke-dasharray="3 2"/>`;
      }

      out += `
        <rect x="244" y="30" width="180" height="170" rx="6" fill="var(--color-paper-2)" stroke="var(--color-rule)" stroke-width="1"/>
        <text x="258" y="54" fill="var(--color-muted)" font-family="var(--font-body)" font-size="11">Latest read route</text>
        <text x="258" y="80" fill="var(--color-ink)" font-family="var(--font-outlier)" font-size="13" font-weight="500">${escapeHtml(data.lb.lastReadAcc || '-')}</text>
        <text x="258" y="106" fill="var(--color-ink)" font-family="var(--font-body)" font-size="13" font-weight="500">Follower Node ${lastNode >= 0 ? lastNode : '-'}</text>
        <text x="258" y="130" fill="var(--color-muted)" font-family="var(--font-outlier)" font-size="12">$${(data.lb.lastReadBal || 0).toFixed(2)}</text>
        <line x1="258" y1="146" x2="410" y2="146" stroke="var(--color-rule)" stroke-width="1"/>
        <text x="258" y="166" fill="var(--color-muted)" font-family="var(--font-body)" font-size="10">Leader excluded from reads.</text>
        <text x="258" y="182" fill="var(--color-muted)" font-family="var(--font-body)" font-size="10">Scaled followers join ring live.</text>
      `;

      svg.innerHTML = out;
      document.getElementById('ringBadge').textContent = lastNode >= 0
        ? `${data.lb.lastReadAcc} to Node ${lastNode}`
        : 'Awaiting read';
    }

    function drawLoadChart(data) {
      const svg = document.getElementById('loadChartSvg');
      const hist = data.lb.history || [];
      const thr = Math.max(1, data.lb.threshold || 480);
      const maxVal = Math.max(thr * 1.35, ...hist, 100);
      const W = 460;
      const H = 190;
      const padL = 44;
      const padR = 14;
      const padT = 16;
      const padB = 26;
      const cw = W - padL - padR;
      const ch = H - padT - padB;

      const yFor = v => padT + ch - (v / maxVal) * ch;
      const thrY = yFor(thr);

      let pts = '';
      let area = `${padL},${padT + ch} `;
      hist.forEach((v, i) => {
        const x = padL + (i / Math.max(1, hist.length - 1)) * cw;
        const y = yFor(v);
        pts += `${x},${y} `;
        area += `${x},${y} `;
      });
      area += `${padL + cw},${padT + ch}`;

      const midY = yFor(maxVal / 2);
      svg.innerHTML = `
        <title>Message load over time chart</title>
        <defs>
          <linearGradient id="loadFillGrad" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stop-color="var(--color-water)" stop-opacity="0.22"/>
            <stop offset="100%" stop-color="var(--color-water)" stop-opacity="0"/>
          </linearGradient>
        </defs>
        <line x1="${padL}" y1="${padT}" x2="${W - padR}" y2="${padT}" stroke="var(--color-rule)" stroke-width="1"/>
        <line x1="${padL}" y1="${midY}" x2="${W - padR}" y2="${midY}" stroke="var(--color-rule)" stroke-width="1"/>
        <line x1="${padL}" y1="${padT + ch}" x2="${W - padR}" y2="${padT + ch}" stroke="var(--color-ink)" stroke-width="1"/>
        <line x1="${padL}" y1="${thrY}" x2="${W - padR}" y2="${thrY}" stroke="var(--color-error)" stroke-width="1.2"/>
        <text x="${padL + 4}" y="${Math.max(12, thrY - 5)}" fill="var(--color-ink)" font-family="var(--font-body)" font-size="10" font-weight="500">Auto-scale threshold (${thr})</text>
        <polygon points="${area}" fill="url(#loadFillGrad)"/>
        <polyline points="${pts}" fill="none" stroke="var(--color-accent)" stroke-width="2"/>
        <text x="6" y="${padT + 4}" fill="var(--color-muted)" font-family="var(--font-outlier)" font-size="10">${Math.round(maxVal)}</text>
        <text x="6" y="${midY + 4}" fill="var(--color-muted)" font-family="var(--font-outlier)" font-size="10">${Math.round(maxVal / 2)}</text>
        <text x="6" y="${padT + ch}" fill="var(--color-muted)" font-family="var(--font-outlier)" font-size="10">0</text>
        <text x="${W / 2}" y="${H - 6}" text-anchor="middle" fill="var(--color-muted)" font-family="var(--font-body)" font-size="10">Recent polling intervals</text>
      `;
    }

    function emitLiveTrafficPulses(nodes) {
      for (const n of nodes) {
        const prev = prevStats[n.id];
        if (prev && !n.crashed) {
          const dReads = n.readsHandled - prev.reads;
          const dWrites = n.writesHandled - prev.writes;
          if (dReads > 0) {
            const count = Math.min(3, Math.max(1, Math.ceil(dReads / 40)));
            const col = n.autoScaled ? 'var(--color-green)' : 'var(--color-water)';
            for (let k = 0; k < count; k++) spawnPulse('lb', n.id, col, 3.8, 0, k * 85);
          }
          if (dWrites > 0) {
            spawnPulse('lb', n.id, 'var(--color-accent)', 4.5, 0, 0);
          }
        }
        prevStats[n.id] = { reads: n.readsHandled, writes: n.writesHandled };
      }
    }

    async function refresh() {
      try {
        const data = await fetch('/status').then(r => r.json());
        const nodes = data.nodes || [];
        const scaledCount = nodes.filter(n => n.autoScaled).length;

        document.getElementById('statLeader').textContent = data.leaderId >= 0 ? `Node ${data.leaderId}` : 'Electing';
        document.getElementById('statTerm').textContent = `Term ${data.term}`;
        document.getElementById('statNodes').textContent = `${nodes.length} nodes`;
        document.getElementById('statScaled').textContent = `${scaledCount} auto-scaled follower(s)`;
        document.getElementById('statLoad').textContent = data.lb.currentLoad;
        document.getElementById('statThreshold').textContent = `Scale threshold: ${data.lb.threshold}`;
        document.getElementById('statTotal').textContent = data.lb.totalReads + data.lb.totalWrites;
        document.getElementById('statSplit').textContent = `Writes: ${data.lb.totalWrites}, reads: ${data.lb.totalReads}`;
        document.getElementById('clusterBadge').textContent = `${nodes.length} servers`;

        const ratio = Math.min(1, data.lb.currentLoad / Math.max(1, data.lb.threshold));
        document.getElementById('loadBar').style.transform = `scaleX(${ratio})`;

        const burstBadge = document.getElementById('burstBadge');
        const b3 = document.getElementById('btnBurst3k');
        const b5 = document.getElementById('btnBurst5k');
        if (data.lb.bursting) {
          burstBadge.textContent = `Burst ${data.lb.burstSent} / ${data.lb.burstTotal}`;
          burstBadge.className = 'badge badge-leader';
          document.getElementById('burstText').textContent = `Sending high load (${data.lb.burstSent} of ${data.lb.burstTotal} messages).`;
          b3.disabled = true;
          b5.disabled = true;
        } else {
          burstBadge.textContent = 'Continuous 200 to 300/s';
          burstBadge.className = 'badge badge-follower';
          document.getElementById('burstText').textContent = `Continuous traffic flows at 200 to 300 messages/sec. Exceeding ${data.lb.threshold} spawns a Follower.`;
          b3.disabled = false;
          b5.disabled = false;
        }

        drawCluster(data);
        emitLiveTrafficPulses(nodes);
        drawRing(data);
        drawLoadChart(data);

        const tbody = document.getElementById('nodeList');
        if (!nodes.length) {
          tbody.innerHTML = '<tr><td colspan="7" class="empty-note">No nodes registered in cluster.</td></tr>';
        } else {
          tbody.innerHTML = nodes.map(n => {
            const bCls = n.crashed ? 'badge-crashed' : n.state === 'LEADER' ? 'badge-leader' : n.autoScaled ? 'badge-scaled' : 'badge-follower';
            const label = n.crashed ? 'Crashed' : n.state === 'LEADER' ? 'Leader' : n.autoScaled ? 'Auto-scaled' : 'Follower';
            return `
              <tr class="${n.crashed ? 'is-crashed' : ''}">
                <td class="num">Node ${n.id}</td>
                <td><span class="badge ${bCls}">${label}</span></td>
                <td class="num">${n.term}</td>
                <td class="num">${n.lastApplied}/${n.logLen}</td>
                <td class="num">${n.readsHandled}</td>
                <td class="num">${n.writesHandled}</td>
                <td>
                  <button type="button" class="btn btn-sm ${n.crashed ? 'btn-recover' : 'btn-danger'}" onclick="toggleNode(${n.id}, ${n.crashed})">
                    ${n.crashed ? 'Recover' : 'Crash'}
                  </button>
                </td>
              </tr>
            `;
          }).join('');
        }

        const entries = Object.entries(data.balances || {});
        const ledgerBody = document.getElementById('ledgerBody');
        if (!entries.length) {
          ledgerBody.innerHTML = '<tr><td colspan="2" class="empty-note">No accounts committed yet. Submit a Deposit write to create one.</td></tr>';
        } else {
          ledgerBody.innerHTML = entries.map(([acc, bal]) =>
            `<tr><td class="num">${escapeHtml(acc)}</td><td class="num">$${Number(bal).toFixed(2)}</td></tr>`
          ).join('');
        }

        const logBox = document.getElementById('logBox');
        const logs = data.logs || [];
        if (!logs.length) {
          logBox.innerHTML = '<div class="empty-note">No events logged yet.</div>';
        } else {
          logBox.innerHTML = logs.slice().reverse().map(line => {
            const cls = line.includes('Shot up') ? 'log-scale'
                      : line.includes('LEADER') ? 'log-leader'
                      : line.includes('crashed') ? 'log-crash' : '';
            return `<div class="log-line ${cls}">${escapeHtml(line)}</div>`;
          }).join('');
        }
      } catch (e) {}
    }

    refresh();
    setInterval(refresh, 300);
  </script>
</body>
</html>
)__ASSET_END__";
    if (name == "styles.css") return R"__ASSET_END__(/* Hallmark Â· shape: app screen Â· genre: modern-minimal Â· theme: Cobalt (by request) Â· redesign: full
 * nav: edge-aligned title bar with legend swatches Â· signature: Cobalt cut-corner telemetry sheets, ASCII-grid map, and graphite event log Â· states: loading, empty, error, full, bursting
 * axes: light/grotesk-sans/cool Â· posture: restrained Â· contrast: pass Â· v2.0.0 */
/* Hallmark Â· pre-emit critique: P5 H5 E5 S5 R5 V4 Â· weakest: E, applied Cobalt cut-corner geometry, tactile keycaps, and graphite log register */
/* Hallmark Â· dropped arrGold, arrCyan Â· renamed inline SVG arrow markers to semantic arrLeader, arrFollower, and arrScaled */
@import url("./tokens.css");
@import url("./base.css");

body {
  padding-block: var(--space-lg);
  background: var(--color-paper);
  color: var(--color-ink);
}

h1, h2, h3 {
  font-family: var(--font-display);
  font-weight: var(--display-weight);
  letter-spacing: var(--tracking-display);
  line-height: 1.02;
  color: var(--color-ink);
}

h3 {
  font-weight: 500;
}

.topbar {
  display: flex;
  flex-wrap: wrap;
  justify-content: space-between;
  align-items: flex-end;
  gap: var(--space-md);
  padding-bottom: var(--space-md);
  border-bottom: var(--rule-hair) solid var(--color-rule-2);
  margin-bottom: var(--space-lg);
}

.topbar h1 {
  max-width: 28ch;
}

.topbar-sub {
  margin-top: var(--space-2xs);
  color: var(--color-muted);
  font-size: var(--text-sm);
}

.legend {
  display: flex;
  flex-wrap: wrap;
  gap: var(--space-md);
  align-items: center;
  font-size: var(--text-sm);
  color: var(--color-ink-2);
}

.legend-item {
  display: inline-flex;
  align-items: center;
  gap: var(--space-xs);
}

.swatch {
  display: inline-block;
  width: 10px;
  height: 10px;
  border-radius: 2px;
  border: var(--rule-hair) solid var(--color-rule-2);
}

.swatch-leader { background: var(--color-accent); }
.swatch-follower { background: var(--color-water); }
.swatch-scaled { background: var(--color-green); }
.swatch-crashed { background: var(--color-error); }

/* Top ruled telemetry strip */
.metrics-strip {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  border: var(--rule-hair) solid var(--color-rule-2);
  border-radius: var(--radius-card);
  background: var(--color-key-face);
  color: var(--color-ink);
  box-shadow: var(--shadow-card);
  margin-bottom: var(--space-lg);
  overflow: hidden;
}

@supports (corner-shape: bevel) {
  .metrics-strip,
  .sheet {
    border-radius: var(--cut-l) var(--space-2xs) var(--cut-l) var(--space-2xs);
    corner-shape: bevel round bevel round;
  }
}

.metric-cell {
  padding: var(--space-md) var(--space-lg);
  min-width: 0;
}

.metric-cell + .metric-cell {
  border-inline-start: var(--rule-hair) solid var(--color-rule);
}

.stat-label {
  font-family: var(--font-display);
  font-size: var(--text-sm);
  font-weight: 500;
  color: var(--color-muted);
}

.stat-value {
  font-family: var(--font-display);
  font-variant-numeric: tabular-nums;
  font-size: var(--text-xl);
  font-weight: 600;
  color: var(--color-ink);
  margin-top: var(--space-2xs);
}

.stat-sub {
  font-size: var(--text-sm);
  color: var(--color-muted);
  margin-top: var(--space-2xs);
  min-height: 1lh;
}

/* Workspace grid */
.grid-main {
  display: grid;
  grid-template-columns: minmax(0, 1.45fr) minmax(0, 1fr);
  gap: var(--space-lg);
  margin-bottom: var(--space-lg);
  align-items: start;
}

.grid-split {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: var(--space-lg);
  margin-bottom: var(--space-lg);
  align-items: start;
}

/* Cobalt cut-corner sheet */
.sheet {
  background: var(--color-key-face);
  color: var(--color-ink);
  border: var(--rule-hair) solid var(--color-rule-2);
  border-radius: var(--radius-card);
  padding: var(--space-lg);
  box-shadow: var(--shadow-card);
  min-width: 0;
}

.sheet-head {
  display: flex;
  flex-wrap: wrap;
  justify-content: space-between;
  align-items: center;
  gap: var(--space-sm);
  padding-bottom: var(--space-sm);
  border-bottom: var(--rule-hair) solid var(--color-rule);
  margin-bottom: var(--space-md);
}

.sheet-actions {
  display: inline-flex;
  align-items: center;
  gap: var(--space-xs);
}

@keyframes hbPulse {
  0% { transform: scale(1); opacity: 0.72; }
  100% { transform: scale(1.65); opacity: 0; }
}

.hb-ring {
  transform-box: fill-box;
  transform-origin: center;
  animation: hbPulse 1.5s var(--ease-out) infinite;
}

.is-paused .hb-ring {
  animation-play-state: paused;
  opacity: 0.35;
}

/* Cobalt cut status chips */
.badge {
  display: inline-flex;
  align-items: center;
  padding: var(--space-2xs) var(--space-xs);
  border-radius: var(--radius-input);
  font-family: var(--font-display);
  font-size: var(--text-xs);
  font-weight: 500;
  background: var(--color-paper-2);
  color: var(--color-ink);
  border: var(--rule-hair) solid var(--color-rule-2);
  white-space: nowrap;
}

.badge-leader {
  background: var(--color-accent-wash);
  border-color: var(--color-accent);
  color: var(--color-ink);
}

.badge-follower {
  background: var(--color-water-soft);
  border-color: var(--color-water);
  color: var(--color-ink);
}

.badge-scaled {
  background: var(--color-green-soft);
  border-color: var(--color-green);
  color: var(--color-ink);
}

.badge-crashed {
  background: var(--color-error-soft);
  border-color: var(--color-error);
  color: var(--color-ink);
}

/* Cobalt 8x16 cell dot-grid diagram well */
.map-frame {
  background:
    radial-gradient(var(--color-glyph-1) 1px, transparent 1px) 0 0 / var(--cell-w) var(--cell-h),
    var(--color-paper);
  color: var(--color-ink);
  border: var(--rule-hair) solid var(--color-rule);
  border-radius: var(--radius-input);
  padding: var(--space-xs);
  margin-bottom: var(--space-md);
}

.topology-diagram,
.ring-diagram,
.load-chart {
  width: 100%;
  height: auto;
}

.chart-frame {
  background: var(--color-paper);
  color: var(--color-ink);
  border: var(--rule-hair) solid var(--color-rule);
  border-radius: var(--radius-input);
  padding: var(--space-xs);
  margin-top: var(--space-md);
}

/* Ruled tables */
.table-wrap {
  width: 100%;
  overflow-x: auto;
}

table {
  width: 100%;
  border-collapse: collapse;
  font-size: var(--text-sm);
}

th,
td {
  text-align: left;
  padding: var(--space-xs) var(--space-sm);
  border-bottom: var(--rule-hair) solid var(--color-rule);
  vertical-align: middle;
}

th {
  font-family: var(--font-display);
  font-size: var(--text-sm);
  font-weight: 500;
  color: var(--color-muted);
  white-space: nowrap;
}

td.num,
th.num {
  text-align: right;
  font-family: var(--font-outlier);
  font-variant-numeric: tabular-nums;
}

tbody tr:last-child td {
  border-bottom: none;
}

tr.is-crashed td {
  color: var(--color-muted);
}

/* Cobalt tactile keycap buttons (no pills, no gradients, 3px key edge) */
.btn-row {
  display: flex;
  flex-wrap: wrap;
  gap: var(--space-sm);
  align-items: center;
}

.btn {
  position: relative;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-height: 40px;
  padding: 0 var(--space-md);
  border-radius: var(--radius-input);
  border: var(--rule-hair) solid var(--color-rule-2);
  background: transparent;
  color: var(--color-ink);
  font-family: var(--font-display);
  font-weight: 500;
  font-size: var(--text-sm);
  white-space: nowrap;
  cursor: pointer;
  box-shadow: var(--shadow-key);
  transition: transform var(--dur-micro) var(--ease-out), box-shadow var(--dur-micro) var(--ease-out), background-color var(--dur-micro) var(--ease-out);
}

.btn::before {
  content: "";
  position: absolute;
  inset: -2px 0;
}

.btn:hover:not(:disabled) {
  transform: translateY(-1px);
  border-color: var(--color-muted);
}

.btn:active:not(:disabled) {
  transform: translateY(3px);
  box-shadow: var(--shadow-key-press);
}

.btn:focus-visible {
  outline: 2px solid var(--color-focus);
  outline-offset: 2px;
}

.btn:disabled {
  opacity: 0.55;
  cursor: not-allowed;
}

.btn-primary {
  background: var(--color-accent);
  border-color: var(--color-accent-edge);
  color: var(--color-accent-ink);
  box-shadow: var(--shadow-key-accent);
}

.btn-primary:hover:not(:disabled) {
  background: var(--color-accent-deep);
}

.btn-secondary {
  background: transparent;
  color: var(--color-ink);
  border-color: var(--color-rule-2);
}

.btn-sm {
  min-height: 34px;
  padding: 0 var(--space-sm);
  font-size: var(--text-xs);
}

.btn-danger {
  background: transparent;
  color: var(--color-ink);
  border-color: var(--color-error);
  box-shadow: none;
}

.btn-danger:hover:not(:disabled) {
  transform: none;
  background: var(--color-error-soft);
}

.btn-recover {
  background: transparent;
  color: var(--color-ink);
  border-color: var(--color-green);
  box-shadow: none;
}

.btn-recover:hover:not(:disabled) {
  transform: none;
  background: var(--color-green-soft);
}

/* Load capacity bar: transform scaleX only */
.meter-track {
  width: 100%;
  height: 12px;
  background: var(--color-paper-3);
  border: var(--rule-hair) solid var(--color-rule-2);
  border-radius: var(--radius-input);
  overflow: hidden;
  margin-block: var(--space-md) var(--space-xs);
}

.meter-fill {
  width: 100%;
  height: 100%;
  background: var(--color-accent);
  transform-origin: left center;
  transform: scaleX(0);
  transition: transform var(--dur-short) var(--ease-out);
}

/* Forms & inputs */
.op-block + .op-block {
  margin-top: var(--space-lg);
  padding-top: var(--space-lg);
  border-top: var(--rule-hair) solid var(--color-rule);
}

.op-title {
  margin-bottom: var(--space-sm);
  color: var(--color-ink);
}

.form-grid {
  display: flex;
  flex-wrap: wrap;
  gap: var(--space-sm);
  align-items: flex-end;
  margin-bottom: var(--space-sm);
}

.field-group {
  display: flex;
  flex-direction: column;
  gap: var(--space-2xs);
}

.field-label {
  font-family: var(--font-display);
  font-size: var(--text-xs);
  font-weight: 500;
  color: var(--color-muted);
}

.field-control {
  height: 40px;
  padding: 0 var(--space-sm);
  background: var(--color-paper);
  color: var(--color-ink);
  border: var(--rule-hair) solid var(--color-muted);
  border-radius: var(--radius-input);
  font-size: var(--text-sm);
  outline: 2px solid transparent;
  outline-offset: -3px;
  transition: background-color var(--dur-micro) var(--ease-out), border-color var(--dur-micro) var(--ease-out);
}

.field-control:hover:not(:disabled) {
  background: var(--color-paper-2);
}

.field-control:focus-visible {
  outline-color: var(--color-focus);
  border-color: var(--color-accent);
}

.field-control:disabled {
  opacity: 0.55;
  cursor: not-allowed;
}

.field-control[aria-invalid="true"] {
  border-color: var(--color-error);
}

.field-num {
  font-family: var(--font-outlier);
  font-variant-numeric: tabular-nums;
  width: 112px;
}

.result-line {
  background: var(--color-paper-2);
  color: var(--color-ink-2);
  border-left: 2px solid var(--color-accent);
  padding: var(--space-xs) var(--space-sm);
  font-size: var(--text-sm);
  min-height: 2.2lh;
  display: flex;
  align-items: center;
}

.result-line.is-error {
  border-left-color: var(--color-error);
  background: var(--color-error-soft);
  color: var(--color-ink);
}

.result-line.is-ok {
  border-left-color: var(--color-green);
  background: var(--color-green-soft);
  color: var(--color-ink);
}

/* Cobalt signature dark Graphite log panel */
.log-box {
  background: var(--color-graphite);
  color: var(--color-graphite-ink);
  border: var(--rule-hair) solid var(--color-graphite-2);
  border-radius: var(--radius-input);
  padding: var(--space-sm);
  height: 240px;
  overflow-y: auto;
  font-family: var(--font-outlier);
  font-size: var(--text-xs);
}

.log-line {
  padding: var(--space-2xs) var(--space-xs);
  border-bottom: var(--rule-hair) solid var(--color-graphite-2);
}

.log-line:last-child {
  border-bottom: none;
}

.log-scale {
  background: var(--color-graphite-2);
  color: var(--color-graphite-ink);
  border-left: 2px solid var(--color-green);
  font-weight: 500;
}

.log-leader {
  background: var(--color-graphite-2);
  color: var(--color-graphite-ink);
  border-left: 2px solid var(--color-accent-2);
  font-weight: 500;
}

.log-crash {
  background: var(--color-graphite-2);
  color: var(--color-graphite-ink);
  border-left: 2px solid var(--color-error);
}

.empty-note {
  padding: var(--space-md) var(--space-xs);
  color: var(--color-muted);
  font-size: var(--text-sm);
}

.log-box .empty-note {
  color: var(--color-graphite-muted);
}

@media (max-width: 60rem) {
  .metrics-strip {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
  .metric-cell:nth-child(3) {
    border-inline-start: none;
    border-top: var(--rule-hair) solid var(--color-rule);
  }
  .metric-cell:nth-child(4) {
    border-top: var(--rule-hair) solid var(--color-rule);
  }
  .grid-main,
  .grid-split {
    grid-template-columns: minmax(0, 1fr);
  }
}

@media (max-width: 40rem) {
  .metrics-strip {
    grid-template-columns: minmax(0, 1fr);
  }
  .metric-cell + .metric-cell {
    border-inline-start: none;
    border-top: var(--rule-hair) solid var(--color-rule);
  }
}
)__ASSET_END__";
    if (name == "tokens.css") return R"__ASSET_END__(:root {
  color-scheme: light;
  --space-3xs: 2px; --space-2xs: 4px; --space-xs: 8px; --space-sm: 12px; --space-md: 16px; --space-lg: 24px; --space-xl: 40px; --space-2xl: 64px; --space-3xl: 96px; --space-4xl: 144px;
  --text-xs: 0.75rem; --text-sm: 0.875rem; --text-md: 1rem; --text-lg: 1.2rem; --text-xl: 1.44rem; --text-2xl: 1.728rem; --text-3xl: 2.074rem;
  --text-display-s: 1.44rem; --text-display: 1.728rem; --text-section: 1.2rem; --text-title: 1rem; --text-lead: 1rem;
  --ease-out: cubic-bezier(0.16, 1, 0.3, 1); --ease-in: cubic-bezier(0.7, 0, 0.84, 0); --ease-in-out: cubic-bezier(0.65, 0, 0.35, 1);
  --dur-micro: 120ms; --dur-short: 220ms; --dur-long: 420ms;
  --color-error: oklch(52% 0.19 27); --color-success: oklch(50% 0.12 150);
  --container: 1440px; --gutter: 64px; --gap-columns: 32px; --section-gap-s: 120px; --section-gap: 144px; --section-gap-l: 176px;
  --z-base: 1; --z-raised: 10; --z-dropdown: 100; --z-sticky: 200; --z-sticky-nav: 300; --z-modal: 400; --z-toast: 500; --z-tooltip: 600;
  --color-paper: oklch(98.6% 0.005 250); --color-paper-2: oklch(96.2% 0.008 252); --color-paper-3: oklch(93% 0.011 252);
  --color-ink: oklch(21% 0.028 262); --color-ink-2: oklch(36% 0.022 260); --color-muted: oklch(50% 0.018 258);
  --color-rule: oklch(90% 0.009 252); --color-rule-2: oklch(83% 0.012 252);
  --color-graphite: oklch(21% 0.024 262); --color-graphite-2: oklch(26.5% 0.026 262); --color-graphite-ink: oklch(93% 0.010 250); --color-graphite-muted: oklch(69% 0.022 256);
  --color-accent: oklch(52% 0.19 258); --color-accent-2: oklch(76% 0.13 254); --color-accent-wash: oklch(95% 0.028 256); --color-accent-ink: oklch(98.6% 0.005 250); --color-focus: oklch(52% 0.19 258);
  --color-accent-deep: oklch(43% 0.2 262); --color-accent-edge: oklch(39% 0.16 260);
  --color-accent-soft: oklch(95% 0.028 256);
  --color-water: oklch(54% 0.10 230); --color-water-soft: oklch(94% 0.024 230);
  --color-green: oklch(50% 0.12 170); --color-green-soft: oklch(94% 0.032 170);
  --color-error-soft: oklch(94% 0.035 27);
  --color-contour: oklch(60% 0.13 257); --color-contour-soft: oklch(89% 0.025 254);
  --color-candidate: oklch(43% 0.2 262);
  --color-glyph-1: oklch(89% 0.025 254); --color-glyph-2: oklch(81% 0.045 255); --color-glyph-3: oklch(70% 0.085 256); --color-glyph-4: oklch(60% 0.13 257);
  --color-graphite-glyph-1: oklch(33% 0.03 262); --color-graphite-glyph-2: oklch(41% 0.05 260); --color-graphite-glyph-3: oklch(53% 0.1 258);
  --color-key-face: oklch(99.4% 0.003 250); --color-key-edge: oklch(80% 0.016 254);
  --font-display: "Familjen Grotesk", system-ui, sans-serif; --font-body: "IBM Plex Sans", system-ui, sans-serif;
  --font-outlier: "JetBrains Mono", ui-monospace, monospace;
  --radius-card: 8px; --radius-input: 6px; --radius-pill: 999px;
  --cell-w: 8px; --cell-h: 16px; --cut: 8px; --cut-l: 16px;
  --rule-hair: 1px; --shadow-card: 0 1px 2px oklch(21% 0.028 262 / 0.06);
  --shadow-key: 0 3px 0 0 var(--color-key-edge), 0 6px 8px -4px oklch(21% 0.028 262 / 0.28), inset 0 1px 0 oklch(100% 0 0 / 0.9);
  --shadow-key-accent: 0 3px 0 0 var(--color-accent-edge), 0 6px 8px -4px oklch(30% 0.12 260 / 0.45), inset 0 1px 0 oklch(100% 0 0 / 0.22);
  --shadow-key-press: inset 0 1px 2px oklch(21% 0.028 262 / 0.14);
  --tracking-display: -0.028em; --display-weight: 600;
}
)__ASSET_END__";
    if (name == "base.css") return R"__ASSET_END__(*, *::before, *::after { box-sizing: border-box; }
html, body { overflow-x: clip; }
html { scrollbar-color: var(--color-muted) var(--color-paper); scroll-padding-top: var(--space-3xl); }
body { margin: 0; background: var(--color-paper); color: var(--color-ink); font-family: var(--font-body); font-size: var(--text-md); line-height: 1.55; -webkit-font-smoothing: antialiased; caret-color: var(--color-accent); }
::selection { background: color-mix(in oklab, var(--color-accent) 22%, transparent); color: var(--color-ink); }
:focus-visible { outline: 2px solid var(--color-focus); outline-offset: 2px; }
a { color: inherit; text-decoration-thickness: 1px; text-underline-offset: 0.16em; }
h1, h2, h3 { margin: 0; font-family: var(--font-display); font-weight: var(--display-weight, 700); font-synthesis: none; letter-spacing: var(--tracking-display); line-height: 1.05; overflow-wrap: break-word; min-width: 0; text-wrap: balance; }
h1 { max-width: 20ch; font-size: var(--text-display); } h2 { max-width: 30ch; font-size: var(--text-section); } h3 { max-width: 40ch; font-size: var(--text-title); line-height: 1.2; }
.lead { font-size: var(--text-lead); line-height: 1.45; max-width: 52ch; }
p { margin: 0; text-wrap: pretty; }
p:not([class]) { max-width: 65ch; }
img, video { max-width: 100%; height: auto; display: block; }
svg { display: block; }
[hidden] { display: none !important; }
button, input, select, textarea { font: inherit; }
.container { width: 100%; max-width: var(--container); margin-inline: auto; padding-inline: var(--gutter); box-sizing: border-box; }
.num, .figure, td.num { font-variant-numeric: tabular-nums; }
.visually-hidden { position: absolute; width: 1px; height: 1px; overflow: hidden; clip-path: inset(50%); white-space: nowrap; }
[data-slot] { position: relative; overflow: hidden; background: linear-gradient(160deg, var(--color-paper-2, var(--color-paper)), var(--color-paper-3, var(--color-paper))); }
[data-slot] > img { position: absolute; inset: 0; width: 100%; height: 100%; object-fit: cover; z-index: 1; }
[data-slot]:has(> img) > :not(img):not([data-keep]) { display: none !important; }
@media (max-width: 60rem) { :root { --gutter: 24px; --section-gap-s: 80px; --section-gap: 96px; --section-gap-l: 120px; } }
@media (max-width: 40rem) { :root { --gutter: 16px; --section-gap-s: 56px; --section-gap: 72px; --section-gap-l: 96px; --gap-columns: 16px; } }
@media (prefers-reduced-motion: reduce) { *:not(.keep-motion), *:not(.keep-motion)::before, *:not(.keep-motion)::after { animation-duration: 0.01ms !important; animation-iteration-count: 1 !important; transition-duration: 0.01ms !important; scroll-behavior: auto !important; } }
/* photographs: duotone graphite paper */
[data-slot]:has(> img) { isolation: isolate; background: var(--color-paper); outline: 1px solid color-mix(in oklab, var(--color-ink) 8%, transparent); outline-offset: -1px; }
[data-slot] > img { z-index: 0; filter: grayscale(1) contrast(1.08); mix-blend-mode: multiply; }
[data-slot]:has(> img)::before { content: ""; position: absolute; inset: 0; z-index: 1; background: var(--color-graphite); mix-blend-mode: lighten; pointer-events: none; }
)__ASSET_END__";
    return "";
}

#endif
