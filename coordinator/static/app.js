/**
 * CodeClaim Control Mesh Frontend Driver
 * Implements secure operator session tokens (sessionStorage only), zero-flicker live polling,
 * failure-aware database health status, robust guarded JSON parsing,
 * semantic vector search, simulation triggers, and interactive human-in-the-loop task controls.
 */

let currentReloadVersion = 1;
let selectedTaskIdForApproval = null;
let selectedTaskIdForRejection = null;
let isPollingActive = true;
let isDbHealthy = true;

// ==============================================================================
// 1. Secure Operator Session Token Management (sessionStorage ONLY - No Long-Lived Storage)
// ==============================================================================

function getOperatorToken() {
  return sessionStorage.getItem("codeclaim_operator_token") || "";
}

function setOperatorToken(token) {
  if (token) {
    sessionStorage.setItem("codeclaim_operator_token", token.trim());
  } else {
    sessionStorage.removeItem("codeclaim_operator_token");
  }
}

function ensureOperatorToken() {
  let token = getOperatorToken();
  if (!token) {
    token = prompt("Please enter the Operator Authorization Token (e.g. COORDINATOR_API_KEY):");
    if (token) {
      setOperatorToken(token);
    }
  }
  return token || "";
}

// ==============================================================================
// 2. Safe Guarded JSON Parser
// ==============================================================================

function safeJsonParse(val, defaultVal = {}) {
  if (val === null || val === undefined) return defaultVal;
  if (typeof val === "object") return val;
  try {
    const parsed = JSON.parse(val);
    return typeof parsed === "object" && parsed !== null ? parsed : defaultVal;
  } catch (e) {
    return { raw: String(val) };
  }
}

// ==============================================================================
// 3. Toast Notification System
// ==============================================================================

function showToast(message, type = "info") {
  const container = document.getElementById("toast-container");
  if (!container) return;

  const toast = document.createElement("div");
  toast.className = `toast toast-${type}`;
  toast.innerText = message;

  container.appendChild(toast);
  setTimeout(() => {
    toast.style.opacity = "0";
    toast.style.transform = "translateX(100%)";
    toast.style.transition = "all 0.3s ease";
    setTimeout(() => toast.remove(), 300);
  }, 4000);
}

// ==============================================================================
// 4. Modal Management
// ==============================================================================

function openModal(modalId) {
  const modal = document.getElementById(modalId);
  if (modal) modal.classList.add("active");
}

function closeModal(modalId) {
  const modal = document.getElementById(modalId);
  if (modal) modal.classList.remove("active");
}

function openApproveModal(taskId) {
  selectedTaskIdForApproval = taskId;
  const label = document.getElementById("approve-task-id-label");
  if (label) label.innerText = taskId.substring(0, 8);
  openModal("approval-modal");
}

function openRejectModal(taskId) {
  selectedTaskIdForRejection = taskId;
  const label = document.getElementById("reject-task-id-label");
  if (label) label.innerText = taskId.substring(0, 8);
  const input = document.getElementById("reject-reason-input");
  if (input) input.value = "";
  openModal("rejection-modal");
}

// ==============================================================================
// 5. Human-In-The-Loop Task Approval & Rejection Handlers
// ==============================================================================

async function submitTaskApproval() {
  if (!selectedTaskIdForApproval) return;
  const token = ensureOperatorToken();
  const operatorInput = document.getElementById("approve-operator-input");
  const approvedBy = operatorInput ? operatorInput.value.trim() : "lead-engineer";

  try {
    const headers = { "Content-Type": "application/json" };
    if (token) headers["X-Operator-Token"] = token;

    const resp = await fetch(`/tasks/${selectedTaskIdForApproval}/approve`, {
      method: "POST",
      headers: headers,
      body: JSON.stringify({ approved_by: approvedBy }),
    });

    if (resp.ok) {
      showToast(`Task ${selectedTaskIdForApproval.substring(0, 8)} approved successfully!`, "success");
      closeModal("approval-modal");
      refreshDashboard(false);
    } else {
      const err = await resp.json().catch(() => ({}));
      showToast(`Approval failed: ${err.detail || "Unauthorized or Bad Request"}`, "error");
    }
  } catch (ex) {
    showToast(`Network error approving task: ${ex.message}`, "error");
  }
}

async function submitTaskRejection() {
  if (!selectedTaskIdForRejection) return;
  const token = ensureOperatorToken();
  const reasonInput = document.getElementById("reject-reason-input");
  const reason = reasonInput && reasonInput.value.trim() ? reasonInput.value.trim() : "Operator requested changes";

  try {
    const headers = { "Content-Type": "application/json" };
    if (token) headers["X-Operator-Token"] = token;

    const resp = await fetch(`/tasks/${selectedTaskIdForRejection}/reject`, {
      method: "POST",
      headers: headers,
      body: JSON.stringify({ rejection_reason: reason, rejected_by: "lead-engineer" }),
    });

    if (resp.ok) {
      showToast(`Task ${selectedTaskIdForRejection.substring(0, 8)} rejected. Agent re-planning triggered.`, "info");
      closeModal("rejection-modal");
      refreshDashboard(false);
    } else {
      const err = await resp.json().catch(() => ({}));
      showToast(`Rejection failed: ${err.detail || "Unauthorized or Bad Request"}`, "error");
    }
  } catch (ex) {
    showToast(`Network error rejecting task: ${ex.message}`, "error");
  }
}

// ==============================================================================
// 6. Simulation Triggers
// ==============================================================================

async function triggerSimulationDrift() {
  const token = ensureOperatorToken();
  showToast("Simulating Billing-Service v2 Breaking Contract publication...", "info");
  try {
    const headers = { "Content-Type": "application/json" };
    if (token) headers["X-Operator-Token"] = token;

    const resp = await fetch("/api/simulate/drift", {
      method: "POST",
      headers: headers,
    });
    if (resp.ok) {
      showToast("Breaking Contract v2 published! CDC drift event emitted to inbox.", "success");
      await refreshDashboard(false);
    } else {
      const err = await resp.json().catch(() => ({}));
      showToast(`Simulation failed: ${err.detail || resp.statusText}`, "error");
    }
  } catch (ex) {
    showToast(`Error triggering drift simulation: ${ex.message}`, "error");
  }
}

async function triggerSimulationReconcile() {
  const token = ensureOperatorToken();
  showToast("Triggering Agent B Orders adaptation task...", "info");
  try {
    const headers = { "Content-Type": "application/json" };
    if (token) headers["X-Operator-Token"] = token;

    const resp = await fetch("/api/simulate/reconcile", {
      method: "POST",
      headers: headers,
    });
    if (resp.ok) {
      showToast("Agent B reconciliation task started & test gates executed!", "success");
      await refreshDashboard(false);
    } else {
      const err = await resp.json().catch(() => ({}));
      showToast(`Reconciliation trigger failed: ${err.detail || resp.statusText}`, "error");
    }
  } catch (ex) {
    showToast(`Error triggering agent reconciliation: ${ex.message}`, "error");
  }
}

// ==============================================================================
// 7. Semantic Memory Vector Search
// ==============================================================================

function handleSearchKeypress(event) {
  if (event.key === "Enter") {
    triggerSemanticSearch();
  }
}

async function triggerSemanticSearch() {
  const input = document.getElementById("semantic-search-input");
  const resultsContainer = document.getElementById("semantic-results");
  if (!input || !resultsContainer) return;

  const query = input.value.trim();
  if (!query) {
    resultsContainer.innerHTML = `<div style="font-size: 12px; color: var(--text-muted); text-align: center; padding: 12px 0;">Enter a query to discover matching contracts.</div>`;
    return;
  }

  resultsContainer.innerHTML = `<div style="font-size: 12px; color: var(--text-secondary); text-align: center; padding: 12px 0;">Computing vector embeddings & querying CockroachDB...</div>`;

  try {
    const token = getOperatorToken();
    const headers = { "Content-Type": "application/json" };
    if (token) headers["X-Operator-Token"] = token;

    const resp = await fetch("/api/semantic-search", {
      method: "POST",
      headers: headers,
      body: JSON.stringify({ query: query, top_k: 4 }),
    });

    if (!resp.ok) {
      const errData = await resp.json().catch(() => ({}));
      throw new Error(errData.detail || `HTTP ${resp.status} Search Error`);
    }

    const data = await resp.json();
    const results = data.results || [];
    const isSimulated = data.simulated === true;

    if (results.length === 0) {
      resultsContainer.innerHTML = `<div style="font-size: 12px; color: var(--text-muted); text-align: center; padding: 12px 0;">No matching candidate contracts found.</div>`;
      return;
    }

    resultsContainer.innerHTML = results.map(r => {
      const score = typeof r.score === "number" ? (r.score * 100).toFixed(1) : "95.0";
      const distance = typeof r.distance === "number" ? r.distance.toFixed(4) : "0.0500";
      return `
        <div class="memory-match-card" style="${isSimulated ? 'border-color: rgba(245, 158, 11, 0.3);' : ''}">
          <div style="display: flex; justify-content: space-between; align-items: center;">
            <strong style="font-size: 13px; color: var(--accent-cockroach);">${escapeHtml(r.service_name)} (v${r.revision})</strong>
            <span class="badge ${isSimulated ? 'badge-amber' : 'badge-purple'}" style="font-size: 10px;">
              ${isSimulated ? 'SIMULATED DEMO' : 'Cosine Match'}: ${score}%
            </span>
          </div>
          <div style="font-size: 11px; color: var(--text-secondary);">${escapeHtml(r.summary || r.route_path || "Contract Schema Match")}</div>
          <div style="font-size: 10px; color: var(--text-muted);">Route: <code>${escapeHtml(r.route_path || "/v1/charges")}</code> | Dist: ${distance}</div>
          <div class="similarity-bar-container">
            <div class="similarity-bar-fill" style="width: ${Math.min(100, Math.max(10, score))}%;"></div>
          </div>
        </div>
      `;
    }).join("");

  } catch (ex) {
    resultsContainer.innerHTML = `<div style="font-size: 12px; color: var(--accent-rose); text-align: center; padding: 12px 0;">Vector search error: ${escapeHtml(ex.message)}</div>`;
  }
}

// ==============================================================================
// 8. Dashboard State Hydration & Live-Reload
// ==============================================================================

function updateSystemHealthIndicator(healthy, errorMsg) {
  isDbHealthy = healthy;
  const pill = document.getElementById("changefeed-status-pill");
  const label = document.getElementById("changefeed-label");
  const banner = document.getElementById("db-outage-banner");

  if (healthy) {
    if (pill) {
      pill.dataset.state = "healthy";
      pill.classList.remove("status-error");
      pill.classList.add("status-healthy");
    }
    if (label) label.innerText = "CDC Changefeed Active";
    if (banner) {
      banner.style.display = "none";
      banner.classList.remove("is-client-error");
    }
  } else {
    if (pill) {
      pill.dataset.state = "degraded";
      pill.classList.remove("status-healthy");
      pill.classList.add("status-error");
    }
    if (label) label.innerText = "Changefeed Degraded / Stalled";
    if (banner) {
      banner.style.display = "block";
      banner.classList.remove("is-client-error");
      banner.innerText = `⚠️ CockroachDB Unreachable — ${errorMsg || 'Changefeed stream interrupted'}`;
    }
  }
}

function updateDashboardClientError(errorMsg) {
  const banner = document.getElementById("db-outage-banner");
  if (!banner) return;
  banner.style.display = "block";
  banner.classList.add("is-client-error");
  banner.innerText = `⚠️ Dashboard render error — ${errorMsg || "refresh the page"}`;
}

function updateOverviewMetrics(data) {
  const services = Array.isArray(data.services) ? data.services : [];
  const tasks = Array.isArray(data.tasks) ? data.tasks : [];
  const events = Array.isArray(data.outbox_events) ? data.outbox_events : [];
  const dbHealthy = data.db_healthy !== false;
  const dbStatus = document.getElementById("metric-db-status");
  const dbDetail = document.getElementById("metric-db-detail");
  const serviceCount = document.getElementById("metric-service-count");
  const taskCount = document.getElementById("metric-task-count");
  const eventCount = document.getElementById("metric-event-count");

  if (dbStatus) dbStatus.innerText = dbHealthy ? "Healthy" : "Unreachable";
  if (dbDetail) dbDetail.innerText = dbHealthy ? "CockroachDB source of truth" : (data.db_error || "connection check failed");
  if (serviceCount) serviceCount.innerText = String(services.length);
  if (taskCount) taskCount.innerText = String(tasks.length);
  if (eventCount) eventCount.innerText = String(events.length);
}

function getBadgeForTaskStatus(status) {
  switch (status) {
    case "OPTIMISTIC_EXECUTING":
      return '<span class="badge badge-blue">Optimistic Executing</span>';
    case "REPLAN_REQUIRED":
    case "REPLANNING":
      return '<span class="badge badge-amber">Replanning</span>';
    case "AWAITING_APPROVAL":
      return '<span class="badge badge-purple" style="animation: pulse 1.5s infinite;">Awaiting Sign-off</span>';
    case "RECONCILED":
      return '<span class="badge badge-emerald">Reconciled</span>';
    case "FAILED":
      return '<span class="badge badge-rose">Failed</span>';
    default:
      return `<span class="badge">${escapeHtml(status)}</span>`;
  }
}

function renderServices(services) {
  const container = document.getElementById("services-list");
  const countBadge = document.getElementById("contracts-count");
  const healthBadge = document.getElementById("mesh-health-badge");
  if (!container) return;

  const items = Array.isArray(services) ? services : [];
  const running = items.filter(service => service.running).length;
  if (countBadge) countBadge.innerText = `${items.length} Services`;
  if (healthBadge) {
    healthBadge.innerText = items.length > 0 && running === items.length ? "Mesh Active" : `${running}/${items.length} online`;
    healthBadge.className = `badge ${items.length > 0 && running === items.length ? "badge-emerald" : "badge-amber"}`;
  }

  if (items.length === 0) {
    container.innerHTML = `<div class="empty-state">No supervised services registered.</div>`;
    return;
  }

  container.innerHTML = items.map(service => {
    const runningNow = service.running === true;
    const serviceName = service.service_name || "unknown-service";
    return `
      <div class="service-row">
        <span class="service-status-dot ${runningNow ? "is-online" : "is-offline"}"></span>
        <div class="service-row-main">
          <strong>${escapeHtml(serviceName)}</strong>
          <span>${runningNow ? `PID ${escapeHtml(String(service.pid || "—"))}` : "not supervised"}</span>
        </div>
        <span class="badge ${runningNow ? "badge-emerald" : "badge-gray"}">${runningNow ? "ONLINE" : "IDLE"}</span>
      </div>
    `;
  }).join("");
}

function renderContractTimeline(contracts) {
  const container = document.getElementById("contract-timeline-container");
  const countBadge = document.getElementById("timeline-count");
  if (!container) return;

  const items = contracts || [];
  if (countBadge) countBadge.innerText = `${items.length} Revisions`;

  if (items.length === 0) {
    container.innerHTML = `<div style="font-size: 12px; color: var(--text-muted); text-align: center; padding: 12px 0;">No contract revisions recorded.</div>`;
    return;
  }

  container.innerHTML = items.slice(0, 8).map(c => {
    const revNumber = c.revision_number || c.revision || 1;
    const diff = c.schema_diff || {};
    const isBreaking = c.is_breaking === true || c.status === "BREAKING" || diff.is_breaking === true || diff.classification === "BREAKING";
    const badgeClass = isBreaking ? "badge-amber" : "badge-emerald";
    const commitShort = (c.source_commit || c.git_commit || "").substring(0, 8);
    const endpoint = (c.http_method ? c.http_method + " " : "") + (c.endpoint_path || c.route_path || "/api/v" + revNumber);
    const timeStr = c.created_at ? new Date(c.created_at).toLocaleTimeString() : "";

    return `
      <div class="timeline-item" style="border-left-color: ${isBreaking ? 'var(--accent-amber)' : 'var(--accent-emerald)'};">
        <div class="timeline-header">
          <strong>${escapeHtml(c.service_name)} (v${escapeHtml(String(revNumber))})</strong>
          <span class="badge ${badgeClass}">${escapeHtml(isBreaking ? "BREAKING" : (c.is_active === false ? "INACTIVE" : (c.status || "ACTIVE")))}</span>
        </div>
        <div style="font-size: 11px; color: var(--text-secondary); font-family: var(--font-mono);">
          ${escapeHtml(endpoint)} · Commit: ${escapeHtml(commitShort || "HEAD")}
        </div>
        <div style="font-size: 10px; color: var(--text-muted); margin-top: 2px;">
          Published: ${escapeHtml(timeStr)}
        </div>
      </div>
    `;
  }).join("");
}

function renderDependencies(dependencies) {
  const container = document.getElementById("dependency-matrix-container");
  const countBadge = document.getElementById("dependency-count");
  if (!container) return;

  const items = dependencies || [];
  if (countBadge) countBadge.innerText = `${items.length} Confirmed`;

  if (items.length === 0) {
    container.innerHTML = `<div style="font-size: 12px; color: var(--text-muted); text-align: center; padding: 12px 0;">No confirmed cross-service dependencies registered yet.</div>`;
    return;
  }

  container.innerHTML = items.map(d => `
    <div class="timeline-item" style="border-left-color: var(--accent-blue);">
      <div class="timeline-header">
        <strong>${escapeHtml(d.consumer_service)} → ${escapeHtml(d.provider_service)} (v${d.assumed_provider_revision || 1})</strong>
        <span class="badge badge-emerald">${escapeHtml(d.confirmation_status || "CONFIRMED")}</span>
      </div>
      <div style="font-size: 11px; color: var(--text-secondary); font-family: var(--font-mono);">
        ${escapeHtml(d.http_method || "POST")} <code>${escapeHtml(d.endpoint_path || "/v1/charges")}</code>
      </div>
      <div style="font-size: 10px; color: var(--text-muted); margin-top: 2px;">
        Consumer Source: <code>${escapeHtml(d.consumer_source_file || "clients/billing_client.py")}</code>
      </div>
    </div>
  `).join("");
}

function renderDependencyCandidates(candidates) {
  const container = document.getElementById("dependency-candidates-container");
  const countBadge = document.getElementById("dependency-candidate-count");
  if (!container) return;

  const items = candidates || [];
  if (countBadge) countBadge.innerText = `${items.length} Candidates`;
  if (items.length === 0) {
    container.innerHTML = `<div style="font-size: 12px; color: var(--text-muted); text-align: center; padding: 12px 0;">No unconfirmed dependency candidates.</div>`;
    return;
  }

  container.innerHTML = items.map(d => `
    <div class="timeline-item" style="border-left-color: var(--accent-amber);">
      <div class="timeline-header">
        <strong>${escapeHtml(d.consumer_service)} → ${escapeHtml(d.provider_service)}</strong>
        <span class="badge badge-amber">${escapeHtml(d.confirmation_status || "UNCONFIRMED")}</span>
      </div>
      <div style="font-size: 11px; color: var(--text-secondary); font-family: var(--font-mono);">
        ${escapeHtml(d.http_method || "HTTP")} <code>${escapeHtml(d.endpoint_path || "unknown")}</code>
      </div>
    </div>
  `).join("");
}

function renderTasks(tasks) {
  const container = document.getElementById("tasks-container");
  const countBadge = document.getElementById("tasks-count");
  if (!container) return;

  if (!tasks || tasks.length === 0) {
    if (countBadge) countBadge.innerText = "0 Active";
    container.innerHTML = `<div style="font-size: 13px; color: var(--text-muted); text-align: center; padding: 32px 0;">No agent repair tasks currently in-flight.</div>`;
    return;
  }

  if (countBadge) countBadge.innerText = `${tasks.length} Active`;

  container.innerHTML = tasks.map(task => {
    const isAwaitingApproval = task.status === "AWAITING_APPROVAL";
    const checkpoint = safeJsonParse(task.checkpoint_state, {});
    const testResults = safeJsonParse(checkpoint.test_results, {});
    const dependencies = Array.isArray(task.declared_dependencies) ? task.declared_dependencies : [];
    const depsHtml = dependencies.map(dep => `
      <span class="task-dependency-chip">
        ${escapeHtml(dep.provider_service || "provider")} · v${escapeHtml(String(dep.assumed_revision || 1))}
      </span>
    `).join("");
    const returncode = testResults.returncode !== undefined ? testResults.returncode : 0;
    const hasTestEvidence = testResults.all_passed !== undefined;
    const allPassed = testResults.all_passed === true && returncode === 0;
    const testStatusDisplay = hasTestEvidence
      ? `<strong style="color: ${allPassed ? 'var(--accent-emerald)' : 'var(--accent-rose)'};">${allPassed ? 'PASSED (0)' : 'FAILED (' + returncode + ')'}</strong>`
      : `<strong style="color: var(--text-muted);">UNKNOWN</strong>`;

    return `
      <div class="card task-card" id="task-card-${escapeHtml(task.task_id)}">
        <div class="card-header">
          <span class="card-title">
            <span>🤖</span>
            <span>Task: <code>${escapeHtml(task.task_id.substring(0, 8))}</code></span>
          </span>
          ${getBadgeForTaskStatus(task.status)}
        </div>
        <div class="task-prompt">${escapeHtml(task.task_summary || "Multi-agent contract reconciliation")}</div>
        ${depsHtml ? `<div class="task-dependency-list">${depsHtml}</div>` : ''}
        <div class="task-meta" style="margin-top: 8px;">
          <span>Target: <strong>${escapeHtml(task.service_name)}</strong></span>
          <span>Agent: <strong>${escapeHtml(task.agent_id || "Agent B")}</strong></span>
          <span>Phase: <strong>${escapeHtml(checkpoint.phase || "PLANNING")}</strong></span>
          <span>Plan Rev: <strong>${task.plan_revision || 1}</strong></span>
          <span>Tests: ${testStatusDisplay}</span>
        </div>
        ${isAwaitingApproval ? `
          <div class="task-actions">
            <button class="btn btn-sm btn-success" onclick="openApproveModal('${escapeHtml(task.task_id)}')">✓ Approve Plan</button>
            <button class="btn btn-sm btn-danger" onclick="openRejectModal('${escapeHtml(task.task_id)}')">✗ Reject & Re-plan</button>
          </div>
        ` : ''}
      </div>
    `;
  }).join("");
}

function renderCompatibilityWorkflow(workItems, incidents) {
  const workContainer = document.getElementById("compatibility-work-list");
  const workCount = document.getElementById("compatibility-work-count");
  const incidentContainer = document.getElementById("compatibility-incidents-list");
  const incidentCount = document.getElementById("compatibility-incident-count");
  const items = workItems || [];
  const blocked = incidents || [];
  if (workCount) workCount.innerText = `${items.length} queued`;
  if (incidentCount) incidentCount.innerText = `${blocked.length} incidents`;

  // 1. Compatibility Work Queue
  if (workContainer) {
    workContainer.innerHTML = items.length ? items.slice(0, 8).map(item => {
      const payload = safeJsonParse(item.payload, {});
      const diff = payload.breaking_diff || {};
      const review = payload.classification === "REVIEW_REQUIRED";
      let badgeClass = "badge-blue";
      if (item.state === "COMPLETED") badgeClass = "badge-emerald";
      else if (item.state === "VERIFIED") badgeClass = "badge-purple";
      else if (item.state === "AWAITING_APPROVAL") badgeClass = "badge-amber";
      else if (item.state === "BLOCKED" || item.state === "INCOMPATIBLE") badgeClass = "badge-rose";
      
      const attemptsStr = item.dispatch_attempts ? ` (attempt ${item.dispatch_attempts}/3)` : "";
      return `
        <div class="timeline-item">
          <div class="timeline-header">
            <strong>${escapeHtml(item.target_service)}</strong>
            <span class="badge ${badgeClass}">${escapeHtml(item.state)}${escapeHtml(attemptsStr)}</span>
          </div>
          <div style="font-size:11px;color:var(--text-secondary)">
            Contract v${escapeHtml(String(item.source_contract_revision || "?"))} · ${escapeHtml(diff.diff_summary || (review ? "Compatibility review required" : "Breaking compatibility work"))}
          </div>
        </div>
      `;
    }).join("") : '<div style="font-size:12px;color:var(--text-muted);text-align:center;padding:12px 0;">No compatibility work recorded.</div>';
  }

  // 2. Structured Blocked / Incompatible Incidents (Detailed Card View)
  if (incidentContainer) {
    incidentContainer.innerHTML = blocked.length ? blocked.map(incident => {
      const evidence = safeJsonParse(incident.evidence, {});
      const payload = safeJsonParse(incident.payload, {});
      const provider = incident.provider_service || evidence.provider_service || payload.source_service || "upstream-service";
      const consumer = incident.target_service || "consumer-service";
      const oldRev = evidence.old_contract_revision || payload.old_contract_revision || 1;
      const newRev = incident.source_contract_revision || evidence.provider_contract_revision || payload.source_contract_revision || 2;
      const reasonCode = evidence.reason_code || incident.reason_code || "UNAVAILABLE_REQUIRED_INPUT";
      const missingInput = incident.unavailable_required_input || evidence.unavailable_required_input || incident.missing_requirement || "Required input missing";
      const breakingDiff = evidence.breaking_diff?.diff_summary || payload.breaking_diff?.diff_summary || evidence.diff_summary || "Required field added by provider";
      const migrationNotes = evidence.migration_notes || payload.migration_notes || evidence.breaking_diff?.migration_note || payload.breaking_diff?.migration_note || "No migration notes provided";
      const sourcesChecked = evidence.sources_checked || [];
      const worktree = evidence.worktree_path || payload.preserved_worktree || "worktrees/task-orders";
      const commit = evidence.source_commit || payload.preserved_commit || "HEAD";
      const resolution = incident.requested_resolution || "Human API/design decision required";

      return `
        <div class="card" style="border-left: 4px solid var(--accent-rose); background-color: rgba(239, 68, 68, 0.05); margin-bottom: 12px; padding: 14px;">
          <div class="card-header" style="margin-bottom: 8px;">
            <div style="display: flex; align-items: center; gap: 8px;">
              <strong style="color: #fda4af; font-size: 14px;">🚨 ${escapeHtml(provider)} (v${oldRev} → v${newRev}) → ${escapeHtml(consumer)}</strong>
            </div>
            <span class="badge badge-rose" style="font-weight: 700;">Human decision required</span>
          </div>

          <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 8px; font-size: 12px; margin-bottom: 8px;">
            <div>
              <span style="color: var(--text-muted);">Reason Code:</span>
              <strong style="color: #fca5a5;">${escapeHtml(reasonCode)}</strong>
            </div>
            <div>
              <span style="color: var(--text-muted);">Unavailable Input:</span>
              <code style="color: #f87171;">${escapeHtml(missingInput)}</code>
            </div>
          </div>

          <div style="font-size: 12px; margin-bottom: 6px;">
            <span style="color: var(--text-muted);">Exact Breaking Diff:</span>
            <div style="color: #fca5a5; font-family: var(--font-mono); font-size: 11px; margin-top: 2px;">
              ${escapeHtml(breakingDiff)}
            </div>
          </div>

          <div style="font-size: 12px; margin-bottom: 6px;">
            <span style="color: var(--text-muted);">Migration Notes:</span>
            <div style="color: var(--text-secondary); font-size: 11px; margin-top: 2px;">
              ${escapeHtml(migrationNotes)}
            </div>
          </div>

          ${sourcesChecked.length > 0 ? `
            <div style="font-size: 11px; margin-bottom: 6px;">
              <span style="color: var(--text-muted);">Agent 2 Evidence (Sources Checked):</span>
              <div style="display: flex; gap: 4px; flex-wrap: wrap; margin-top: 2px;">
                ${sourcesChecked.map(s => `<code style="font-size: 10px; background: rgba(0,0,0,0.3); padding: 2px 5px; border-radius: 3px;">${escapeHtml(s)}</code>`).join("")}
              </div>
            </div>
          ` : ''}

          <div style="display: flex; justify-content: space-between; align-items: center; font-size: 11px; color: var(--text-muted); border-top: 1px solid rgba(255,255,255,0.06); padding-top: 8px; margin-top: 8px;">
            <span>Preserved: <code>${escapeHtml(worktree)}</code> (${escapeHtml(String(commit).substring(0, 8))})</span>
            <span style="color: #f43f5e; font-weight: 600;">Status: Human decision required</span>
          </div>

          <div style="font-size: 11px; color: var(--text-primary); background: rgba(0,0,0,0.25); padding: 6px 10px; border-radius: 4px; margin-top: 6px;">
            <strong>Requested Resolution:</strong> ${escapeHtml(resolution)}
          </div>
        </div>
      `;
    }).join("") : '<div style="font-size:12px;color:var(--text-muted);text-align:center;padding:12px 0;">No human decisions required.</div>';
  }
}

function renderAuditLineage(auditHistory) {
  const container = document.getElementById("audit-lineage-list");
  if (!container) return;

  const items = auditHistory || [];
  if (items.length === 0) {
    container.innerHTML = `<div style="font-size: 12px; color: var(--text-muted); text-align: center; padding: 12px 0;">No audit records recorded yet.</div>`;
    return;
  }

  container.innerHTML = items.slice(0, 10).map(a => {
    const isBlocked = a.event_type?.includes("BLOCKED") || a.event_type?.includes("INCOMPATIBLE");
    const isPassed = a.event_type?.includes("RESULT") || a.event_type?.includes("VERIFIED") || a.event_type?.includes("COMPLETED");
    const badgeColor = isBlocked ? "badge-rose" : (isPassed ? "badge-emerald" : "badge-purple");
    const timeStr = a.created_at ? new Date(a.created_at).toLocaleTimeString() : "";

    return `
      <div class="timeline-item" style="border-left-color: ${isBlocked ? 'var(--accent-rose)' : (isPassed ? 'var(--accent-emerald)' : 'var(--accent-purple)')};">
        <div class="timeline-header">
          <span style="font-family: var(--font-mono); font-size: 11px; font-weight: 600;">${escapeHtml(a.event_type)}</span>
          <span class="badge ${badgeColor}" style="font-size: 9px;">${escapeHtml(a.actor || "harness")}</span>
        </div>
        <div style="font-size: 11px; color: var(--text-secondary); margin-top: 2px;">
          ${escapeHtml(a.summary)}
        </div>
        ${a.outbox_event_id || a.causation_id ? `
          <div style="font-size: 10px; color: var(--text-muted); font-family: var(--font-mono); margin-top: 2px;">
            Lineage: evt:${escapeHtml(String(a.outbox_event_id || "").substring(0, 8))} · cause:${escapeHtml(String(a.causation_id || "").substring(0, 8))}
          </div>
        ` : ''}
        <div style="font-size: 10px; color: var(--text-muted); margin-top: 2px;">
          ${escapeHtml(timeStr)} · Source: <strong>${escapeHtml(a.source_service || "coordinator")}</strong>
        </div>
      </div>
    `;
  }).join("");
}

function renderOutboxEvents(events) {
  const container = document.getElementById("outbox-list");
  if (!container) return;

  if (!events || events.length === 0) {
    container.innerHTML = `<div style="font-size: 12px; color: var(--text-muted); text-align: center; padding: 16px 0;">No outbox events recorded yet.</div>`;
    return;
  }

  container.innerHTML = events.slice(0, 8).map(evt => {
    const eventType = evt.event_type || "EVENT";
    let badgeClass = "badge-purple";
    if (eventType.includes("DEPLOYMENT_COMPLETED")) badgeClass = "badge-emerald";
    if (eventType.includes("ROLLED_BACK") || eventType.includes("FAILED") || eventType.includes("BLOCKED")) badgeClass = "badge-rose";
    if (eventType.includes("DRIFT") || eventType.includes("REPLAN")) badgeClass = "badge-amber";

    const payload = safeJsonParse(evt.payload, {});
    const summary = payload.summary || payload.service_name || payload.error || payload.missing_requirement || (typeof payload === 'object' ? JSON.stringify(payload).substring(0, 60) : String(payload));

    return `
      <div class="timeline-item">
        <div class="timeline-header">
          <span class="timeline-event-type">${escapeHtml(eventType)}</span>
          <span class="timeline-time">${escapeHtml(evt.created_at ? new Date(evt.created_at).toLocaleTimeString() : "")}</span>
        </div>
        <div style="font-size: 11px; color: var(--text-secondary);">${escapeHtml(summary)}</div>
      </div>
    `;
  }).join("");
}

function renderDeployments(deployments) {
  const container = document.getElementById("deployments-list");
  if (!container) return;

  if (!deployments || deployments.length === 0) {
    container.innerHTML = `<div style="font-size: 12px; color: var(--text-muted); text-align: center; padding: 12px 0;">No deployments recorded yet.</div>`;
    return;
  }

  container.innerHTML = deployments.slice(0, 6).map(dep => {
    const isHealthy = dep.status === "HEALTHY";
    return `
      <div class="timeline-item" style="border-left-color: ${isHealthy ? 'var(--accent-emerald)' : 'var(--accent-rose)'};">
        <div class="timeline-header">
          <span style="font-weight: 600; font-family: var(--font-mono); color: ${isHealthy ? '#34d399' : '#f87171'};">
            ${escapeHtml(dep.service_name)} (v${dep.reload_version || 1})
          </span>
          <span class="badge ${isHealthy ? 'badge-emerald' : 'badge-rose'}" style="font-size: 10px;">${escapeHtml(dep.status)}</span>
        </div>
        <div style="font-size: 11px; color: var(--text-muted); font-family: var(--font-mono);">
          Commit: ${escapeHtml((dep.source_commit || "").substring(0, 8))} | Time: ${escapeHtml(dep.created_at ? new Date(dep.created_at).toLocaleTimeString() : "")}
        </div>
      </div>
    `;
  }).join("");
}

function renderDiffViewer(driftEvents) {
  const container = document.getElementById("diff-display");
  const badge = document.getElementById("diff-status-badge");
  if (!container) return;

  if (!driftEvents || driftEvents.length === 0) {
    if (badge) {
      badge.className = "badge badge-amber";
      badge.innerText = "No Active Drift";
    }
    container.innerHTML = `<span style="color: var(--text-muted);">Awaiting schema drift detection event...</span>`;
    return;
  }

  const latestDrift = driftEvents[0];
  const diff = safeJsonParse(latestDrift.breaking_diff || latestDrift.diff_payload, {});
  const isBreaking = (latestDrift.status === "REPLAN_REQUIRED" || latestDrift.is_breaking !== false);
  const oldRev = latestDrift.old_contract_revision || latestDrift.from_version || 1;
  const newRev = latestDrift.new_contract_revision || latestDrift.to_version || 2;

  if (badge) {
    badge.className = isBreaking ? "badge badge-rose" : "badge badge-emerald";
    badge.innerText = isBreaking ? "Breaking Change Detected" : "Non-Breaking Evolution";
  }

  let html = `<div><strong>Diff:</strong> ${escapeHtml(latestDrift.source_service || "billing-service")} (v${oldRev} → v${newRev})</div>`;
  
  if (diff.breaking_changes && Array.isArray(diff.breaking_changes) && diff.breaking_changes.length > 0) {
    html += diff.breaking_changes.map(c => `
      <div class="diff-removed">- ${escapeHtml(c.path || c.field || (typeof c === 'string' ? c : JSON.stringify(c)))}: ${escapeHtml(c.change || c.reason || c.description || "Breaking removal/mutation")}</div>
    `).join("");
  }

  if (diff.type_changes && Array.isArray(diff.type_changes) && diff.type_changes.length > 0) {
    html += diff.type_changes.map(c => `
      <div class="diff-removed">- ${escapeHtml(c.field)}: type changed from ${escapeHtml(c.old_type)} to ${escapeHtml(c.new_type)}</div>
    `).join("");
  }

  if (diff.removed_fields && Array.isArray(diff.removed_fields) && diff.removed_fields.length > 0) {
    html += diff.removed_fields.map(c => `
      <div class="diff-removed">- ${escapeHtml(c.field)}: field removed (${escapeHtml(c.old_type || 'unknown')})</div>
    `).join("");
  }

  if (diff.required_fields_added && Array.isArray(diff.required_fields_added) && diff.required_fields_added.length > 0) {
    html += diff.required_fields_added.map(c => `
      <div class="diff-removed">- ${escapeHtml(c.field)}: required field added (${escapeHtml(c.new_type || 'unknown')})</div>
    `).join("");
  }

  if (diff.optional_fields_added && Array.isArray(diff.optional_fields_added) && diff.optional_fields_added.length > 0) {
    html += diff.optional_fields_added.map(c => `
      <div class="diff-added">+ ${escapeHtml(c.field)}: optional field added (${escapeHtml(c.new_type || 'unknown')})</div>
    `).join("");
  }

  if (!diff.breaking_changes && !diff.type_changes && !diff.removed_fields && !diff.required_fields_added && !diff.optional_fields_added) {
    html += `<pre style="font-size: 11px; margin-top: 4px;">${escapeHtml(JSON.stringify(diff, null, 2))}</pre>`;
  }

  container.innerHTML = html;
}

// ==============================================================================
// 9. Master Refresh & Polling Loop
// ==============================================================================

async function refreshDashboard(showToastFeedback = false) {
  try {
    const token = getOperatorToken();
    const headers = token ? { "X-Operator-Token": token } : {};
    const resp = await fetch("/api/dashboard/state", { headers });
    if (!resp.ok) {
      if (resp.status === 502 || resp.status === 503) {
        updateSystemHealthIndicator(false, `HTTP ${resp.status} Coordinator / CockroachDB outage`);
      } else {
        updateDashboardClientError(`HTTP ${resp.status} Coordinator response`);
      }
      return;
    }

    const data = await resp.json();
    updateSystemHealthIndicator(data.db_healthy !== false, data.db_error);
    updateOverviewMetrics(data);

    // 1. Update version badge
    if (data.reload_version !== undefined) {
      const versionEl = document.getElementById("version-number");
      if (versionEl) versionEl.innerText = data.reload_version;
      currentReloadVersion = data.reload_version;
    }

    // 2. Render all panels & views
    renderServices(data.services || []);
    renderTasks(data.tasks || []);
    renderContractTimeline(data.contracts || []);
    renderDependencies(data.dependencies || []);
    renderDependencyCandidates(data.dependency_candidates || []);
    renderOutboxEvents(data.outbox_events || []);
    renderDeployments(data.deployments || []);
    renderDiffViewer(data.drift_events || []);
    renderCompatibilityWorkflow(data.compatibility_work || [], data.compatibility_incidents || []);
    renderAuditLineage(data.audit_history || []);

    if (showToastFeedback) {
      showToast("Dashboard synchronized.", "info");
    }
  } catch (ex) {
    console.error("Dashboard refresh error:", ex);
    updateDashboardClientError(ex.message);
    showToast(`Dashboard refresh failed: ${ex.message}`, "error");
  }
}

async function pollVersionCheck() {
  if (!isPollingActive) return;

  try {
    const resp = await fetch("/deploy/version");
    if (resp.ok) {
      const data = await resp.json();
      const newVersion = data.reload_version;
      if (newVersion && newVersion > currentReloadVersion) {
        const wasInitial = (currentReloadVersion === 1);
        currentReloadVersion = newVersion;
        console.log(`Live reload triggered: version incremented to v${newVersion}`);
        if (!wasInitial) {
          showToast(`Deployment version upgraded to v${newVersion}`, "info");
        }
        
        const autoReloadToggle = document.getElementById("toggle-auto-page-reload");
        if (autoReloadToggle && autoReloadToggle.checked) {
          setTimeout(() => window.location.reload(), 500);
        } else {
          await refreshDashboard(false);
        }
      }
    }
  } catch (ex) {
    // Ignore transient network errors during background polling
  }
}

function escapeHtml(str) {
  if (str === null || str === undefined) return "";
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

// Initialize on page load
document.addEventListener("DOMContentLoaded", () => {
  console.log("CodeClaim Control Mesh UI Initialized.");
  refreshDashboard(false);

  // Background version poller every 2000ms
  setInterval(pollVersionCheck, 2000);

  // Periodic state refresh every 3500ms
  setInterval(() => refreshDashboard(false), 3500);
});
