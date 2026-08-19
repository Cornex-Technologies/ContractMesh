import { useEffect, useMemo, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  Activity,
  AlertTriangle,
  ArrowUpRight,
  Bot,
  Boxes,
  Check,
  CheckCircle2,
  ChevronRight,
  CircleDot,
  CircleX,
  Clock3,
  Code2,
  Database,
  GitBranch,
  Globe2,
  History,
  LayoutDashboard,
  Loader2,
  LockKeyhole,
  Network,
  RefreshCw,
  Search,
  ScrollText,
  ShieldAlert,
  ShieldCheck,
  Sparkles,
  Terminal,
  Workflow,
  X,
  Zap,
} from "lucide-react";
import { Badge, Button, Card, CardContent, CardDescription, CardHeader, CardTitle, Dialog, ScrollArea, Separator, Skeleton } from "./components/ui/primitives";
import { formatDateTime, formatTime, getStatusTone, safeJson, shortId } from "./lib/utils";
import { AgentRunsPage, AuditTrailPage, ContractDiffsPage, OperatorAuthProvider } from "./pages/ControlPlanePages";

const EMPTY_STATE = {
  reload_version: 0,
  services: [],
  tasks: [],
  outbox_events: [],
  drift_events: [],
  deployments: [],
  contracts: [],
  dependencies: [],
  dependency_candidates: [],
  compatibility_work: [],
  compatibility_work_history: [],
  compatibility_incidents: [],
  audit_history: [],
  agent_dependency_graph: { nodes: [], edges: [], active_task_count: 0, active_agent_count: 0 },
  db_healthy: false,
  db_error: null,
  is_demo_mode: false,
  public_demo_enabled: false,
};

const VALID_VIEWS = new Set(["overview", "agents", "contract-diffs", "audit"]);
const GRAPH_TIME_WINDOWS = [
  { value: 1, label: "< 1 min" },
  { value: 15, label: "< 15 mins" },
  { value: 30, label: "< 30 mins" },
  { value: 60, label: "< 60 mins" },
];

function viewFromLocation() {
  const value = window.location.hash.replace(/^#\/?/, "");
  return VALID_VIEWS.has(value) ? value : "overview";
}

function getStoredOperatorToken() {
  return window.sessionStorage.getItem("codeclaim_operator_token") || "";
}

function ensureOperatorToken() {
  const existing = getStoredOperatorToken();
  if (existing) return existing;
  const entered = window.prompt("Enter the CodeClaim operator token:");
  if (!entered?.trim()) return "";
  window.sessionStorage.setItem("codeclaim_operator_token", entered.trim());
  return entered.trim();
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  const contentType = response.headers.get("content-type") || "";
  const payload = contentType.includes("application/json") ? await response.json() : await response.text();
  if (!response.ok) {
    const error = new Error(typeof payload === "object" ? payload.detail || "Request failed" : payload || response.statusText);
    error.status = response.status;
    throw error;
  }
  return payload;
}

async function fetchDashboardState(graphMinutes) {
  const query = new URLSearchParams({ graph_minutes: String(graphMinutes) });
  return fetchJson(`/api/dashboard/state?${query.toString()}`);
}

async function operatorPost(path, body) {
  const token = ensureOperatorToken();
  const headers = { "Content-Type": "application/json" };
  if (token) headers["X-Operator-Token"] = token;
  return fetchJson(path, { method: "POST", headers, body: body ? JSON.stringify(body) : undefined });
}

function App() {
  const queryClient = useQueryClient();
  const [activeView, setActiveView] = useState(viewFromLocation);
  const [autoReload, setAutoReload] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [searchResults, setSearchResults] = useState([]);
  const [searchMessage, setSearchMessage] = useState("Search the contract memory when you need to find an upstream interface.");
  const [dialog, setDialog] = useState(null);
  const [rejectionReason, setRejectionReason] = useState("");
  const [toasts, setToasts] = useState([]);
  const [graphMinutes, setGraphMinutes] = useState(30);
  const [graphService, setGraphService] = useState("");
  const [graphEndpoint, setGraphEndpoint] = useState("");
  const initialVersion = useRef(null);

  const dashboardQuery = useQuery({
    queryKey: ["dashboard-state", graphMinutes],
    queryFn: () => fetchDashboardState(graphMinutes),
    placeholderData: (previousData) => previousData,
    refetchInterval: 3500,
    refetchOnWindowFocus: true,
    retry: false,
  });

  const data = dashboardQuery.data || EMPTY_STATE;
  const dbHealthy = dashboardQuery.isSuccess && data.db_healthy !== false;
  const displayData = useMemo(() => ({ ...EMPTY_STATE, ...data }), [data]);
  const graphCatalog = useMemo(() => {
    const serviceNames = new Set();
    const endpointValues = new Set();
    const endpointsByService = new Map();
    const addOperation = (serviceName, method, path) => {
      if (serviceName) serviceNames.add(serviceName);
      if (method && path) {
        const endpoint = `${String(method).toUpperCase()} ${path}`;
        endpointValues.add(endpoint);
        if (serviceName) {
          if (!endpointsByService.has(serviceName)) endpointsByService.set(serviceName, new Set());
          endpointsByService.get(serviceName).add(endpoint);
        }
      }
    };

    (displayData.services || []).forEach((service) => addOperation(service.service_name));
    (displayData.contracts || []).forEach((contract) => addOperation(contract.service_name, contract.http_method, contract.endpoint_path));
    (displayData.dependencies || []).forEach((dependency) => {
      addOperation(dependency.consumer_service);
      addOperation(dependency.provider_service, dependency.http_method, dependency.endpoint_path);
    });
    (displayData.agent_dependency_graph?.nodes || []).forEach((node) => {
      addOperation(node.service_name, node.http_method, node.endpoint_path);
      addOperation(node.operation_service, node.http_method, node.endpoint_path);
    });

    return {
      services: [...serviceNames].sort(),
      endpoints: [...endpointValues].sort(),
      endpointsByService: Object.fromEntries([...endpointsByService.entries()].map(([service, endpoints]) => [service, [...endpoints].sort()])),
    };
  }, [displayData]);

  useEffect(() => {
    if (!dashboardQuery.data) return;
    if (initialVersion.current === null) {
      initialVersion.current = dashboardQuery.data.reload_version;
      return;
    }
    if (autoReload && dashboardQuery.data.reload_version > initialVersion.current) {
      window.location.reload();
    }
    initialVersion.current = dashboardQuery.data.reload_version;
  }, [autoReload, dashboardQuery.data]);

  useEffect(() => {
    if (!toasts.length) return undefined;
    const timer = window.setTimeout(() => setToasts((items) => items.slice(1)), 4200);
    return () => window.clearTimeout(timer);
  }, [toasts]);

  useEffect(() => {
    const syncView = () => setActiveView(viewFromLocation());
    window.addEventListener("hashchange", syncView);
    window.addEventListener("popstate", syncView);
    return () => {
      window.removeEventListener("hashchange", syncView);
      window.removeEventListener("popstate", syncView);
    };
  }, []);

  const notify = (message, tone = "info") => {
    setToasts((items) => [...items, { id: `${Date.now()}-${Math.random()}`, message, tone }]);
  };

  const refresh = () => dashboardQuery.refetch();
  const invalidate = () => queryClient.invalidateQueries({ queryKey: ["dashboard-state"] });

  const actionMutation = useMutation({
    mutationFn: ({ path, body }) => operatorPost(path, body),
    onSuccess: (_, variables) => {
      setDialog(null);
      setRejectionReason("");
      notify(variables.successMessage, "success");
      invalidate();
    },
    onError: (error, variables) => notify(`${variables.failureLabel}: ${error.message}`, "error"),
  });

  const publicDemoMutation = useMutation({
    mutationFn: () => fetchJson("/api/demo/run", { method: "POST" }),
    onSuccess: (run) => {
      notify(
        run.status === "COMPLETED"
          ? "The public demo has already completed; the current CockroachDB evidence is still available."
          : "Demo started. The dashboard will update as each durable phase commits.",
        "success",
      );
      invalidate();
    },
    onError: (error) => notify(`Public demo could not start: ${error.message}`, "error"),
  });

  const searchMutation = useMutation({
    mutationFn: (query) => displayData.public_demo_enabled
      ? fetchJson("/api/semantic-search", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query, top_k: 5 }),
      })
      : operatorPost("/api/semantic-search", { query, top_k: 5 }),
    onSuccess: (result) => {
      setSearchResults(result.results || []);
      setSearchMessage(result.simulated ? "Demo-mode results — not coordination truth." : `${result.count || 0} semantic matches from CockroachDB.`);
    },
    onError: (error) => {
      setSearchResults([]);
      setSearchMessage(`Search unavailable: ${error.message}`);
    },
  });

  const runSimulation = (kind) => {
    const isDrift = kind === "drift";
    actionMutation.mutate({
      path: isDrift ? "/api/simulate/drift" : "/api/simulate/reconcile",
      successMessage: isDrift ? "Breaking contract revision published." : "Compatibility adaptation task dispatched.",
      failureLabel: isDrift ? "Drift simulation failed" : "Adaptation simulation failed",
    });
  };

  const runPublicDemo = () => publicDemoMutation.mutate();

  const submitApproval = () => {
    if (!dialog?.taskId) return;
    actionMutation.mutate({
      path: `/tasks/${dialog.taskId}/approve`,
      body: { approved_by: dialog.approvedBy || "lead-engineer" },
      successMessage: `Task ${shortId(dialog.taskId)} approved.`,
      failureLabel: "Approval failed",
    });
  };

  const submitRejection = () => {
    if (!dialog?.taskId) return;
    actionMutation.mutate({
      path: `/tasks/${dialog.taskId}/reject`,
      body: { rejection_reason: rejectionReason.trim() || "Operator requested changes", rejected_by: "lead-engineer" },
      successMessage: `Task ${shortId(dialog.taskId)} sent back for replanning.`,
      failureLabel: "Rejection failed",
    });
  };

  const navigateTo = (view) => {
    setActiveView(view);
    const target = `#/${view}`;
    if (window.location.hash !== target) window.history.pushState({}, "", target);
  };

  const runSearch = (event) => {
    event.preventDefault();
    const query = searchQuery.trim();
    if (query) searchMutation.mutate(query);
  };

  return (
    <div className="react-dashboard">
      <header className="react-topbar">
        <div className="react-brand">
          <div className="react-brand-mark">CC</div>
          <div>
            <div className="react-eyebrow">COCKROACHDB / CONTROL PLANE</div>
            <div className="react-brand-name">CodeClaim <span>Control Mesh</span></div>
            <div className="react-brand-subtitle">Cross-service compatibility workspace</div>
          </div>
        </div>

        <nav className="react-nav" aria-label="Dashboard sections">
          {[
            ["overview", "Overview", LayoutDashboard],
            ["contract-diffs", "Contract diffs", Code2],
            ["agents", "Agent runs", Bot],
            ["audit", "Audit trail", ScrollText],
          ].map(([id, label, Icon]) => (
            <button key={id} className={`react-nav-item ${activeView === id ? "is-active" : ""}`} onClick={() => navigateTo(id)}>
              <Icon size={14} /> {label}
            </button>
          ))}
        </nav>

        <div className="react-top-actions">
          <div className={`react-status-pill ${dbHealthy ? "is-healthy" : dashboardQuery.isLoading ? "is-loading" : "is-error"}`}>
            <span className="react-pulse" />
            {dbHealthy ? "CockroachDB Connected" : dashboardQuery.isLoading ? "Connecting" : "Coordinator Offline"}
          </div>
          <Badge tone="blue">v{displayData.reload_version || "—"}</Badge>
          <label className="react-switch-label">
            <input type="checkbox" checked={autoReload} onChange={(event) => setAutoReload(event.target.checked)} />
            Auto reload
          </label>
          <Button variant="ghost" size="sm" onClick={refresh} disabled={dashboardQuery.isFetching}>
            <RefreshCw size={14} className={dashboardQuery.isFetching ? "spin" : ""} /> Refresh
          </Button>
        </div>
      </header>

      {(dashboardQuery.isError || (dashboardQuery.isSuccess && data.db_healthy === false)) && (
        <div className="react-system-banner" role="alert">
          <AlertTriangle size={16} />
          <span>{dashboardQuery.error?.message || data.db_error || "CockroachDB is unreachable."}</span>
          <Button variant="ghost" size="sm" onClick={refresh}>Retry connection</Button>
        </div>
      )}

      <OperatorAuthProvider>
        {activeView === "agents" ? <AgentRunsPage /> : activeView === "contract-diffs" ? <ContractDiffsPage /> : activeView === "audit" ? <AuditTrailPage /> : <main className="react-workspace" id="overview">
        <section className="react-hero-row">
          <div>
            <div className="react-eyebrow">LIVE OPERATIONS / {dashboardQuery.isFetching ? "SYNCING" : "STABLE"}</div>
            <h1>Compatibility control plane</h1>
            <p>One place to see contract drift, agent work, and durable event lineage across your service mesh.</p>
          </div>
          <div className="react-hero-meta">
            <div className="react-meta-label">Last synchronized</div>
            <strong>{dashboardQuery.dataUpdatedAt ? formatDateTime(dashboardQuery.dataUpdatedAt) : "Waiting for coordinator"}</strong>
            <span><Activity size={13} /> {displayData.is_demo_mode ? "Demo mode" : "Live CockroachDB state"}</span>
          </div>
        </section>

        <section className="react-metric-grid" aria-label="System overview">
          <MetricCard icon={Database} label="Database" value={dashboardQuery.isLoading ? "Checking" : dbHealthy ? "Healthy" : "Offline"} detail={dbHealthy ? "CockroachDB source of truth" : "Connection requires attention"} tone={dbHealthy ? "green" : "red"} />
          <MetricCard icon={Boxes} label="Services" value={displayData.services.length} detail={`${displayData.services.filter((service) => service.running).length} supervised online`} tone="blue" />
          <MetricCard icon={Bot} label="Agent work" value={displayData.tasks.length} detail="active compatibility tasks" tone="amber" />
          <MetricCard icon={Workflow} label="Event spine" value={displayData.outbox_events.length} detail="recent transactional events" tone="purple" />
        </section>

        <section className="react-dashboard-grid react-home-grid">
          <aside className="react-column react-rail" id="contracts">
            <Card className="react-panel">
              <CardHeader>
                <PanelHeading icon={Globe2} title="Contract mesh" detail={`${displayData.contracts.length} revisions`} tone="blue" />
              </CardHeader>
              <CardContent>
                <div className="react-trigger-box">
                  <div className="react-eyebrow">DEMO SCENARIO TRIGGERS</div>
                  <div className="react-trigger-grid">
                    {displayData.public_demo_enabled ? (
                      <Button size="sm" variant="success" onClick={runPublicDemo} disabled={publicDemoMutation.isPending}>
                        {publicDemoMutation.isPending ? <Loader2 size={14} className="spin" /> : <Sparkles size={14} />}
                        {publicDemoMutation.isPending ? "Running demo..." : "Run complete demo"}
                      </Button>
                    ) : (
                      <>
                        <Button size="sm" onClick={() => runSimulation("drift")} disabled={actionMutation.isPending}><Zap size={14} /> Breaking drift</Button>
                        <Button size="sm" variant="success" onClick={() => runSimulation("reconcile")} disabled={actionMutation.isPending}><Bot size={14} /> Adaptation</Button>
                      </>
                    )}
                  </div>
                  <p className="react-microcopy">
                    {displayData.public_demo_enabled
                      ? "Runs a bounded Billing token_id compatibility scenario. No operator token, prompt, source write, or arbitrary code execution is exposed."
                      : "Publishes through the same coordinator transaction path used by live contract changes."}
                  </p>
                </div>

                <ServiceTopology services={displayData.services} />
                <DependencyMatrix dependencies={displayData.dependencies} />
                <CandidateList candidates={displayData.dependency_candidates} />

                <div className="react-search-box">
                  <div className="react-section-kicker"><Sparkles size={13} /> Semantic contract memory</div>
                  <form onSubmit={runSearch} className="react-search-form">
                    <Search size={15} />
                    <input aria-label="Search contract memory" placeholder="Find payment, checkout, auth..." value={searchQuery} onChange={(event) => setSearchQuery(event.target.value)} />
                    <button type="submit" aria-label="Search" disabled={searchMutation.isPending}><ArrowUpRight size={15} /></button>
                  </form>
                  <p className="react-microcopy">{searchMessage}</p>
                  {searchMutation.isPending ? <Skeleton className="react-search-skeleton" /> : <SearchResults results={searchResults} />}
                </div>
              </CardContent>
            </Card>

          </aside>

          <section className="react-column react-main-column react-graph-column" aria-label="Live agent dependency graph">
            <Card className="react-panel">
              <CardHeader>
                <PanelHeading icon={Network} title="Live agent dependency graph" detail="Confirmed API-level relationships between active agents" tone="purple" />
                <Badge tone="purple">live topology</Badge>
              </CardHeader>
              <CardContent>
                <AgentDependencyGraph
                  graph={displayData.agent_dependency_graph}
                  graphCatalog={graphCatalog}
                  activeTasks={displayData.tasks}
                  graphMinutes={graphMinutes}
                  onGraphMinutesChange={setGraphMinutes}
                  serviceFilter={graphService}
                  onServiceFilterChange={(value) => { setGraphService(value); setGraphEndpoint(""); }}
                  endpointFilter={graphEndpoint}
                  onEndpointFilterChange={setGraphEndpoint}
                />
              </CardContent>
            </Card>
          </section>
        </section>
        </main>
        }
      </OperatorAuthProvider>

      <ToastViewport toasts={toasts} />

      <Dialog open={dialog?.type === "approve"} onOpenChange={(open) => !open && setDialog(null)} title="Approve compatibility plan" description={`Authorize task ${shortId(dialog?.taskId)} to continue past the checkpoint.`}>
        <div className="react-dialog-body">
          <label htmlFor="approved-by">Approver identity</label>
          <input id="approved-by" value={dialog?.approvedBy || ""} onChange={(event) => setDialog((current) => ({ ...current, approvedBy: event.target.value }))} />
          <div className="react-dialog-actions"><Button variant="ghost" onClick={() => setDialog(null)}>Cancel</Button><Button variant="success" onClick={submitApproval} disabled={actionMutation.isPending}>{actionMutation.isPending ? <Loader2 className="spin" size={14} /> : <Check size={14} />} Approve plan</Button></div>
        </div>
      </Dialog>

      <Dialog open={dialog?.type === "reject"} onOpenChange={(open) => !open && setDialog(null)} title="Reject and re-plan" description={`Send task ${shortId(dialog?.taskId)} back with explicit operator feedback.`}>
        <div className="react-dialog-body">
          <label htmlFor="rejection-reason">Reason for replanning</label>
          <textarea id="rejection-reason" rows="4" value={rejectionReason} onChange={(event) => setRejectionReason(event.target.value)} placeholder="What must the agent reconsider?" />
          <div className="react-dialog-actions"><Button variant="ghost" onClick={() => setDialog(null)}>Cancel</Button><Button variant="danger" onClick={submitRejection} disabled={actionMutation.isPending}>{actionMutation.isPending ? <Loader2 className="spin" size={14} /> : <ShieldAlert size={14} />} Reject & re-plan</Button></div>
        </div>
      </Dialog>
    </div>
  );
}

function MetricCard({ icon: Icon, label, value, detail, tone }) {
  return <div className="react-metric-card"><div className="react-metric-label"><span className={`metric-dot metric-dot-${tone}`} /><Icon size={14} /> {label}</div><strong>{value}</strong><span>{detail}</span></div>;
}

function PanelHeading({ icon: Icon, title, detail, tone }) {
  return <div className="react-panel-heading"><div className={`react-panel-icon tone-${tone}`}><Icon size={16} /></div><div><CardTitle>{title}</CardTitle><CardDescription>{detail}</CardDescription></div></div>;
}

function graphNodeTone(node) {
  if (!node.is_active) return "ghost";
  const status = String(node.status || "").toUpperCase();
  if (status.includes("FAIL") || status.includes("REPLAN")) return "warning";
  if (status.includes("APPROVAL")) return "approval";
  return "active";
}

function graphOperationLabel(node) {
  const method = node.http_method ? String(node.http_method).toUpperCase() : "";
  const path = node.endpoint_path || "API point not recorded";
  return [method, path].filter(Boolean).join(" ");
}

function graphNodeDescription(node) {
  const operation = graphOperationLabel(node);
  const owner = node.agent_id || "No active agent";
  return `${owner} working on ${node.service_name || "unknown service"}, ${operation}`;
}

function graphEndpointValue(node) {
  if (!node.http_method || !node.endpoint_path) return "";
  return `${String(node.http_method).toUpperCase()} ${node.endpoint_path}`;
}

function filterAgentDependencyGraph(graph, serviceFilter, endpointFilter) {
  const nodes = graph?.nodes || [];
  const edges = graph?.edges || [];
  if (!serviceFilter && !endpointFilter) return graph || { nodes: [], edges: [] };

  const selectedNodes = nodes.filter((node) => (
    (!serviceFilter || node.service_name === serviceFilter)
    && (!endpointFilter || graphEndpointValue(node) === endpointFilter)
  ));
  const visibleIds = new Set(selectedNodes.map((node) => node.node_id));

  // Preserve the directly connected provider/consumer node so the selected
  // endpoint remains understandable as a dependency graph, not a flat list.
  edges.forEach((edge) => {
    if (visibleIds.has(edge.from) || visibleIds.has(edge.to)) {
      visibleIds.add(edge.from);
      visibleIds.add(edge.to);
    }
  });

  const filteredNodes = nodes.filter((node) => visibleIds.has(node.node_id));
  const filteredEdges = edges.filter((edge) => visibleIds.has(edge.from) && visibleIds.has(edge.to));
  return {
    ...graph,
    nodes: filteredNodes,
    edges: filteredEdges,
    active_agent_count: new Set(filteredNodes.filter((node) => node.is_active && node.agent_id).map((node) => node.agent_id)).size,
  };
}

function AgentDependencyGraph({
  graph,
  graphCatalog,
  activeTasks,
  graphMinutes,
  onGraphMinutesChange,
  serviceFilter,
  onServiceFilterChange,
  endpointFilter,
  onEndpointFilterChange,
}) {
  const [hoveredId, setHoveredId] = useState(null);
  const filteredGraph = useMemo(() => filterAgentDependencyGraph(graph, serviceFilter, endpointFilter), [graph, serviceFilter, endpointFilter]);
  const nodes = filteredGraph?.nodes || [];
  const edges = filteredGraph?.edges || [];
  const allNodes = graph?.nodes || [];
  const serviceOptions = graphCatalog?.services || [];
  const endpointOptions = useMemo(() => [...new Set(allNodes
    .filter((node) => !serviceFilter || node.service_name === serviceFilter)
    .map(graphEndpointValue)
    .filter(Boolean))].sort(), [allNodes, serviceFilter]);
  const availableEndpointOptions = useMemo(() => {
    const selectedServiceEndpoints = new Set(serviceFilter
      ? (graphCatalog?.endpointsByService?.[serviceFilter] || [])
      : (graphCatalog?.endpoints || []));
    endpointOptions.forEach((endpoint) => selectedServiceEndpoints.add(endpoint));
    (graph?.edges || [])
      .filter((edge) => !serviceFilter || edge.consumer_service === serviceFilter || edge.provider_service === serviceFilter)
      .map((edge) => `${String(edge.http_method || "").toUpperCase()} ${edge.endpoint_path || ""}`.trim())
      .filter(Boolean)
      .forEach((endpoint) => selectedServiceEndpoints.add(endpoint));
    return [...selectedServiceEndpoints].sort();
  }, [endpointOptions, graph, graphCatalog, serviceFilter]);
  const scopedActiveTasks = useMemo(() => (activeTasks || []).filter((task) => !serviceFilter || task.service_name === serviceFilter), [activeTasks, serviceFilter]);
  const latestScopedActivity = useMemo(() => scopedActiveTasks
    .map((task) => task.updated_at || task.created_at)
    .filter(Boolean)
    .sort((left, right) => new Date(right) - new Date(left))[0], [scopedActiveTasks]);
  const emptyGraphMessage = scopedActiveTasks.length && !endpointFilter
    ? `${scopedActiveTasks.length} active ${serviceFilter ? `${serviceFilter} ` : ""}task record${scopedActiveTasks.length === 1 ? "" : "s"} exist, but none had activity in the last ${graphMinutes} minutes${latestScopedActivity ? `; latest update ${formatDateTime(latestScopedActivity)}` : ""}.`
    : "No active agents or confirmed API dependencies match these filters.";

  const layout = useMemo(() => {
    const incoming = new Set(edges.map((edge) => edge.to));
    const outgoing = new Set(edges.map((edge) => edge.from));
    const columns = { left: [], center: [], right: [] };
    nodes.forEach((node) => {
      const hasIncoming = incoming.has(node.node_id);
      const hasOutgoing = outgoing.has(node.node_id);
      const column = hasIncoming && !hasOutgoing ? "left" : hasOutgoing && !hasIncoming ? "right" : "center";
      columns[column].push(node);
    });

    const positions = new Map();
    const xByColumn = { left: 22, center: 50, right: 78 };
    const rowGap = 76;
    const top = 38;
    Object.entries(columns).forEach(([column, columnNodes]) => {
      columnNodes.forEach((node, index) => positions.set(node.node_id, { x: xByColumn[column], y: top + index * rowGap }));
    });
    const rowCount = Math.max(columns.left.length, columns.center.length, columns.right.length, 1);
    return { positions, height: Math.max(260, top + rowCount * rowGap + 28) };
  }, [nodes, edges]);

  const hovered = nodes.find((node) => node.node_id === hoveredId) || null;

  return (
    <div className="react-agent-graph-shell">
      <div className="react-agent-graph-filters" aria-label="Dependency graph filters">
        <label>
          <span>Activity window</span>
          <select aria-label="Activity window" value={graphMinutes} onChange={(event) => onGraphMinutesChange(Number(event.target.value))}>
            {GRAPH_TIME_WINDOWS.map((window) => <option value={window.value} key={window.value}>{window.label}</option>)}
          </select>
        </label>
        <label>
          <span>Project / service</span>
          <select aria-label="Project / service" value={serviceFilter} onChange={(event) => onServiceFilterChange(event.target.value)}>
            <option value="">All projects</option>
            {serviceOptions.map((service) => <option value={service} key={service}>{service}</option>)}
          </select>
        </label>
        <label>
          <span>Endpoint</span>
          <select aria-label="Endpoint" value={endpointFilter} onChange={(event) => onEndpointFilterChange(event.target.value)} disabled={!availableEndpointOptions.length}>
            <option value="">All endpoints</option>
            {availableEndpointOptions.map((endpoint) => <option value={endpoint} key={endpoint}>{endpoint}</option>)}
          </select>
        </label>
        {(serviceFilter || endpointFilter) ? <button type="button" className="react-graph-clear" onClick={() => { onServiceFilterChange(""); onEndpointFilterChange(""); }}>Clear filters</button> : null}
      </div>

      {!nodes.length ? <EmptyState icon={Network} text={emptyGraphMessage} /> : null}

      {nodes.length ? <>
      <div className="react-agent-graph-toolbar">
        <div className="react-graph-legend" aria-label="Graph legend">
          <span><i className="react-graph-legend-dot is-active" /> Active agent</span>
          <span><i className="react-graph-legend-dot is-ghost" /> Dependency target without active agent</span>
          <span><i className="react-graph-legend-line" /> Confirmed HTTP dependency</span>
        </div>
        <span className="react-graph-count">{filteredGraph.active_agent_count || 0} agents · {edges.length} confirmed links · last {graphMinutes} min</span>
      </div>

      <div className="react-agent-graph" style={{ height: `${layout.height}px` }}>
        <svg className="react-agent-graph-edges" viewBox={`0 0 100 ${layout.height}`} preserveAspectRatio="none" aria-hidden="true">
          <defs>
            <marker id="agent-graph-arrow" markerWidth="7" markerHeight="7" refX="5" refY="3.5" orient="auto">
              <path d="M0,0 L7,3.5 L0,7 z" />
            </marker>
          </defs>
          {edges.map((edge) => {
            const from = layout.positions.get(edge.from);
            const to = layout.positions.get(edge.to);
            if (!from || !to) return null;
            const middle = (from.x + to.x) / 2;
            return <path key={edge.edge_id} className="react-agent-graph-edge" d={`M ${from.x} ${from.y} C ${middle} ${from.y}, ${middle} ${to.y}, ${to.x} ${to.y}`} markerEnd="url(#agent-graph-arrow)" />;
          })}
        </svg>

        {nodes.map((node) => {
          const position = layout.positions.get(node.node_id);
          if (!position) return null;
          const tone = graphNodeTone(node);
          const isHovered = hoveredId === node.node_id;
          return (
            <button
              key={node.node_id}
              type="button"
              className={`react-agent-graph-node is-${tone} ${isHovered ? "is-hovered" : ""}`}
              style={{ left: `${position.x}%`, top: `${position.y}px` }}
              aria-label={graphNodeDescription(node)}
              onMouseEnter={() => setHoveredId(node.node_id)}
              onMouseLeave={() => setHoveredId(null)}
              onFocus={() => setHoveredId(node.node_id)}
              onBlur={() => setHoveredId(null)}
            >
              <span className="react-agent-graph-dot" aria-hidden="true"><Bot size={13} /></span>
              <span className="react-agent-graph-node-copy"><strong>{node.service_name || "Unknown service"}</strong><small>{graphOperationLabel(node)}</small></span>
            </button>
          );
        })}

        {hovered ? (
          <div className="react-agent-graph-tooltip" role="status">
            <div className="react-widget-kicker">{hovered.is_active ? "ACTIVE AGENT" : "DEPENDENCY TARGET"}</div>
            <strong>{hovered.agent_id || "No active agent"}</strong>
            <span>Project / service: {hovered.service_name || "—"}</span>
            <span>API point: {graphOperationLabel(hovered)}</span>
            {hovered.status ? <span>Status: {hovered.status}</span> : null}
            {hovered.provider_revision ? <span>Provider revision: v{hovered.provider_revision}</span> : null}
          </div>
        ) : null}
      </div>

      {edges.length ? <p className="react-graph-caption">Arrows point from the consumer agent to the provider API it depends on. Only confirmed method/path bindings create links.</p> : <p className="react-graph-caption">Active agents are shown without inferred links until a confirmed HTTP dependency is recorded.</p>}
      </> : null}
    </div>
  );
}

function ServiceTopology({ services }) {
  return <div className="react-section-block"><SectionLabel icon={Boxes} title="Registered services" count={`${services.length} total`} />{services.length ? <div className="react-list">{services.map((service) => <div className="react-list-row" key={service.service_name}><div className="react-row-icon"><GitBranch size={14} /></div><div className="react-row-main"><strong>{service.service_name}</strong><span>{service.running ? `PID ${service.pid || "active"}` : "not supervised"}</span></div><Badge tone={service.running ? "green" : "slate"}>{service.running ? "online" : "idle"}</Badge></div>)}</div> : <EmptyState icon={Boxes} text="No services registered yet." />}</div>;
}

function DependencyMatrix({ dependencies }) {
  return <div className="react-section-block"><SectionLabel icon={Globe2} title="Confirmed dependencies" count={`${dependencies.length} confirmed`} />{dependencies.length ? <div className="react-list">{dependencies.slice(0, 5).map((dependency) => <div className="react-list-row" key={dependency.dependency_id || `${dependency.consumer_service}-${dependency.endpoint_path}`}><div className="react-row-icon tone-green"><CheckCircle2 size={14} /></div><div className="react-row-main"><strong>{dependency.consumer_service} <ChevronRight size={12} /> {dependency.provider_service}</strong><span>{dependency.http_method} {dependency.endpoint_path} · v{dependency.assumed_provider_revision}</span></div><Badge tone="green">confirmed</Badge></div>)}</div> : <EmptyState icon={LockKeyhole} text="No confirmed service dependencies." />}</div>;
}

function CandidateList({ candidates }) {
  return <div className="react-section-block"><SectionLabel icon={CircleDot} title="Dependency candidates" count={`${candidates.length} review`} />{candidates.length ? <div className="react-list">{candidates.slice(0, 4).map((candidate) => <div className="react-list-row" key={candidate.dependency_id}><div className="react-row-main"><strong>{candidate.consumer_service} <ChevronRight size={12} /> {candidate.provider_service}</strong><span>{candidate.http_method} {candidate.endpoint_path}</span></div><Badge tone={getStatusTone(candidate.confirmation_status)}>{candidate.confirmation_status}</Badge></div>)}</div> : <EmptyState icon={ShieldCheck} text="No unconfirmed candidates." />}</div>;
}

function SearchResults({ results }) {
  if (!results.length) return null;
  return <div className="react-search-results">{results.map((result, index) => <div className="react-search-result" key={`${result.service_name}-${result.route_path}-${index}`}><div><strong>{result.service_name}</strong><span>{result.route_path} · v{result.revision}</span></div><Badge tone="purple">{Number(result.score || 0).toFixed(2)}</Badge></div>)}</div>;
}

function TaskList({ tasks, onApprove, onReject }) {
  if (!tasks.length) return <EmptyState icon={Bot} text="No agent repair tasks currently in flight." />;
  return <div className="react-section-block"><SectionLabel icon={Bot} title="In-flight agent tasks" count={`${tasks.length} active`} /><div className="react-list">{tasks.slice(0, 10).map((task) => <TaskRow key={task.task_id} task={task} onApprove={onApprove} onReject={onReject} />)}</div></div>;
}

function getTaskTestState(checkpoint) {
  const status = String(checkpoint.test_status || "").trim().toUpperCase();
  const completion = safeJson(checkpoint.completion);
  const evidence = safeJson(checkpoint.test_results || completion.test_results, null);

  if (evidence && typeof evidence === "object" && Object.keys(evidence).length > 0 && typeof evidence.all_passed === "boolean") {
    if (evidence.all_passed && evidence.returncode === 0) return { label: "passed", tone: "green" };
    return { label: "failed", tone: "red" };
  }

  if (["PASSED", "PASS", "SUCCESS"].includes(status)) return { label: "passed", tone: "green" };
  if (["FAILED", "FAIL", "ERROR"].includes(status)) return { label: "failed", tone: "red" };
  if (["NOT_RUN", "NOT_STARTED"].includes(status)) return { label: "not run", tone: "slate" };
  return { label: "not reported", tone: "slate" };
}

function TaskRow({ task, onApprove, onReject }) {
  const checkpoint = safeJson(task.checkpoint_state);
  const testState = getTaskTestState(checkpoint);
  const awaitingApproval = task.status === "AWAITING_APPROVAL";
  return <div className="react-task-row"><div className="react-task-top"><div className="react-task-title"><div className="react-avatar"><Bot size={14} /></div><div><strong>Task {shortId(task.task_id)}</strong><span>{task.service_name} · {task.agent_id || "agent"}</span></div></div><Badge tone={getStatusTone(task.status)}>{task.status}</Badge></div><p className="react-task-summary">{task.task_summary || "Multi-agent contract reconciliation"}</p>{task.declared_dependencies?.length ? <div className="react-chip-row">{task.declared_dependencies.map((dependency, index) => <span className="react-chip" key={`${dependency.provider_service}-${index}`}>{dependency.provider_service} · v{dependency.assumed_revision || 1}</span>)}</div> : null}<div className="react-task-meta"><span><Clock3 size={12} /> {checkpoint.phase || "planning"}</span><span><GitBranch size={12} /> plan rev {task.plan_revision || 1}</span><span><CheckCircle2 size={12} className={`tone-icon-${testState.tone}`} /> tests {testState.label}</span></div>{awaitingApproval ? <div className="react-task-actions"><Button size="sm" variant="success" onClick={() => onApprove(task)}><Check size={13} /> Approve</Button><Button size="sm" variant="danger" onClick={() => onReject(task)}><X size={13} /> Reject & re-plan</Button></div> : null}</div>;
}

function CompatibilityWorkRow({ item }) {
  const payload = safeJson(item.payload);
  const provider = item.source_service || payload.source_service || "provider";
  const method = item.http_method || payload.http_method;
  const path = item.endpoint_path || payload.endpoint_path;
  const operation = [method, path].filter(Boolean).join(" ") || "HTTP interface not recorded";
  const consumerRevision = item.consumer_assumed_revision || payload.consumer_assumed_revision;
  const revisionDetail = `provider v${item.source_contract_revision || "—"}${consumerRevision ? ` · consumer assumed v${consumerRevision}` : ""}`;

  return <div className="react-list-row"><div className="react-row-main"><strong>{item.target_service}</strong><span>{provider} · {operation}</span><span>{revisionDetail}</span></div><Badge tone={getStatusTone(item.state)}>{item.state}</Badge></div>;
}

function CompatibilityWork({ workItems, historyItems }) {
  return <>
    <div className="react-section-block">
      <SectionLabel icon={Workflow} title="Active compatibility obligations" count={`${workItems.length} active`} />
      {workItems.length ? <div className="react-list">{workItems.slice(0, 6).map((item) => <CompatibilityWorkRow item={item} key={item.work_item_id} />)}</div> : <EmptyState icon={Workflow} text="No active compatibility obligations." />}
    </div>
    <div className="react-section-block">
      <SectionLabel icon={History} title="Resolved compatibility history" count={`${historyItems.length} recent`} />
      {historyItems.length ? <div className="react-list">{historyItems.slice(0, 4).map((item) => <CompatibilityWorkRow item={item} key={item.work_item_id} />)}</div> : <EmptyState icon={History} text="No resolved compatibility work yet." />}
    </div>
  </>;
}

function IncidentList({ incidents }) {
  return <div className="react-section-block react-incident-block"><SectionLabel icon={ShieldAlert} title="Blocked incidents" count={`${incidents.length} incidents`} />{incidents.length ? <div className="react-list">{incidents.slice(0, 4).map((incident) => <div className="react-incident-row" key={incident.incident_id}><div className="react-incident-title"><AlertTriangle size={14} /><strong>{incident.incident_type || "Compatibility blocked"}</strong></div><p>{incident.missing_requirement || incident.requested_resolution || "Human decision required."}</p><Badge tone="red">human decision</Badge></div>)}</div> : <EmptyState icon={ShieldCheck} text="No human decisions required." />}</div>;
}

function DiffViewer({ event }) {
  if (!event) return <EmptyState icon={Code2} text="Awaiting schema drift detection event." />;
  const diff = safeJson(event.breaking_diff || event.diff_payload);
  const changes = [
    ...(diff.breaking_changes || []).map((change) => `− ${change.path || change.field || "schema"}: ${change.change || change.reason || "breaking change"}`),
    ...(diff.type_changes || []).map((change) => `− ${change.field}: ${change.old_type || "old"} → ${change.new_type || "new"}`),
    ...(diff.required_fields_added || []).map((change) => `− ${change.field}: required field added`),
    ...(diff.removed_fields || []).map((change) => `− ${change.field}: field removed`),
    ...(diff.optional_fields_added || []).map((change) => `+ ${change.field}: optional field added`),
  ];
  return <div className="react-diff-viewer"><div className="react-diff-title"><strong>{event.source_service || "provider"}</strong><span>v{event.old_contract_revision || 1} → v{event.new_contract_revision || 2}</span></div>{changes.length ? changes.slice(0, 6).map((change, index) => <div key={`${change}-${index}`} className={change.startsWith("+") ? "react-diff-line is-added" : "react-diff-line"}>{change}</div>) : <pre>{JSON.stringify(diff, null, 2)}</pre>}<div className="react-diff-footer"><Badge tone={getStatusTone(event.status)}>{event.status || "active"}</Badge><span>{formatTime(event.created_at)}</span></div></div>;
}

function AuditList({ items }) {
  if (!items.length) return <EmptyState icon={ScrollText} text="No audit records recorded yet." />;
  return <ScrollArea className="react-feed">{items.slice(0, 8).map((item) => { const blocked = /BLOCKED|INCOMPATIBLE|FAILED/.test(item.event_type || ""); return <div className={`react-feed-row ${blocked ? "is-danger" : ""}`} key={item.history_id || `${item.event_type}-${item.created_at}`}><div className="react-feed-top"><strong>{item.event_type}</strong><Badge tone={blocked ? "red" : "purple"}>{item.actor || "coordinator"}</Badge></div><p>{item.summary || "State transition recorded."}</p><span>evt:{shortId(item.outbox_event_id)} · {formatTime(item.created_at)}</span></div>; })}</ScrollArea>;
}

function OutboxList({ items }) {
  if (!items.length) return <EmptyState icon={Terminal} text="No outbox events recorded yet." />;
  return <ScrollArea className="react-feed">{items.slice(0, 8).map((item) => <div className="react-feed-row" key={item.event_id}><div className="react-feed-top"><strong>{item.event_type}</strong><span>{formatTime(item.created_at)}</span></div><p>{eventSummary(item)}</p></div>)}</ScrollArea>;
}

function eventSummary(event) {
  const payload = safeJson(event.payload);
  return payload.summary || payload.service_name || payload.error || payload.missing_requirement || "Transactional event recorded.";
}

function SectionLabel({ icon: Icon, title, count }) {
  return <div className="react-section-label"><span><Icon size={13} /> {title}</span><small>{count}</small></div>;
}

function EmptyState({ icon: Icon, text }) {
  return <div className="react-empty-state"><Icon size={17} /><span>{text}</span></div>;
}

function ToastViewport({ toasts }) {
  return <div className="react-toast-viewport" aria-live="polite">{toasts.map((toast) => <div className={`react-toast is-${toast.tone}`} key={toast.id}>{toast.tone === "success" ? <CheckCircle2 size={15} /> : toast.tone === "error" ? <CircleX size={15} /> : <Activity size={15} />}<span>{toast.message}</span></div>)}</div>;
}

export default App;
