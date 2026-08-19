import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { beforeEach, describe, expect, it, vi } from "vitest";
import App from "./App";
import { groupAuditRecords } from "./pages/ControlPlanePages";

function renderDashboard() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={queryClient}>
      <App />
    </QueryClientProvider>,
  );
}

const healthyState = {
  reload_version: 12,
  db_healthy: true,
  is_demo_mode: false,
  services: [{ service_name: "billing-service", running: true, pid: 42 }],
  tasks: [{
    task_id: "task-12345678",
    agent_id: "agent-orders",
    service_name: "orders-service",
    task_summary: "Update checkout client for Billing v2",
    status: "AWAITING_APPROVAL",
    plan_revision: 2,
    checkpoint_state: { phase: "integration", test_status: "PASSED" },
    declared_dependencies: [{ provider_service: "billing-service", assumed_revision: 1 }],
  }],
  contracts: [{ contract_revision_id: "rev-1" }],
  dependencies: [],
  dependency_candidates: [],
  compatibility_work: [],
  compatibility_incidents: [],
  outbox_events: [{ event_id: "evt-1", event_type: "CONTRACT_PUBLISHED", payload: { summary: "Billing v2 published" } }],
  drift_events: [],
  deployments: [],
  audit_history: [],
};

describe("CodeClaim React dashboard", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
    window.sessionStorage.clear();
    window.history.replaceState({}, "", "#/overview");
    vi.spyOn(window, "prompt").mockReturnValue("test-token");
  });

  it("hydrates the overview and keeps detailed operational pages behind persistent top navigation", async () => {
    const fetchMock = vi.fn().mockImplementation((url) => {
      const payload = String(url).includes("/api/agent-runs")
        ? { obligations: [], count: 0 }
        : String(url).includes("/api/contract-diffs")
          ? { diffs: [], count: 0 }
          : String(url).includes("/api/audit-trail")
            ? { audit: [], outbox: [], audit_count: 0, outbox_count: 0 }
            : healthyState;
      return Promise.resolve(new Response(JSON.stringify(payload), {
        status: 200,
        headers: { "content-type": "application/json" },
      }));
    });
    vi.stubGlobal("fetch", fetchMock);

    renderDashboard();

    expect(screen.getByRole("heading", { name: "Compatibility control plane" })).toBeInTheDocument();
    expect(await screen.findByText("CockroachDB Connected")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Agent runs/i })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Contract diffs/i })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Audit trail/i })).toBeInTheDocument();
    expect(screen.queryByText("Update checkout client for Billing v2")).not.toBeInTheDocument();
    expect(screen.queryByText("CONTRACT_PUBLISHED")).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: /Agent runs/i }));
    expect(await screen.findByRole("heading", { name: "Agent runs" })).toBeInTheDocument();
    expect(await screen.findByText("No compatibility obligations match this time window.")).toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledWith(expect.stringContaining("/api/agent-runs"), expect.anything());
  });

  it("uses an in-app auth dialog instead of a browser prompt for protected pages", async () => {
    let agentRunsAttempts = 0;
    const fetchMock = vi.fn().mockImplementation((url, options = {}) => {
      if (String(url).includes("/api/agent-runs")) {
        agentRunsAttempts += 1;
        if (agentRunsAttempts === 1) {
          return Promise.resolve(new Response(JSON.stringify({ detail: "Operator authentication required" }), {
            status: 401,
            headers: { "content-type": "application/json" },
          }));
        }
        expect(options.headers).toEqual({ "X-Operator-Token": "entered-token" });
        return Promise.resolve(new Response(JSON.stringify({ obligations: [], count: 0 }), {
          status: 200,
          headers: { "content-type": "application/json" },
        }));
      }
      return Promise.resolve(new Response(JSON.stringify(healthyState), {
        status: 200,
        headers: { "content-type": "application/json" },
      }));
    });
    vi.stubGlobal("fetch", fetchMock);

    renderDashboard();
    await screen.findByText("CockroachDB Connected");
    fireEvent.click(screen.getByRole("button", { name: /Agent runs/i }));

    expect(await screen.findByRole("heading", { name: "Operator authentication" })).toBeInTheDocument();
    expect(window.prompt).not.toHaveBeenCalled();

    fireEvent.change(screen.getByLabelText("CodeClaim operator token"), { target: { value: "entered-token" } });
    fireEvent.click(screen.getByRole("button", { name: "Continue" }));
    expect(await screen.findByText("No compatibility obligations match this time window.")).toBeInTheDocument();
    expect(agentRunsAttempts).toBe(2);
  });

  it("uses the explicit public demo read path without requesting operator credentials", async () => {
    const fetchMock = vi.fn().mockImplementation((url, options = {}) => {
      if (String(url).includes("/api/agent-runs")) {
        expect(options.headers || {}).not.toHaveProperty("X-Operator-Token");
        return Promise.resolve(new Response(JSON.stringify({ obligations: [], count: 0 }), {
          status: 200,
          headers: { "content-type": "application/json" },
        }));
      }
      return Promise.resolve(new Response(JSON.stringify({ ...healthyState, public_demo_enabled: true }), {
        status: 200,
        headers: { "content-type": "application/json" },
      }));
    });
    vi.stubGlobal("fetch", fetchMock);

    renderDashboard();
    await screen.findByText("CockroachDB Connected");
    fireEvent.click(screen.getByRole("button", { name: /Agent runs/i }));

    expect(await screen.findByRole("heading", { name: "Agent runs" })).toBeInTheDocument();
    expect(await screen.findByText("No compatibility obligations match this time window.")).toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: "Operator authentication" })).not.toBeInTheDocument();
    expect(window.prompt).not.toHaveBeenCalled();
  });

  it("launches the opt-in public demo without an operator-token prompt", async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify({
      ...healthyState,
      public_demo_enabled: true,
    }), {
      status: 202,
      headers: { "content-type": "application/json" },
    }));
    vi.stubGlobal("fetch", fetchMock);

    renderDashboard();
    expect(await screen.findByText("CockroachDB Connected")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Run complete demo" }));

    expect(window.prompt).not.toHaveBeenCalled();
    await waitFor(() => expect(fetchMock.mock.calls.some(([url]) => String(url).includes("/api/demo/run"))).toBe(true));
    const demoRequest = fetchMock.mock.calls.find(([url]) => String(url).includes("/api/demo/run"));
    expect(demoRequest[1]).toMatchObject({ method: "POST" });
    expect(demoRequest[1].headers || {}).not.toHaveProperty("X-Operator-Token");
  });

  it("does not render test status on the overview after agent execution is moved to its own page", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify({
      ...healthyState,
      tasks: [{
        ...healthyState.tasks[0],
        status: "OPTIMISTIC_EXECUTING",
        checkpoint_state: { phase: "planning", test_status: "NOT_RUN" },
      }],
    }), {
      status: 200,
      headers: { "content-type": "application/json" },
    })));

    renderDashboard();

    expect(await screen.findByText("CockroachDB Connected")).toBeInTheDocument();
    expect(screen.queryByText("tests not run")).not.toBeInTheDocument();
    expect(screen.queryByText("tests unknown")).not.toBeInTheDocument();
  });

  it("shows a clear outage state when the coordinator cannot reach CockroachDB", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify({ detail: "CockroachDB database is unreachable: timeout" }), {
      status: 503,
      headers: { "content-type": "application/json" },
    })));

    renderDashboard();

    expect(await screen.findByRole("alert")).toHaveTextContent("CockroachDB database is unreachable: timeout");
    expect(screen.getByText("Coordinator Offline")).toBeInTheDocument();
  });

  it("renders active agent nodes and confirmed API dependency links on the overview", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify({
      ...healthyState,
      agent_dependency_graph: {
        active_task_count: 2,
        active_agent_count: 2,
        nodes: [
          {
            node_id: "task:billing:work-1",
            kind: "agent",
            task_id: "task-billing",
            agent_id: "agent-a",
            service_name: "billing-service",
            operation_service: "billing-service",
            http_method: "POST",
            endpoint_path: "/v1/charges",
            status: "OPTIMISTIC_EXECUTING",
            is_active: true,
          },
          {
            node_id: "task:orders:dep-1",
            kind: "agent",
            task_id: "task-orders",
            agent_id: "agent-b",
            service_name: "orders-service",
            operation_service: "billing-service",
            http_method: "POST",
            endpoint_path: "/v1/charges",
            status: "OPTIMISTIC_EXECUTING",
            is_active: true,
          },
        ],
        edges: [{
          edge_id: "task:orders:dep-1->task:billing:work-1",
          from: "task:orders:dep-1",
          to: "task:billing:work-1",
          provider_service: "billing-service",
          consumer_service: "orders-service",
          http_method: "POST",
          endpoint_path: "/v1/charges",
          status: "CONFIRMED",
        }],
      },
    }), {
      status: 200,
      headers: { "content-type": "application/json" },
    })));

    renderDashboard();

    expect(await screen.findByRole("button", { name: /agent-b working on orders-service/i })).toBeInTheDocument();
    expect(document.querySelector(".react-graph-count")).toHaveTextContent("2 agents");
    expect(document.querySelector(".react-graph-count")).toHaveTextContent("1 confirmed links");
    fireEvent.mouseEnter(screen.getByRole("button", { name: /agent-b working on orders-service/i }));
    expect(await screen.findByText("Project / service: orders-service")).toBeInTheDocument();
    expect(screen.getByText("API point: POST /v1/charges")).toBeInTheDocument();
  });

  it("keeps graph filters selectable from the service inventory when no agents are active", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify({
      ...healthyState,
      contracts: [{ service_name: "billing-service", http_method: "POST", endpoint_path: "/v1/charges" }],
      agent_dependency_graph: { nodes: [], edges: [], active_task_count: 0, active_agent_count: 0 },
    }), {
      status: 200,
      headers: { "content-type": "application/json" },
    })));

    renderDashboard();
    await screen.findByText("CockroachDB Connected");

    const activityWindow = screen.getByRole("combobox", { name: "Activity window" });
    expect(activityWindow).toHaveValue("30");
    expect(screen.getByRole("option", { name: "< 1 min" })).toBeInTheDocument();
    expect(screen.getByRole("option", { name: "< 15 mins" })).toBeInTheDocument();
    expect(screen.getByRole("option", { name: "< 30 mins" })).toBeInTheDocument();
    expect(screen.getByRole("option", { name: "< 60 mins" })).toBeInTheDocument();

    fireEvent.change(activityWindow, { target: { value: "15" } });
    expect(activityWindow).toHaveValue("15");

    fireEvent.change(screen.getByRole("combobox", { name: "Project / service" }), { target: { value: "billing-service" } });
    expect(screen.getByRole("option", { name: "POST /v1/charges" })).toBeInTheDocument();
  });

  it("groups audit records with their outbox records and shared workflow lineage", () => {
    const groups = groupAuditRecords([
      { history_id: "audit-1", outbox_event_id: "evt-1", correlation_id: "corr-1", causation_id: "corr-1", event_type: "TASK_REGISTERED", created_at: "2026-08-18T10:00:00Z" },
      { history_id: "audit-2", outbox_event_id: "evt-2", correlation_id: "corr-1", causation_id: "corr-1", event_type: "DRIFT_DETECTED", created_at: "2026-08-18T10:01:00Z" },
    ], [
      { event_id: "evt-1", aggregate_type: "AGENT_TASK", aggregate_id: "task-1", event_type: "TASK_REGISTERED", payload: { task_id: "task-1" }, created_at: "2026-08-18T10:00:00Z" },
      { event_id: "evt-2", aggregate_type: "DRIFT_EVENT", aggregate_id: "drift-1", event_type: "DRIFT_DETECTED", payload: { task_id: "task-1", source_event_id: "evt-1" }, created_at: "2026-08-18T10:01:00Z" },
    ]);

    expect(groups).toHaveLength(1);
    expect(groups[0].records).toHaveLength(4);
    expect(groups[0].records.map((entry) => entry.kind)).toEqual(["audit", "outbox", "audit", "outbox"]);
  });
});
