import { createContext, useCallback, useContext, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  Activity,
  Bot,
  Braces,
  CalendarClock,
  ChevronRight,
  CircleAlert,
  Clock3,
  Code2,
  GitBranch,
  History,
  ListTree,
  ScrollText,
  Terminal,
  Workflow,
  X,
} from "lucide-react";
import { Badge, Button, Card, CardContent, CardDescription, CardHeader, CardTitle, Dialog, EmptyState, ScrollArea, Skeleton } from "../components/ui/primitives";
import { formatDateTime, formatTime, getStatusTone, safeJson, shortId } from "../lib/utils";

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

const OperatorAuthContext = createContext(null);

function getStoredOperatorToken() {
  return window.sessionStorage.getItem("codeclaim_operator_token") || "";
}

export function OperatorAuthProvider({ children }) {
  const [token, setToken] = useState(getStoredOperatorToken);
  const [authDialogOpen, setAuthDialogOpen] = useState(false);
  const [draftToken, setDraftToken] = useState("");
  const [authRevision, setAuthRevision] = useState(0);

  const requestJson = useCallback(async (url) => {
    const headers = token ? { "X-Operator-Token": token } : {};
    try {
      return await fetchJson(url, { headers });
    } catch (error) {
      if (error.status !== 401) throw error;

      // Do not keep retrying a rejected credential. The next request will be
      // anonymous and the dialog will be shown again only when the API asks
      // for operator authentication.
      window.sessionStorage.removeItem("codeclaim_operator_token");
      setToken("");
      setAuthDialogOpen(true);
      const authError = new Error("Operator authentication is required to view this page.");
      authError.status = 401;
      authError.requiresOperatorAuth = true;
      throw authError;
    }
  }, [token]);

  const authenticate = (nextToken) => {
    const value = nextToken.trim();
    if (!value) return false;
    window.sessionStorage.setItem("codeclaim_operator_token", value);
    setToken(value);
    setDraftToken("");
    setAuthDialogOpen(false);
    setAuthRevision((revision) => revision + 1);
    return true;
  };

  return (
    <OperatorAuthContext.Provider value={{ requestJson, authRevision }}>
      {children}
      <Dialog open={authDialogOpen} onOpenChange={setAuthDialogOpen} title="Operator authentication" description="This page reads protected CodeClaim operational data. The token is sent only to this coordinator and is kept for this browser tab session.">
        <form className="react-dialog-body" onSubmit={(event) => { event.preventDefault(); authenticate(draftToken); }}>
          <label htmlFor="operator-token">CodeClaim operator token</label>
          <input id="operator-token" type="password" autoComplete="current-password" value={draftToken} onChange={(event) => setDraftToken(event.target.value)} placeholder="Enter the coordinator operator token" autoFocus />
          <p className="react-auth-note">The token protects audit history, contract-diff evidence, event payloads, and operator actions from unauthenticated access.</p>
          <div className="react-dialog-actions">
            <Button type="button" variant="ghost" onClick={() => setAuthDialogOpen(false)}>Cancel</Button>
            <Button type="submit" variant="primary" disabled={!draftToken.trim()}>Continue</Button>
          </div>
        </form>
      </Dialog>
    </OperatorAuthContext.Provider>
  );
}

function useOperatorAuth() {
  const context = useContext(OperatorAuthContext);
  if (!context) throw new Error("OperatorAuthProvider is required for protected CodeClaim pages.");
  return context;
}

function useOperatorQuery(queryKey, url, options = {}) {
  const { requestJson, authRevision } = useOperatorAuth();
  return useQuery({
    queryKey: [...queryKey, authRevision],
    queryFn: () => requestJson(url),
    ...options,
  });
}

function queryString(from, to) {
  const query = new URLSearchParams();
  if (from) query.set("from", from);
  if (to) query.set("to", to);
  const value = query.toString();
  return value ? `?${value}` : "";
}

function useDateFilters() {
  const [from, setFrom] = useState("");
  const [to, setTo] = useState("");
  return {
    from,
    to,
    setFrom,
    setTo,
    query: queryString(from, to),
    clear: () => { setFrom(""); setTo(""); },
  };
}

function PageHero({ eyebrow, title, description, count, icon: Icon }) {
  return (
    <section className="react-page-hero">
      <div>
        <div className="react-eyebrow">{eyebrow}</div>
        <h1>{title}</h1>
        <p>{description}</p>
      </div>
      <div className="react-page-hero-count">
        <Icon size={17} />
        <strong>{count}</strong>
        <span>records in view</span>
      </div>
    </section>
  );
}

function DateTimeFilters({ filters, label = "Time window" }) {
  return (
    <div className="react-filter-bar" aria-label={label}>
      <div className="react-filter-title"><CalendarClock size={15} /> {label}</div>
      <label>
        <span>From</span>
        <input type="datetime-local" value={filters.from} onChange={(event) => filters.setFrom(event.target.value)} />
      </label>
      <label>
        <span>To</span>
        <input type="datetime-local" value={filters.to} onChange={(event) => filters.setTo(event.target.value)} />
      </label>
      {(filters.from || filters.to) ? <Button variant="ghost" size="sm" onClick={filters.clear}><X size={13} /> Clear</Button> : null}
    </div>
  );
}

function QueryState({ query, emptyIcon: Icon, emptyText }) {
  if (query.isLoading) return <div className="react-page-loading"><Skeleton className="react-page-skeleton" /><Skeleton className="react-page-skeleton" /><Skeleton className="react-page-skeleton" /></div>;
  if (query.isError) return <div className="react-page-error"><CircleAlert size={16} /><span>{query.error.message}</span></div>;
  if (!query.data) return <EmptyState icon={Icon} text={emptyText} />;
  return null;
}

function eventLabel(event) {
  const payload = safeJson(event?.payload);
  return payload.summary || payload.task_summary || payload.service_name || payload.target_service || payload.error || "Transactional event recorded.";
}

function EventRow({ event, onOpen }) {
  if (!event) return null;
  return (
    <button className="react-detail-event" type="button" title="Double click to inspect the stored JSON payload" onDoubleClick={(clickEvent) => { clickEvent.stopPropagation(); onOpen(event.event_id); }} onClick={(clickEvent) => clickEvent.stopPropagation()}>
      <span className="react-detail-event-icon"><Terminal size={13} /></span>
      <span className="react-detail-event-main"><strong>{event.event_type}</strong><span>{eventLabel(event)}</span></span>
      <span className="react-detail-event-meta">{formatTime(event.created_at)}<br />evt:{shortId(event.event_id)}</span>
    </button>
  );
}

function EventPayloadDialog({ eventId, onClose }) {
  const query = useOperatorQuery(["event-detail", eventId], `/api/events/${eventId}`, {
    enabled: Boolean(eventId),
    retry: false,
  });
  const payload = query.data ? {
    outbox: query.data.outbox,
    audit: query.data.audit,
    drift: query.data.drift,
  } : null;
  return (
    <Dialog open={Boolean(eventId)} onOpenChange={(open) => !open && onClose()} title="Transactional event payload" description={eventId ? `Event ${eventId} — read directly from CockroachDB.` : ""}>
      <div className="react-json-dialog-body">
        {query.isLoading ? <Skeleton className="react-json-skeleton" /> : query.isError ? <div className="react-page-error"><CircleAlert size={15} /> {query.error.message}</div> : <pre className="react-json-viewer">{JSON.stringify(payload, null, 2)}</pre>}
        <div className="react-dialog-actions"><Button variant="ghost" onClick={onClose}>Close</Button></div>
      </div>
    </Dialog>
  );
}

function ObligationDialog({ selected, onClose, onOpenEvent }) {
  if (!selected) return null;
  const obligation = selected.obligation || {};
  return (
    <Dialog open={Boolean(selected)} onOpenChange={(open) => !open && onClose()} title="Compatibility obligation" description={`${obligation.target_service || "consumer"} must adapt to ${obligation.source_service || "provider"} contract revision ${obligation.source_contract_revision || "—"}.`}>
      <div className="react-obligation-dialog">
        <div className="react-dialog-summary-grid">
          <div><span>Operation</span><strong>{obligation.http_method} {obligation.endpoint_path}</strong></div>
          <div><span>State</span><Badge tone={getStatusTone(obligation.state)}>{obligation.state}</Badge></div>
          <div><span>Obligation ID</span><strong>{shortId(obligation.work_item_id)}</strong></div>
          <div><span>Created</span><strong>{formatDateTime(obligation.created_at)}</strong></div>
        </div>

        <div className="react-detail-section">
          <div className="react-detail-section-title"><Bot size={14} /> Tasks under this obligation <small>{selected.tasks?.length || 0}</small></div>
          {selected.tasks?.length ? selected.tasks.map((task) => <div className="react-detail-task" key={task.task_id}><div><strong>Task {shortId(task.task_id)}</strong><span>{task.agent_id} · {task.service_name}</span><p>{task.task_summary}</p></div><Badge tone={getStatusTone(task.status)}>{task.status}</Badge></div>) : <EmptyState icon={Bot} text="No task has claimed this obligation yet." />}
        </div>

        <div className="react-detail-section">
          <div className="react-detail-section-title"><GitBranch size={14} /> Checkpoint history <small>{selected.checkpoints?.length || 0}</small></div>
          {selected.checkpoints?.length ? <ScrollArea className="react-detail-scroll">{selected.checkpoints.map((checkpoint) => <div className="react-detail-line" key={checkpoint.checkpoint_id}><span>{checkpoint.status} · plan rev {checkpoint.plan_revision}</span><time>{formatTime(checkpoint.created_at)}</time></div>)}</ScrollArea> : <EmptyState icon={GitBranch} text="No checkpoints recorded yet." />}
        </div>

        <div className="react-detail-section">
          <div className="react-detail-section-title"><Terminal size={14} /> Events <small>{selected.events?.length || 0}</small></div>
          {selected.events?.length ? <div className="react-detail-event-list">{selected.events.map((entry) => <EventRow key={entry.outbox.event_id} event={entry.outbox} onOpen={onOpenEvent} />)}</div> : <EmptyState icon={Terminal} text="No outbox events linked yet." />}
        </div>
      </div>
    </Dialog>
  );
}

function AgentObligationWidget({ item, onOpen, onOpenEvent }) {
  const obligation = item.obligation || {};
  const task = item.tasks?.[0];
  const operation = [obligation.http_method, obligation.endpoint_path].filter(Boolean).join(" ");
  return (
    <article className="react-full-width-widget" tabIndex={0} title="Double click to inspect tasks and checkpoints" onDoubleClick={() => onOpen(item)} onKeyDown={(event) => { if (event.key === "Enter" || event.key === " ") onOpen(item); }}>
      <div className="react-full-widget-leading"><div className="react-full-widget-icon tone-purple"><Bot size={17} /></div><div><div className="react-widget-kicker">COMPATIBILITY OBLIGATION · {shortId(obligation.work_item_id)}</div><h2>{obligation.target_service} adapts to {obligation.source_service} v{obligation.source_contract_revision}</h2><p>{operation || "HTTP interface not recorded"} · created {formatDateTime(obligation.created_at)}</p></div></div>
      <div className="react-full-widget-middle"><span>Task</span><strong>{task ? `Task ${shortId(task.task_id)}` : "Unclaimed"}</strong><small>{task?.agent_id || "Awaiting compatible harness"}</small></div>
      <div className="react-full-widget-middle"><span>Execution</span><strong>{item.checkpoints?.length || 0} checkpoints</strong><small>{task?.plan_revision ? `plan revision ${task.plan_revision}` : "No execution yet"}</small></div>
      <div className="react-full-widget-status"><Badge tone={getStatusTone(obligation.state)}>{obligation.state}</Badge><span>{item.events?.length || 0} events</span></div>
      <ChevronRight className="react-full-widget-chevron" size={18} />
      {item.events?.length ? <div className="react-widget-events"><span className="react-widget-events-label">Recent causal events</span>{item.events.slice(-3).map((entry) => <EventRow key={entry.outbox.event_id} event={entry.outbox} onOpen={onOpenEvent} />)}</div> : null}
    </article>
  );
}

export function AgentRunsPage() {
  const filters = useDateFilters();
  const [selected, setSelected] = useState(null);
  const [eventId, setEventId] = useState(null);
  const query = useOperatorQuery(["agent-runs", filters.query], `/api/agent-runs${filters.query}`, { retry: false });
  const obligations = query.data?.obligations || [];
  return (
    <main className="react-workspace react-page-workspace">
      <PageHero eyebrow="AGENT EXECUTION / OBLIGATIONS" title="Agent runs" description="Every compatibility obligation is a single operational thread. Double click an obligation to inspect its tasks, checkpoints, and causal events." count={query.data?.count ?? "—"} icon={Bot} />
      <DateTimeFilters filters={filters} label="Filter obligations by creation time" />
      <section className="react-full-widget-list" aria-label="Compatibility obligations">
        <QueryState query={query} emptyIcon={Bot} emptyText="No compatibility obligations match this time window." />
        {!query.isLoading && !query.isError && !obligations.length ? <EmptyState icon={Workflow} text="No compatibility obligations match this time window." /> : null}
        {obligations.map((item) => <AgentObligationWidget key={item.obligation.work_item_id} item={item} onOpen={setSelected} onOpenEvent={setEventId} />)}
      </section>
      <ObligationDialog selected={selected} onClose={() => setSelected(null)} onOpenEvent={setEventId} />
      <EventPayloadDialog eventId={eventId} onClose={() => setEventId(null)} />
    </main>
  );
}

function diffChanges(diff) {
  const value = safeJson(diff);
  return [
    ...(value.breaking_changes || []).map((change) => `${change.path || change.field || "schema"}: ${change.change || change.reason || "breaking change"}`),
    ...(value.type_changes || []).map((change) => `${change.field || change.path || "field"}: ${change.old_type || "old"} → ${change.new_type || "new"}`),
    ...(value.required_fields_added || []).map((change) => `${change.field || change}: required field added`),
    ...(value.removed_fields || []).map((change) => `${change.field || change}: field removed`),
    ...(value.review_reasons || []).map((reason) => `Review: ${reason}`),
  ];
}

function ContractDiffWidget({ diff, onOpen }) {
  const sourcePayload = safeJson(diff.source_event_payload);
  const changes = diffChanges(diff.breaking_diff);
  return (
    <article className="react-full-width-widget react-diff-widget" tabIndex={0} title="Double click to inspect the persisted diff and audit entries" onDoubleClick={() => onOpen(diff)} onKeyDown={(event) => { if (event.key === "Enter" || event.key === " ") onOpen(diff); }}>
      <div className="react-full-widget-leading"><div className="react-full-widget-icon tone-red"><Code2 size={17} /></div><div><div className="react-widget-kicker">CONTRACT DIFFERENCE · {shortId(diff.drift_id)}</div><h2>{diff.source_service} → {diff.target_service}</h2><p>{sourcePayload.http_method || "HTTP"} {sourcePayload.endpoint_path || "interface"} · {formatDateTime(diff.created_at)}</p></div></div>
      <div className="react-diff-revisions"><span>Revision transition</span><strong>v{diff.old_contract_revision} → v{diff.new_contract_revision}</strong><small>{diff.source_event_type || "persisted drift event"}</small></div>
      <div className="react-diff-change-summary"><span>Recorded changes</span><strong>{changes.length || 1}</strong><small>{changes[0] || safeJson(diff.breaking_diff).diff_summary || "Structured contract difference"}</small></div>
      <div className="react-full-widget-status"><Badge tone={getStatusTone(diff.status)}>{diff.status}</Badge><span>{formatTime(diff.created_at)}</span></div>
      <ChevronRight className="react-full-widget-chevron" size={18} />
    </article>
  );
}

export function ContractDiffsPage() {
  const filters = useDateFilters();
  const [selected, setSelected] = useState(null);
  const query = useOperatorQuery(["contract-diffs", filters.query], `/api/contract-diffs${filters.query}`, { retry: false });
  const detailQuery = useOperatorQuery(["contract-diff-detail", selected?.drift_id], `/api/contract-diffs/${selected?.drift_id}`, { enabled: Boolean(selected), retry: false });
  return (
    <main className="react-workspace react-page-workspace">
      <PageHero eyebrow="CONTRACTS / DIFFERENCER" title="Contract diffs" description="The complete persisted drift history, with deterministic revisions and database-backed diff evidence." count={query.data?.count ?? "—"} icon={Code2} />
      <DateTimeFilters filters={filters} label="Filter diffs by detection time" />
      <section className="react-full-widget-list" aria-label="Contract differences">
        <QueryState query={query} emptyIcon={Code2} emptyText="No contract differences match this time window." />
        {!query.isLoading && !query.isError && !(query.data?.diffs || []).length ? <EmptyState icon={Code2} text="No contract differences match this time window." /> : null}
        {(query.data?.diffs || []).map((diff) => <ContractDiffWidget key={diff.drift_id} diff={diff} onOpen={setSelected} />)}
      </section>
      <Dialog open={Boolean(selected)} onOpenChange={(open) => !open && setSelected(null)} title="Persisted contract difference" description={selected ? `Drift ${selected.drift_id} — source event and audit rows from CockroachDB.` : ""}>
        <div className="react-json-dialog-body">
          {detailQuery.isLoading ? <Skeleton className="react-json-skeleton" /> : detailQuery.isError ? <div className="react-page-error"><CircleAlert size={15} /> {detailQuery.error.message}</div> : <pre className="react-json-viewer">{JSON.stringify(detailQuery.data, null, 2)}</pre>}
          <div className="react-dialog-actions"><Button variant="ghost" onClick={() => setSelected(null)}>Close</Button></div>
        </div>
      </Dialog>
    </main>
  );
}

const LINEAGE_PAYLOAD_KEYS = [
  "correlation_id",
  "causation_id",
  "parent_event_id",
  "source_event_id",
  "outbox_event_id",
  "task_id",
  "work_item_id",
  "drift_id",
  "contract_id",
  "deployment_id",
];

function lineageKeys(kind, record) {
  const keys = new Set();
  const add = (namespace, value) => {
    if (value !== null && value !== undefined && String(value).trim()) keys.add(`${namespace}:${String(value)}`);
  };

  if (kind === "audit") {
    add("correlation", record.correlation_id);
    add("causation", record.causation_id);
    add("outbox", record.outbox_event_id);
  } else {
    add("outbox", record.event_id);
    add("aggregate", `${record.aggregate_type || ""}:${record.aggregate_id || ""}`);
    const payload = safeJson(record.payload);
    LINEAGE_PAYLOAD_KEYS.forEach((field) => add(field, payload[field]));
  }
  return keys;
}

export function groupAuditRecords(audit, outbox) {
  const outboxById = new Map(outbox.map((event) => [String(event.event_id), event]));
  const groups = [];
  const groupByKey = new Map();

  const mergeGroups = (matchingGroups) => {
    const uniqueGroups = [...new Set(matchingGroups)];
    if (!uniqueGroups.length) {
      const created = { key: `lineage:${groups.length}`, lineageKeys: new Set(), records: [], latest: null, correlationId: null, causationId: null };
      groups.push(created);
      return created;
    }
    const primary = uniqueGroups[0];
    uniqueGroups.slice(1).forEach((secondary) => {
      secondary.records.forEach((entry) => primary.records.push(entry));
      secondary.lineageKeys.forEach((key) => { primary.lineageKeys.add(key); groupByKey.set(key, primary); });
      if (!primary.latest || new Date(secondary.latest) > new Date(primary.latest)) primary.latest = secondary.latest;
      if (!primary.correlationId) primary.correlationId = secondary.correlationId;
      if (!primary.causationId) primary.causationId = secondary.causationId;
      const index = groups.indexOf(secondary);
      if (index >= 0) groups.splice(index, 1);
    });
    return primary;
  };

  const addEntry = (kind, record, extra = {}) => {
    const keys = lineageKeys(kind, record);
    const matchingGroups = [...new Set([...keys].map((key) => groupByKey.get(key)).filter(Boolean))];
    const group = mergeGroups(matchingGroups);
    keys.forEach((key) => { group.lineageKeys.add(key); groupByKey.set(key, group); });
    group.records.push({ kind, record, ...extra });
    if (!group.latest || new Date(record.created_at) > new Date(group.latest)) group.latest = record.created_at;
    if (kind === "audit") {
      group.correlationId ||= record.correlation_id;
      group.causationId ||= record.causation_id;
    } else {
      const payload = safeJson(record.payload);
      group.correlationId ||= payload.correlation_id;
      group.causationId ||= payload.causation_id;
    }
  };

  audit.forEach((record) => {
    const linked = record.outbox_event_id ? outboxById.get(String(record.outbox_event_id)) : null;
    addEntry("audit", record, { linkedOutbox: linked });
    // Keep the audit row and its transactional outbox row visibly paired in
    // the same causal group instead of hiding the outbox behind the JSON icon.
    if (linked) addEntry("outbox", linked, { linkedAudit: record });
  });
  const auditOutboxIds = new Set(audit.map((record) => String(record.outbox_event_id || "")).filter(Boolean));
  outbox.forEach((event) => {
    if (!auditOutboxIds.has(String(event.event_id))) addEntry("outbox", event);
  });

  groups.forEach((group) => {
    group.records.sort((left, right) => new Date(left.record.created_at) - new Date(right.record.created_at));
    group.key = group.correlationId ? `correlation:${group.correlationId}` : group.key;
  });
  return groups.sort((left, right) => new Date(right.latest) - new Date(left.latest));
}

function AuditGroup({ group, onOpenEvent, onOpenJson }) {
  return (
    <article className="react-audit-group">
      <div className="react-audit-group-header"><div><div className="react-widget-kicker">CAUSAL CORRELATION · {shortId(group.correlationId || group.key)}</div><h2>{group.records.length} related records</h2></div><span>{formatDateTime(group.latest)}</span></div>
      <div className="react-audit-timeline">{group.records.map((entry, index) => {
        const record = entry.record;
        const event = entry.linkedOutbox || (entry.kind === "outbox" ? record : null);
        return <div className="react-audit-timeline-row" key={`${entry.kind}-${record.history_id || record.event_id || index}`}><div className={`react-audit-node ${entry.kind === "audit" ? "is-audit" : "is-outbox"}`}>{entry.kind === "audit" ? <ScrollText size={13} /> : <Terminal size={13} />}</div><div className="react-audit-record"><div className="react-feed-top"><strong>{record.event_type}</strong><Badge tone={entry.kind === "audit" ? "green" : "purple"}>{entry.kind === "audit" ? "audit" : "outbox"}</Badge></div><p>{record.summary || eventLabel(record)}</p><span>{record.actor ? `${record.actor} · ` : ""}{formatTime(record.created_at)} · {event ? `evt:${shortId(event.event_id)}` : "no outbox link"}</span></div>{event ? <button className="react-audit-inspect" type="button" onDoubleClick={() => onOpenEvent(event.event_id)} onClick={() => onOpenEvent(event.event_id)} title="Inspect JSON payload"><Braces size={14} /></button> : <button className="react-audit-inspect" type="button" onClick={() => onOpenJson(record.schema_diff || record)} title="Inspect stored record"><Braces size={14} /></button>}</div>;
      })}</div>
    </article>
  );
}

export function AuditTrailPage() {
  const filters = useDateFilters();
  const [eventId, setEventId] = useState(null);
  const [jsonRecord, setJsonRecord] = useState(null);
  const query = useOperatorQuery(["audit-trail", filters.query], `/api/audit-trail${filters.query}`, { retry: false });
  const groups = useMemo(() => groupAuditRecords(query.data?.audit || [], query.data?.outbox || []), [query.data]);
  return (
    <main className="react-workspace react-page-workspace">
      <PageHero eyebrow="EVENT SPINE / LINEAGE" title="Audit trail" description="Audit transitions are grouped by causal correlation, with their linked transactional outbox event beneath the same lineage." count={(query.data?.audit_count || 0) + (query.data?.outbox_count || 0) || "—"} icon={History} />
      <DateTimeFilters filters={filters} label="Filter lineage by event time" />
      <section className="react-audit-groups" aria-label="Grouped audit lineage">
        <QueryState query={query} emptyIcon={History} emptyText="No lineage records match this time window." />
        {!query.isLoading && !query.isError && !groups.length ? <EmptyState icon={History} text="No lineage records match this time window." /> : null}
        {groups.map((group) => <AuditGroup key={group.key} group={group} onOpenEvent={setEventId} onOpenJson={setJsonRecord} />)}
      </section>
      <EventPayloadDialog eventId={eventId} onClose={() => setEventId(null)} />
      <Dialog open={Boolean(jsonRecord)} onOpenChange={(open) => !open && setJsonRecord(null)} title="Stored audit record" description="This record has no linked transactional outbox payload."><div className="react-json-dialog-body"><pre className="react-json-viewer">{JSON.stringify(jsonRecord, null, 2)}</pre><div className="react-dialog-actions"><Button variant="ghost" onClick={() => setJsonRecord(null)}>Close</Button></div></div></Dialog>
    </main>
  );
}
