import React, { useEffect, useMemo, useState } from 'react';
import { createRoot } from 'react-dom/client';
import {
  Activity,
  AlertCircle,
  BarChart3,
  CalendarDays,
  CheckCircle2,
  Download,
  ExternalLink,
  FileText,
  Link2,
  Loader2,
  Lock,
  LogOut,
  RefreshCw,
  Settings,
  ShieldAlert,
  Table2,
  UserPlus,
  Users,
  Video
} from 'lucide-react';
import './styles.css';

type View = 'overview' | 'content' | 'connections' | 'members' | 'sync' | 'account';
type LoadState = 'ready' | 'loading' | 'empty' | 'stale' | 'partial' | 'permission' | 'error';

type User = {
  id: string;
  email: string;
  display_name?: string | null;
};

type Workspace = {
  id: string;
  name: string;
  slug: string;
  role: 'owner' | 'admin' | 'analyst' | 'viewer';
};

type DashboardMetric = {
  key: string;
  label: string;
  value: number | null;
  delta: number | null;
  percent_change: number | null;
};

type DashboardData = {
  demo_data: boolean;
  connection: {
    status: string;
    reconnect_reason?: string | null;
    last_successful_sync_at?: string | null;
    next_sync_at?: string | null;
  };
  latest_sync: {
    status: string;
    started_at: string;
    finished_at?: string | null;
  } | null;
  metrics: DashboardMetric[];
  trend: Array<{ observed_at: string; follower_count: number | null; likes_count: number | null }>;
  top_content: ContentRow[];
};

type ContentRow = {
  id: string;
  provider_content_id: string;
  published_at: string | null;
  title: string | null;
  description: string | null;
  share_url: string | null;
  observed_at: string | null;
  view_count: number | null;
  like_count: number | null;
  comment_count: number | null;
  share_count: number | null;
  engagement_rate: number | null;
};

type ContentData = {
  rows: ContentRow[];
  total: number;
};

type SyncRun = {
  id: string;
  trigger_type: string;
  status: string;
  started_at: string;
  finished_at: string | null;
  duration_ms: number | null;
  content_seen_count: number;
  error_category?: string | null;
};

type Member = {
  user_id: string;
  email: string;
  display_name?: string | null;
  role: Workspace['role'];
};

const views: Array<{ id: View; label: string; icon: React.ComponentType<{ size?: number }> }> = [
  { id: 'overview', label: 'Overview', icon: BarChart3 },
  { id: 'content', label: 'Content', icon: Video },
  { id: 'connections', label: 'Connections', icon: Link2 },
  { id: 'members', label: 'Members', icon: Users },
  { id: 'sync', label: 'Sync history', icon: Activity },
  { id: 'account', label: 'Account', icon: Settings }
];

async function api(path: string, options: RequestInit = {}) {
  const response = await fetch(path, {
    ...options,
    credentials: 'include',
    headers: {
      'content-type': 'application/json',
      ...(options.headers || {})
    }
  });
  const text = await response.text();
  const body = text ? JSON.parse(text) : {};
  if (!response.ok) {
    throw new Error(body.error || 'request_failed');
  }
  return body;
}

function readCookie(name: string) {
  return document.cookie
    .split(';')
    .map(value => value.trim())
    .find(value => value.startsWith(`${name}=`))
    ?.slice(name.length + 1) || '';
}

function formatNumber(value: number | null | undefined) {
  if (value === null || value === undefined) return 'N/A';
  return new Intl.NumberFormat().format(value);
}

function formatDate(value?: string | null) {
  if (!value) return 'N/A';
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: 'medium',
    timeStyle: 'short'
  }).format(new Date(value));
}

function resolveLoadState(dashboard: DashboardData | null): LoadState {
  if (!dashboard) return 'empty';
  if (dashboard.latest_sync?.status === 'failed') return 'error';
  if (dashboard.latest_sync?.status === 'partial') return 'partial';
  if (dashboard.demo_data && dashboard.trend.length > 0) return 'ready';
  if (dashboard.connection.status === 'disconnected') return 'empty';
  if (dashboard.connection.status === 'reconnect_required') return 'stale';
  const lastSuccessful = dashboard.connection.last_successful_sync_at
    ? new Date(dashboard.connection.last_successful_sync_at).getTime()
    : 0;
  if (lastSuccessful && Date.now() - lastSuccessful > 30 * 60 * 60 * 1000) return 'stale';
  return dashboard.trend.length > 0 ? 'ready' : 'empty';
}

function App() {
  const [user, setUser] = useState<User | null>(null);
  const [csrf, setCsrf] = useState<string>('');
  const [email, setEmail] = useState('');
  const [token, setToken] = useState('');
  const [workspaceName, setWorkspaceName] = useState('');
  const [workspaces, setWorkspaces] = useState<Workspace[]>([]);
  const [activeWorkspaceId, setActiveWorkspaceId] = useState<string>('');
  const [view, setView] = useState<View>('overview');
  const [state, setState] = useState<LoadState>('empty');
  const [dashboard, setDashboard] = useState<DashboardData | null>(null);
  const [content, setContent] = useState<ContentData | null>(null);
  const [syncRuns, setSyncRuns] = useState<SyncRun[]>([]);
  const [members, setMembers] = useState<Member[]>([]);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState('');

  const activeWorkspace = useMemo(
    () => workspaces.find(workspace => workspace.id === activeWorkspaceId) || workspaces[0],
    [activeWorkspaceId, workspaces]
  );

  useEffect(() => {
    let cancelled = false;
    async function resumeSession() {
      try {
        const session = await api('/api/session');
        if (cancelled) return;
        setUser(session.user);
        setCsrf(readCookie('sis_csrf'));
        const workspaceResult = await api('/api/workspaces');
        if (cancelled) return;
        setWorkspaces(workspaceResult.workspaces);
        setActiveWorkspaceId(workspaceResult.workspaces[0]?.id || '');
      } catch {
        if (!cancelled) {
          setUser(null);
          setCsrf('');
        }
      }
    }
    void resumeSession();
    return () => {
      cancelled = true;
    };
  }, []);

  async function loadWorkspaces() {
    const workspaceResult = await api('/api/workspaces');
    setWorkspaces(workspaceResult.workspaces);
    setActiveWorkspaceId(workspaceResult.workspaces[0]?.id || '');
  }

  async function loadWorkspaceData(workspaceId: string, role?: Workspace['role']) {
    setState('loading');
    setMessage('');
    try {
      const [dashboardResult, contentResult, syncResult] = await Promise.all([
        api(`/api/workspaces/${workspaceId}/dashboard?range=30d`),
        api(`/api/workspaces/${workspaceId}/content?sort=views&direction=desc`),
        api(`/api/workspaces/${workspaceId}/sync-runs`)
      ]);
      setDashboard(dashboardResult);
      setContent(contentResult);
      setSyncRuns(syncResult.sync_runs);
      if (role === 'owner' || role === 'admin') {
        const memberResult = await api(`/api/workspaces/${workspaceId}/members`);
        setMembers(memberResult.members);
      } else {
        setMembers([]);
      }
      setState(resolveLoadState(dashboardResult));
    } catch (error) {
      const text = error instanceof Error ? error.message : 'load_failed';
      setMessage(text);
      setState(text === 'permission_denied' ? 'permission' : 'error');
    }
  }

  useEffect(() => {
    if (!user || !activeWorkspace) return;
    void loadWorkspaceData(activeWorkspace.id, activeWorkspace.role);
  }, [user, activeWorkspace?.id]);

  async function requestLink(event: React.FormEvent) {
    event.preventDefault();
    setBusy(true);
    setMessage('');
    try {
      const result = await api('/api/auth/magic-link/request', {
        method: 'POST',
        body: JSON.stringify({ email })
      });
      setToken(result.dev_token || '');
      setMessage(result.dev_token ? 'Development link ready.' : 'Magic link requested.');
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'request_failed');
    } finally {
      setBusy(false);
    }
  }

  async function verifyLink(event: React.FormEvent) {
    event.preventDefault();
    setBusy(true);
    setMessage('');
    try {
      const result = await api('/api/auth/magic-link/verify', {
        method: 'POST',
        body: JSON.stringify({ token })
      });
      setUser(result.user);
      setCsrf(result.csrf_token);
      await loadWorkspaces();
      setMessage('');
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'verify_failed');
    } finally {
      setBusy(false);
    }
  }

  async function createWorkspace(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const formElement = event.currentTarget;
    const form = new FormData(formElement);
    const submittedName = String(form.get('workspace_name') || workspaceName).trim();
    setBusy(true);
    setMessage('');
    try {
      const result = await api('/api/workspaces', {
        method: 'POST',
        headers: { 'x-csrf-token': csrf },
        body: JSON.stringify({ name: submittedName })
      });
      const next = [...workspaces, result.workspace];
      setWorkspaces(next);
      setActiveWorkspaceId(result.workspace.id);
      setWorkspaceName('');
      formElement.reset();
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'workspace_failed');
    } finally {
      setBusy(false);
    }
  }

  async function signOut() {
    setBusy(true);
    try {
      await api('/api/sign-out', {
        method: 'POST',
        headers: { 'x-csrf-token': csrf },
        body: JSON.stringify({})
      });
    } catch {
      // The local shell still clears client state when the server session is already gone.
    } finally {
      setUser(null);
      setCsrf('');
      setWorkspaces([]);
      setActiveWorkspaceId('');
      setBusy(false);
    }
  }

  async function manualSync() {
    if (!activeWorkspace) return;
    setBusy(true);
    setMessage('');
    try {
      await api(`/api/workspaces/${activeWorkspace.id}/sync-runs`, {
        method: 'POST',
        headers: { 'x-csrf-token': csrf },
        body: JSON.stringify({})
      });
      await loadWorkspaceData(activeWorkspace.id, activeWorkspace.role);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'sync_failed');
    } finally {
      setBusy(false);
    }
  }

  async function startConnection() {
    if (!activeWorkspace) return;
    setBusy(true);
    setMessage('');
    try {
      const result = await api(`/api/workspaces/${activeWorkspace.id}/connections/tiktok/start`, {
        method: 'POST',
        headers: { 'x-csrf-token': csrf },
        body: JSON.stringify({ return_path: `/app?workspace=${activeWorkspace.id}` })
      });
      window.location.href = result.authorization_url;
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'connection_failed');
      setBusy(false);
    }
  }

  async function disconnectConnection() {
    if (!activeWorkspace) return;
    setBusy(true);
    setMessage('');
    try {
      await api(`/api/workspaces/${activeWorkspace.id}/connections/tiktok`, {
        method: 'DELETE',
        headers: { 'x-csrf-token': csrf },
        body: JSON.stringify({})
      });
      await loadWorkspaceData(activeWorkspace.id, activeWorkspace.role);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'disconnect_failed');
    } finally {
      setBusy(false);
    }
  }

  if (!user) {
    return (
      <main className="public-shell">
        <section className="product-panel" aria-labelledby="product-title">
          <img src="/logo.png" alt="" className="brand-mark" />
          <div>
            <p className="eyebrow">Social Insights Studio</p>
            <h1 id="product-title">TikTok analytics workspace</h1>
            <p className="lede">
              Connect accounts, sync performance history, and review content health from one secure dashboard.
            </p>
          </div>
          <div className="status-row" aria-label="Platform status">
            <span><CheckCircle2 size={16} /> Secure sessions</span>
            <span><Lock size={16} /> Server-side tokens</span>
            <span><Activity size={16} /> Background sync</span>
          </div>
        </section>

        <section className="auth-panel" aria-labelledby="signin-title">
          <h2 id="signin-title">Sign in</h2>
          <form onSubmit={requestLink} className="stack">
            <label>
              Email
              <input value={email} onChange={event => setEmail(event.target.value)} type="email" required />
            </label>
            <button type="submit" disabled={busy}>
              {busy ? <Loader2 className="spin" size={18} /> : <FileText size={18} />}
              Request link
            </button>
          </form>
          <form onSubmit={verifyLink} className="stack">
            <label>
              Magic link token
              <input value={token} onChange={event => setToken(event.target.value)} required />
            </label>
            <button type="submit" disabled={busy || !token}>
              <CheckCircle2 size={18} />
              Verify
            </button>
          </form>
          {message && <p className="notice">{message}</p>}
        </section>
      </main>
    );
  }

  return (
    <main className="app-shell">
      <aside className="sidebar" aria-label="Primary">
        <div className="brand-row">
          <img src="/logo.png" alt="" />
          <strong>Social Insights Studio</strong>
        </div>
        <label className="workspace-select">
          Workspace
          <select value={activeWorkspace?.id || ''} onChange={event => setActiveWorkspaceId(event.target.value)}>
            {workspaces.map(workspace => (
              <option key={workspace.id} value={workspace.id}>{workspace.name}</option>
            ))}
          </select>
        </label>
        <nav>
          {views.map(item => {
            const Icon = item.icon;
            return (
              <button
                key={item.id}
                className={view === item.id ? 'active' : ''}
                onClick={() => setView(item.id)}
                title={item.label}
              >
                <Icon size={18} />
                <span>{item.label}</span>
              </button>
            );
          })}
        </nav>
      </aside>

      <section className="workspace">
        <header className="topbar">
          <div>
            <p className="eyebrow">{activeWorkspace ? activeWorkspace.role : 'No workspace'}</p>
            <h1>{activeWorkspace ? activeWorkspace.name : 'Create your first workspace'}</h1>
          </div>
          <div className="top-actions">
            {activeWorkspace && dashboard?.connection.status === 'active' && (
              <button type="button" onClick={manualSync} disabled={busy || activeWorkspace.role === 'viewer'}>
                {busy ? <Loader2 className="spin" size={18} /> : <RefreshCw size={18} />}
                Sync
              </button>
            )}
            <button className="icon-button" onClick={signOut} title="Sign out">
              <LogOut size={18} />
            </button>
          </div>
        </header>

        {message && <p className="notice error">{message}</p>}
        {dashboard?.demo_data && (
          <p className="notice">Local demo data. These fixture values are labeled and unavailable in production seed mode.</p>
        )}

        {workspaces.length === 0 ? (
          <section className="empty-band">
            <h2>First workspace</h2>
            <form onSubmit={createWorkspace} className="inline-form">
              <label>
                Name
                <input
                  name="workspace_name"
                  defaultValue={workspaceName}
                  onChange={event => setWorkspaceName(event.target.value)}
                  required
                />
              </label>
              <button type="submit" disabled={busy}>
                <Users size={18} />
                Create
              </button>
            </form>
          </section>
        ) : (
          <ShellView
            view={view}
            state={state}
            workspace={activeWorkspace}
            dashboard={dashboard}
            content={content}
            syncRuns={syncRuns}
            members={members}
            busy={busy}
            onConnect={startConnection}
            onDisconnect={disconnectConnection}
          />
        )}
      </section>
    </main>
  );
}

function StateBanner({ state }: { state: LoadState }) {
  if (state === 'ready') return null;
  const map = {
    ready: { icon: CheckCircle2, title: 'Ready', text: 'Dashboard data is available.' },
    loading: { icon: Loader2, title: 'Sync state loading', text: 'Loading workspace data.' },
    empty: { icon: AlertCircle, title: 'No provider data', text: 'Connect TikTok to start building snapshots.' },
    stale: { icon: CalendarDays, title: 'Stale data', text: 'Last successful sync is outside the selected range.' },
    partial: { icon: ShieldAlert, title: 'Partial sync', text: 'Some provider data was unavailable during the last run.' },
    permission: { icon: Lock, title: 'Permission denied', text: 'This role cannot perform the selected action.' },
    error: { icon: AlertCircle, title: 'Provider error', text: 'The previous operation failed with a sanitized error.' }
  };
  const item = map[state];
  const Icon = item.icon;
  return (
    <section className={`state-banner ${state}`}>
      <Icon className={state === 'loading' ? 'spin' : ''} size={20} />
      <div>
        <strong>{item.title}</strong>
        <span>{item.text}</span>
      </div>
    </section>
  );
}

function ShellView({
  view,
  state,
  workspace,
  dashboard,
  content,
  syncRuns,
  members,
  busy,
  onConnect,
  onDisconnect
}: {
  view: View;
  state: LoadState;
  workspace?: Workspace;
  dashboard: DashboardData | null;
  content: ContentData | null;
  syncRuns: SyncRun[];
  members: Member[];
  busy: boolean;
  onConnect: () => void;
  onDisconnect: () => void;
}) {
  if (!workspace) return null;
  return (
    <div className="content-flow">
      <StateBanner state={state} />
      {view === 'overview' && <Overview dashboard={dashboard} />}
      {view === 'content' && <Content workspace={workspace} content={content} />}
      {view === 'connections' && (
        <Connections
          role={workspace.role}
          dashboard={dashboard}
          busy={busy}
          onConnect={onConnect}
          onDisconnect={onDisconnect}
        />
      )}
      {view === 'members' && <Members role={workspace.role} members={members} />}
      {view === 'sync' && <SyncHistory syncRuns={syncRuns} />}
      {view === 'account' && <Account />}
    </div>
  );
}

function Overview({ dashboard }: { dashboard: DashboardData | null }) {
  const metrics = dashboard?.metrics || [
    { key: 'follower_count', label: 'Followers', value: null, delta: null, percent_change: null },
    { key: 'following_count', label: 'Following', value: null, delta: null, percent_change: null },
    { key: 'likes_count', label: 'Total likes', value: null, delta: null, percent_change: null },
    { key: 'video_count', label: 'Total videos', value: null, delta: null, percent_change: null }
  ];
  return (
    <>
      <section className="metric-grid">
        {metrics.map(metric => (
          <article className="metric-card" key={metric.key}>
            <span>{metric.label}</span>
            <strong>{formatNumber(metric.value)}</strong>
            <small>
              {metric.delta === null
                ? 'No baseline snapshot'
                : `${metric.delta >= 0 ? '+' : ''}${formatNumber(metric.delta)} ${metric.percent_change === null ? '' : `(${metric.percent_change.toFixed(1)}%)`}`}
            </small>
          </article>
        ))}
      </section>
      <section className="panel">
        <div className="panel-title">
          <BarChart3 size={18} />
          <h2>Trends</h2>
        </div>
        {dashboard && dashboard.trend.length > 0 ? (
          <div className="trend-list">
            {dashboard.trend.slice(-8).map(point => (
              <div key={point.observed_at}>
                <span>{formatDate(point.observed_at)}</span>
                <strong>{formatNumber(point.follower_count)}</strong>
                <small>{formatNumber(point.likes_count)} likes</small>
              </div>
            ))}
          </div>
        ) : (
          <div className="chart-empty">No profile snapshots stored for the selected range.</div>
        )}
      </section>
    </>
  );
}

function Content({ workspace, content }: { workspace: Workspace; content: ContentData | null }) {
  const exportHref = `/api/workspaces/${workspace.id}/exports/content.csv`;
  return (
    <section className="panel">
      <div className="panel-title">
        <Table2 size={18} />
        <h2>Content performance</h2>
      </div>
      <div className="toolbar">
        <select aria-label="Metric">
          <option>Views</option>
          <option>Likes</option>
          <option>Comments</option>
          <option>Shares</option>
          <option>Engagement rate</option>
        </select>
        <a className={`button-link ${workspace.role === 'viewer' ? 'disabled' : ''}`} href={workspace.role === 'viewer' ? undefined : exportHref}>
          <Download size={18} /> CSV
        </a>
      </div>
      {content && content.rows.length > 0 ? (
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Published</th>
                <th>Content</th>
                <th>Views</th>
                <th>Likes</th>
                <th>Comments</th>
                <th>Shares</th>
                <th>Engagement</th>
              </tr>
            </thead>
            <tbody>
              {content.rows.map(row => (
                <tr key={row.id}>
                  <td>{formatDate(row.published_at)}</td>
                  <td>
                    {row.share_url ? (
                      <a href={row.share_url} target="_blank" rel="noreferrer">{row.title || row.description || row.provider_content_id}</a>
                    ) : row.title || row.description || row.provider_content_id}
                  </td>
                  <td>{formatNumber(row.view_count)}</td>
                  <td>{formatNumber(row.like_count)}</td>
                  <td>{formatNumber(row.comment_count)}</td>
                  <td>{formatNumber(row.share_count)}</td>
                  <td>{row.engagement_rate === null ? 'N/A' : `${row.engagement_rate.toFixed(2)}%`}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <div className="table-empty">No content snapshots available.</div>
      )}
    </section>
  );
}

function Connections({
  role,
  dashboard,
  busy,
  onConnect,
  onDisconnect
}: {
  role: Workspace['role'];
  dashboard: DashboardData | null;
  busy: boolean;
  onConnect: () => void;
  onDisconnect: () => void;
}) {
  const allowed = role === 'owner' || role === 'admin';
  const status = dashboard?.connection.status || 'disconnected';
  return (
    <section className="panel">
      <div className="panel-title">
        <Link2 size={18} />
        <h2>TikTok connection</h2>
      </div>
      <div className="connection-row">
        <span className={`pill ${status}`}>{status.replace(/_/g, ' ')}</span>
        <button type="button" disabled={!allowed || busy} onClick={onConnect}><ExternalLink size={18} /> Connect</button>
        <button type="button" disabled={!allowed || busy || status === 'disconnected'} onClick={onDisconnect}><Lock size={18} /> Disconnect</button>
      </div>
      {dashboard?.connection.reconnect_reason && <p className="notice error">{dashboard.connection.reconnect_reason}</p>}
    </section>
  );
}

function Members({ role, members }: { role: Workspace['role']; members: Member[] }) {
  const canManage = role === 'owner' || role === 'admin';
  return (
    <section className="panel">
      <div className="panel-title">
        <UserPlus size={18} />
        <h2>Members</h2>
      </div>
      <div className="toolbar">
        <input aria-label="Invite email" placeholder="teammate@example.com" disabled={!canManage} />
        <select aria-label="Role" disabled={!canManage}>
          <option>Viewer</option>
          <option>Analyst</option>
          <option>Admin</option>
        </select>
        <button type="button" disabled={!canManage}><UserPlus size={18} /> Invite</button>
      </div>
      {canManage && members.length > 0 ? (
        <div className="settings-list">
          {members.map(member => (
            <span key={member.user_id}>{member.email} · {member.role}</span>
          ))}
        </div>
      ) : (
        <div className="table-empty">{canManage ? 'No members loaded.' : 'Member management requires owner or admin access.'}</div>
      )}
    </section>
  );
}

function SyncHistory({ syncRuns }: { syncRuns: SyncRun[] }) {
  return (
    <section className="panel">
      <div className="panel-title">
        <RefreshCw size={18} />
        <h2>Sync history</h2>
      </div>
      {syncRuns.length > 0 ? (
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Started</th>
                <th>Trigger</th>
                <th>Status</th>
                <th>Items</th>
                <th>Error</th>
              </tr>
            </thead>
            <tbody>
              {syncRuns.map(run => (
                <tr key={run.id}>
                  <td>{formatDate(run.started_at)}</td>
                  <td>{run.trigger_type}</td>
                  <td>{run.status}</td>
                  <td>{formatNumber(run.content_seen_count)}</td>
                  <td>{run.error_category || 'N/A'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <div className="table-empty">No sync runs recorded.</div>
      )}
    </section>
  );
}

function Account() {
  return (
    <section className="panel">
      <div className="panel-title">
        <Settings size={18} />
        <h2>Session</h2>
      </div>
      <div className="settings-list">
        <span>Opaque server session</span>
        <span>HttpOnly cookie</span>
        <span>SameSite=Lax</span>
      </div>
    </section>
  );
}

createRoot(document.getElementById('root')!).render(<App />);
