import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { createRoot } from 'react-dom/client';
import {
  Activity,
  AlertCircle,
  ArrowLeft,
  BarChart3,
  CalendarDays,
  CheckCircle2,
  ChevronDown,
  Download,
  ExternalLink,
  FileText,
  Link2,
  Loader2,
  Lock,
  LogOut,
  RefreshCw,
  Search,
  Settings,
  ShieldAlert,
  Trash2,
  UserPlus,
  Users,
  Video
} from 'lucide-react';
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis
} from 'recharts';
import './styles.css';

type View = 'overview' | 'content' | 'connections' | 'members' | 'sync' | 'account';
type LoadState = 'ready' | 'loading' | 'empty' | 'stale' | 'partial' | 'permission' | 'error' | 'reconnect';
type RangeKey = '7d' | '30d' | '90d' | 'custom';
type Role = 'owner' | 'admin' | 'analyst' | 'viewer';
type SortDirection = 'asc' | 'desc';
type ContentSort = 'published_at' | 'views' | 'likes' | 'comments' | 'shares' | 'engagement';

type User = {
  id: string;
  email: string;
  display_name?: string | null;
};

type Workspace = {
  id: string;
  name: string;
  slug: string;
  role: Role;
};

type DashboardMetric = {
  key: string;
  label: string;
  value: number | null;
  baseline: number | null;
  delta: number | null;
  percent_change: number | null;
};

type DashboardData = {
  range: { from: string; to: string };
  demo_data: boolean;
  connection: {
    provider: string;
    status: string;
    reconnect_reason?: string | null;
    last_sync_at?: string | null;
    last_successful_sync_at?: string | null;
    next_sync_at?: string | null;
  };
  latest_sync: SyncRun | null;
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
  duration_seconds?: number | null;
  height?: number | null;
  width?: number | null;
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
  limit: number;
  offset: number;
};

type ContentDetail = {
  item: ContentRow & {
    provider_metadata?: Record<string, unknown> | null;
  };
  current_metrics: {
    observed_at: string | null;
    view_count: number | null;
    like_count: number | null;
    comment_count: number | null;
    share_count: number | null;
    engagement_rate: number | null;
  } | null;
  history: Array<{
    observed_at: string;
    view_count: number | null;
    like_count: number | null;
    comment_count: number | null;
    share_count: number | null;
    engagement_rate: number | null;
  }>;
};

type SyncRun = {
  id: string;
  trigger_type: string;
  status: string;
  started_at: string;
  finished_at: string | null;
  duration_ms: number | null;
  attempt?: number;
  profile_count?: number;
  content_seen_count: number;
  content_snapshot_count?: number;
  error_category?: string | null;
  provider_code?: string | null;
  retryable?: boolean | null;
};

type SyncData = {
  sync_runs: SyncRun[];
  total: number;
  limit: number;
  offset: number;
};

type Member = {
  user_id: string;
  email: string;
  display_name?: string | null;
  role: Role;
  status?: string;
  joined_at?: string;
};

type Invitation = {
  id: string;
  email: string;
  role: Exclude<Role, 'owner'>;
  created_at: string;
  expires_at: string;
  accepted_at: string | null;
  revoked_at: string | null;
  invited_by_email: string;
};

const views: Array<{ id: View; label: string; icon: React.ComponentType<{ size?: number; 'aria-hidden'?: boolean }> }> =
  [
    { id: 'overview', label: 'Overview', icon: BarChart3 },
    { id: 'content', label: 'Content', icon: Video },
    { id: 'connections', label: 'Connections', icon: Link2 },
    { id: 'members', label: 'Members', icon: Users },
    { id: 'sync', label: 'Sync history', icon: Activity },
    { id: 'account', label: 'Account', icon: Settings }
  ];

const metricLabels: Record<string, string> = {
  follower_count: 'Followers',
  following_count: 'Following',
  likes_count: 'Total likes',
  video_count: 'Total videos'
};

const contentSortLabels: Record<ContentSort, string> = {
  published_at: 'Published',
  views: 'Views',
  likes: 'Likes',
  comments: 'Comments',
  shares: 'Shares',
  engagement: 'Engagement'
};

async function api<T>(path: string, options: RequestInit = {}): Promise<T> {
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
  return (
    document.cookie
      .split(';')
      .map((value) => value.trim())
      .find((value) => value.startsWith(`${name}=`))
      ?.slice(name.length + 1) || ''
  );
}

function formatNumber(value: number | null | undefined) {
  if (value === null || value === undefined) return 'N/A';
  return new Intl.NumberFormat().format(value);
}

function formatCompact(value: number | null | undefined) {
  if (value === null || value === undefined) return 'N/A';
  return new Intl.NumberFormat(undefined, { notation: 'compact', maximumFractionDigits: 1 }).format(value);
}

function formatDate(
  value?: string | null,
  options: Intl.DateTimeFormatOptions = { dateStyle: 'medium', timeStyle: 'short' }
) {
  if (!value) return 'N/A';
  return new Intl.DateTimeFormat(undefined, options).format(new Date(value));
}

function formatDuration(value?: number | null) {
  if (!value) return 'N/A';
  if (value < 1000) return `${value} ms`;
  return `${(value / 1000).toFixed(1)} s`;
}

function formatPercent(value?: number | null) {
  if (value === null || value === undefined) return 'N/A';
  return `${value.toFixed(2)}%`;
}

function formatTooltipNumber(value: unknown) {
  return typeof value === 'number' ? formatNumber(value) : 'N/A';
}

function formatTooltipPercent(value: unknown) {
  return typeof value === 'number' ? formatPercent(value) : 'N/A';
}

function todayInputValue(offsetDays = 0) {
  const date = new Date();
  date.setDate(date.getDate() + offsetDays);
  return date.toISOString().slice(0, 10);
}

function pathDetailState() {
  const match = window.location.pathname.match(/^\/app\/workspaces\/([^/]+)\/content\/([^/]+)/);
  return match ? { workspaceId: match[1], contentId: match[2] } : null;
}

function initialUrlState() {
  const params = new URLSearchParams(window.location.search);
  const detail = pathDetailState();
  return {
    view: (params.get('view') as View) || (detail ? 'content' : 'overview'),
    workspaceId: detail?.workspaceId || params.get('workspace') || '',
    contentId: detail?.contentId || '',
    range: (params.get('range') as RangeKey) || '30d',
    from: params.get('from') || todayInputValue(-30),
    to: params.get('to') || todayInputValue(0),
    metric: params.get('metric') || 'both',
    compare: params.get('compare') !== 'false',
    topSort: (params.get('topSort') as ContentSort) || 'views',
    contentSort: (params.get('sort') as ContentSort) || 'views',
    contentDir: (params.get('direction') as SortDirection) || 'desc',
    search: params.get('search') || '',
    page: Math.max(Number(params.get('page') || 1), 1),
    pageSize: Math.min(Math.max(Number(params.get('pageSize') || 10), 5), 50)
  };
}

function queryFromRange(range: RangeKey, from: string, to: string) {
  const params = new URLSearchParams();
  if (range === 'custom') {
    params.set('range', 'custom');
    params.set('from', from);
    params.set('to', to);
  } else {
    params.set('range', range);
  }
  return params;
}

function resolveLoadState(dashboard: DashboardData | null): LoadState {
  if (!dashboard) return 'empty';
  if (dashboard.latest_sync?.status === 'failed') return 'error';
  if (dashboard.latest_sync?.status === 'partial') return 'partial';
  if (dashboard.connection.status === 'reconnect_required') return 'reconnect';
  const lastSuccessful = dashboard.connection.last_successful_sync_at
    ? new Date(dashboard.connection.last_successful_sync_at).getTime()
    : 0;
  if (dashboard.connection.status === 'active' && lastSuccessful && Date.now() - lastSuccessful > 30 * 60 * 60 * 1000) {
    return 'stale';
  }
  if (dashboard.demo_data && dashboard.trend.length > 0) return 'ready';
  if (dashboard.connection.status === 'disconnected') return 'empty';
  return dashboard.trend.length > 0 ? 'ready' : 'empty';
}

function roleCanManage(role?: Role) {
  return role === 'owner' || role === 'admin';
}

function roleCanSync(role?: Role) {
  return role === 'owner' || role === 'admin' || role === 'analyst';
}

function App() {
  const initial = useMemo(initialUrlState, []);
  const [user, setUser] = useState<User | null>(null);
  const [csrf, setCsrf] = useState('');
  const [email, setEmail] = useState('');
  const [token, setToken] = useState('');
  const [workspaceName, setWorkspaceName] = useState('');
  const [workspaces, setWorkspaces] = useState<Workspace[]>([]);
  const [activeWorkspaceId, setActiveWorkspaceId] = useState(initial.workspaceId);
  const [view, setView] = useState<View>(views.some((item) => item.id === initial.view) ? initial.view : 'overview');
  const [contentDetailId, setContentDetailId] = useState(initial.contentId);
  const [range, setRange] = useState<RangeKey>(
    ['7d', '30d', '90d', 'custom'].includes(initial.range) ? initial.range : '30d'
  );
  const [customFrom, setCustomFrom] = useState(initial.from);
  const [customTo, setCustomTo] = useState(initial.to);
  const [compare, setCompare] = useState(initial.compare);
  const [trendMetric, setTrendMetric] = useState(initial.metric);
  const [topSort, setTopSort] = useState<ContentSort>(initial.topSort);
  const [contentSort, setContentSort] = useState<ContentSort>(initial.contentSort);
  const [contentDir, setContentDir] = useState<SortDirection>(initial.contentDir);
  const [contentSearch, setContentSearch] = useState(initial.search);
  const [contentPage, setContentPage] = useState(initial.page);
  const [contentPageSize, setContentPageSize] = useState(initial.pageSize);
  const [syncPage, setSyncPage] = useState(1);
  const [dashboard, setDashboard] = useState<DashboardData | null>(null);
  const [content, setContent] = useState<ContentData | null>(null);
  const [contentDetail, setContentDetail] = useState<ContentDetail | null>(null);
  const [syncData, setSyncData] = useState<SyncData>({ sync_runs: [], total: 0, limit: 25, offset: 0 });
  const [members, setMembers] = useState<Member[]>([]);
  const [invitations, setInvitations] = useState<Invitation[]>([]);
  const [state, setState] = useState<LoadState>('loading');
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState('');
  const [toast, setToast] = useState('');
  const [accountOpen, setAccountOpen] = useState(false);
  const [disconnectOpen, setDisconnectOpen] = useState(false);
  const [expandedRun, setExpandedRun] = useState<string>('');

  const activeWorkspace = useMemo(
    () => workspaces.find((workspace) => workspace.id === activeWorkspaceId) || workspaces[0],
    [activeWorkspaceId, workspaces]
  );

  const rangeQuery = useMemo(() => queryFromRange(range, customFrom, customTo), [range, customFrom, customTo]);

  const rangeInvalid = useMemo(() => {
    if (range !== 'custom') return '';
    const from = new Date(customFrom);
    const to = new Date(customTo);
    if (!customFrom || !customTo || Number.isNaN(from.getTime()) || Number.isNaN(to.getTime()) || from > to) {
      return 'Choose a valid start and end date.';
    }
    if (to.getTime() - from.getTime() > 366 * 24 * 60 * 60 * 1000) {
      return 'Custom ranges are limited to 366 days.';
    }
    return '';
  }, [range, customFrom, customTo]);

  useEffect(() => {
    let cancelled = false;
    async function resumeSession() {
      try {
        const session = await api<{ user: User }>('/api/session');
        if (cancelled) return;
        setUser(session.user);
        setCsrf(readCookie('sis_csrf'));
        const workspaceResult = await api<{ workspaces: Workspace[] }>('/api/workspaces');
        if (cancelled) return;
        setWorkspaces(workspaceResult.workspaces);
        setActiveWorkspaceId((current) => current || workspaceResult.workspaces[0]?.id || '');
      } catch {
        if (!cancelled) {
          setUser(null);
          setCsrf('');
          setState('empty');
        }
      }
    }
    void resumeSession();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    function onPopState() {
      const next = initialUrlState();
      setView(views.some((item) => item.id === next.view) ? next.view : 'overview');
      setActiveWorkspaceId(next.workspaceId || activeWorkspaceId);
      setContentDetailId(next.contentId);
      setRange(next.range);
      setCustomFrom(next.from);
      setCustomTo(next.to);
      setTrendMetric(next.metric);
      setCompare(next.compare);
      setTopSort(next.topSort);
      setContentSort(next.contentSort);
      setContentDir(next.contentDir);
      setContentSearch(next.search);
      setContentPage(next.page);
      setContentPageSize(next.pageSize);
    }
    window.addEventListener('popstate', onPopState);
    return () => window.removeEventListener('popstate', onPopState);
  }, [activeWorkspaceId]);

  useEffect(() => {
    if (!user || !activeWorkspace) return;
    const params = new URLSearchParams();
    params.set('workspace', activeWorkspace.id);
    params.set('view', view);
    params.set('range', range);
    params.set('metric', trendMetric);
    params.set('compare', String(compare));
    params.set('topSort', topSort);
    params.set('sort', contentSort);
    params.set('direction', contentDir);
    params.set('page', String(contentPage));
    params.set('pageSize', String(contentPageSize));
    if (range === 'custom') {
      params.set('from', customFrom);
      params.set('to', customTo);
    }
    if (contentSearch) params.set('search', contentSearch);
    const path = contentDetailId ? `/app/workspaces/${activeWorkspace.id}/content/${contentDetailId}` : '/app/';
    window.history.replaceState({}, '', `${path}?${params.toString()}`);
  }, [
    user,
    activeWorkspace,
    view,
    range,
    customFrom,
    customTo,
    trendMetric,
    compare,
    topSort,
    contentSort,
    contentDir,
    contentSearch,
    contentPage,
    contentPageSize,
    contentDetailId
  ]);

  async function loadWorkspaces() {
    const workspaceResult = await api<{ workspaces: Workspace[] }>('/api/workspaces');
    setWorkspaces(workspaceResult.workspaces);
    setActiveWorkspaceId(workspaceResult.workspaces[0]?.id || '');
  }

  const loadWorkspaceData = useCallback(
    async (workspace: Workspace) => {
      if (rangeInvalid) {
        setMessage(rangeInvalid);
        setState('error');
        return;
      }
      setState('loading');
      setMessage('');
      const dashboardParams = new URLSearchParams(rangeQuery);
      dashboardParams.set('top_sort', topSort);
      const contentParams = new URLSearchParams(rangeQuery);
      contentParams.set('sort', contentSort);
      contentParams.set('direction', contentDir);
      contentParams.set('limit', String(contentPageSize));
      contentParams.set('offset', String((contentPage - 1) * contentPageSize));
      if (contentSearch.trim()) contentParams.set('search', contentSearch.trim());
      const syncParams = new URLSearchParams();
      syncParams.set('limit', '25');
      syncParams.set('offset', String((syncPage - 1) * 25));
      try {
        const [dashboardResult, contentResult, syncResult] = await Promise.all([
          api<DashboardData>(`/api/workspaces/${workspace.id}/dashboard?${dashboardParams.toString()}`),
          api<ContentData>(`/api/workspaces/${workspace.id}/content?${contentParams.toString()}`),
          api<SyncData>(`/api/workspaces/${workspace.id}/sync-runs?${syncParams.toString()}`)
        ]);
        setDashboard(dashboardResult);
        setContent(contentResult);
        setSyncData(syncResult);
        if (roleCanManage(workspace.role)) {
          const memberResult = await api<{ members: Member[]; invitations: Invitation[] }>(
            `/api/workspaces/${workspace.id}/members`
          );
          setMembers(memberResult.members);
          setInvitations(memberResult.invitations || []);
        } else {
          setMembers([]);
          setInvitations([]);
        }
        setState(resolveLoadState(dashboardResult));
      } catch (error) {
        const text = error instanceof Error ? error.message : 'load_failed';
        setMessage(text);
        setState(text === 'permission_denied' ? 'permission' : 'error');
      }
    },
    [contentDir, contentPage, contentPageSize, contentSearch, contentSort, rangeInvalid, rangeQuery, syncPage, topSort]
  );

  useEffect(() => {
    if (!user || !activeWorkspace) return;
    void loadWorkspaceData(activeWorkspace);
  }, [user, activeWorkspace, loadWorkspaceData]);

  const detailWorkspaceId = activeWorkspace?.id || '';

  useEffect(() => {
    if (!user || !detailWorkspaceId || !contentDetailId) {
      setContentDetail(null);
      return;
    }
    let cancelled = false;
    async function loadDetail() {
      try {
        const detail = await api<ContentDetail>(`/api/workspaces/${detailWorkspaceId}/content/${contentDetailId}`);
        if (!cancelled) setContentDetail(detail);
      } catch (error) {
        if (!cancelled) {
          setContentDetail(null);
          setMessage(error instanceof Error ? error.message : 'content_detail_failed');
        }
      }
    }
    void loadDetail();
    return () => {
      cancelled = true;
    };
  }, [user, detailWorkspaceId, contentDetailId]);

  async function requestLink(event: React.FormEvent) {
    event.preventDefault();
    setBusy(true);
    setMessage('');
    try {
      const result = await api<{ sent: boolean; dev_token?: string }>('/api/auth/magic-link/request', {
        method: 'POST',
        body: JSON.stringify({ email })
      });
      if (result.dev_token) {
        await verifyToken(result.dev_token);
        setToast('Signed in with local development authentication.');
      } else {
        setMessage('Magic link requested. Paste the token from your email to continue.');
      }
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'request_failed');
    } finally {
      setBusy(false);
    }
  }

  async function verifyToken(value: string) {
    const result = await api<{ user: User; csrf_token: string }>('/api/auth/magic-link/verify', {
      method: 'POST',
      body: JSON.stringify({ token: value })
    });
    setUser(result.user);
    setCsrf(result.csrf_token);
    await loadWorkspaces();
    setMessage('');
    setToken('');
  }

  async function verifyLink(event: React.FormEvent) {
    event.preventDefault();
    setBusy(true);
    setMessage('');
    try {
      await verifyToken(token);
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
      const result = await api<{ workspace: Workspace }>('/api/workspaces', {
        method: 'POST',
        headers: { 'x-csrf-token': csrf },
        body: JSON.stringify({ name: submittedName })
      });
      setWorkspaces((current) => [...current, result.workspace]);
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
      // Client state is still cleared if the session was already invalidated.
    } finally {
      setUser(null);
      setCsrf('');
      setWorkspaces([]);
      setActiveWorkspaceId('');
      setAccountOpen(false);
      setBusy(false);
    }
  }

  async function manualSync() {
    if (!activeWorkspace) return;
    setBusy(true);
    setMessage('');
    try {
      const result = await api<{ status?: string; error?: { message?: string; category?: string } }>(
        `/api/workspaces/${activeWorkspace.id}/sync-runs`,
        {
          method: 'POST',
          headers: { 'x-csrf-token': csrf },
          body: JSON.stringify({})
        }
      );
      if (result.status === 'failed') {
        const failureMessage = result.error?.message || result.error?.category || 'sync_failed';
        await loadWorkspaceData(activeWorkspace);
        setMessage(failureMessage);
        setState('error');
      } else if (result.status === 'partial') {
        await loadWorkspaceData(activeWorkspace);
        setToast('Manual sync completed with partial data.');
        setState('partial');
      } else {
        await loadWorkspaceData(activeWorkspace);
        setToast('Manual sync queued.');
      }
    } catch (error) {
      const code = error instanceof Error ? error.message : 'sync_failed';
      setMessage(
        code === 'manual_sync_cooldown' ? 'Manual sync is cooling down. Try again after the 15-minute window.' : code
      );
    } finally {
      setBusy(false);
    }
  }

  async function startConnection() {
    if (!activeWorkspace) return;
    setBusy(true);
    setMessage('');
    try {
      const result = await api<{ authorization_url: string }>(
        `/api/workspaces/${activeWorkspace.id}/connections/tiktok/start`,
        {
          method: 'POST',
          headers: { 'x-csrf-token': csrf },
          body: JSON.stringify({ return_path: `/app/?workspace=${activeWorkspace.id}&view=connections` })
        }
      );
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
      setToast('TikTok connection disabled locally. Provider revocation was attempted first.');
      setDisconnectOpen(false);
      await loadWorkspaceData(activeWorkspace);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'disconnect_failed');
    } finally {
      setBusy(false);
    }
  }

  async function inviteMember(emailValue: string, roleValue: Exclude<Role, 'owner'>) {
    if (!activeWorkspace) return;
    setBusy(true);
    setMessage('');
    try {
      await api(`/api/workspaces/${activeWorkspace.id}/invitations`, {
        method: 'POST',
        headers: { 'x-csrf-token': csrf },
        body: JSON.stringify({ email: emailValue, role: roleValue })
      });
      setToast('Invitation recorded.');
      await loadWorkspaceData(activeWorkspace);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'invite_failed');
    } finally {
      setBusy(false);
    }
  }

  async function updateMemberRole(member: Member, roleValue: Role) {
    if (!activeWorkspace) return;
    setBusy(true);
    setMessage('');
    try {
      await api(`/api/workspaces/${activeWorkspace.id}/members/${member.user_id}`, {
        method: 'PATCH',
        headers: { 'x-csrf-token': csrf },
        body: JSON.stringify({ role: roleValue })
      });
      setToast(`${member.email} updated.`);
      await loadWorkspaceData(activeWorkspace);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'role_update_failed');
    } finally {
      setBusy(false);
    }
  }

  async function removeMember(member: Member) {
    if (!activeWorkspace) return;
    setBusy(true);
    setMessage('');
    try {
      await api(`/api/workspaces/${activeWorkspace.id}/members/${member.user_id}`, {
        method: 'DELETE',
        headers: { 'x-csrf-token': csrf },
        body: JSON.stringify({})
      });
      setToast(`${member.email} removed.`);
      await loadWorkspaceData(activeWorkspace);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'remove_failed');
    } finally {
      setBusy(false);
    }
  }

  function openContentDetail(contentId: string) {
    setView('content');
    setContentDetailId(contentId);
    window.scrollTo({ top: 0, behavior: 'smooth' });
  }

  function closeContentDetail() {
    setContentDetailId('');
    setView('content');
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
            <span>
              <CheckCircle2 size={16} aria-hidden /> Secure sessions
            </span>
            <span>
              <Lock size={16} aria-hidden /> Server-side tokens
            </span>
            <span>
              <Activity size={16} aria-hidden /> Background sync
            </span>
          </div>
        </section>

        <section className="auth-panel" aria-labelledby="signin-title">
          <h2 id="signin-title">Sign in</h2>
          <form onSubmit={requestLink} className="stack">
            <label>
              Email
              <input
                value={email}
                onChange={(event) => setEmail(event.target.value)}
                type="email"
                autoComplete="email"
                required
              />
            </label>
            <button type="submit" disabled={busy}>
              {busy ? <Loader2 className="spin" size={18} aria-hidden /> : <FileText size={18} aria-hidden />}
              Request link
            </button>
          </form>
          <form onSubmit={verifyLink} className="stack">
            <label>
              Magic link token
              <input value={token} onChange={(event) => setToken(event.target.value)} autoComplete="one-time-code" />
            </label>
            <button type="submit" disabled={busy || !token}>
              <CheckCircle2 size={18} aria-hidden />
              Verify token
            </button>
          </form>
          {message && (
            <p className="notice" role="status">
              {message}
            </p>
          )}
        </section>
      </main>
    );
  }

  return (
    <main className="app-shell">
      <aside className="sidebar" aria-label="Primary navigation">
        <div className="brand-row">
          <img src="/logo.png" alt="" />
          <strong>Social Insights Studio</strong>
        </div>
        <WorkspaceSelect
          workspaces={workspaces}
          activeWorkspaceId={activeWorkspace?.id || ''}
          onChange={setActiveWorkspaceId}
        />
        <Nav
          view={view}
          onChange={(next) => {
            setView(next);
            setContentDetailId('');
          }}
        />
      </aside>

      <section className="workspace">
        <WorkspaceHeader
          workspace={activeWorkspace}
          dashboard={dashboard}
          busy={busy}
          accountOpen={accountOpen}
          onAccountToggle={() => setAccountOpen((open) => !open)}
          onManualSync={manualSync}
          onSignOut={signOut}
        />

        <div aria-live="polite" className="live-region">
          {toast || message}
        </div>
        {toast && <p className="notice success">{toast}</p>}
        {message && <p className="notice error">{message}</p>}
        {dashboard?.demo_data && (
          <p className="notice">
            Local demo data. Fixture values are labeled, deterministic, local-only, and unavailable in production seed
            mode.
          </p>
        )}

        {workspaces.length === 0 ? (
          <section className="empty-band" aria-labelledby="first-workspace-title">
            <h2 id="first-workspace-title">First workspace</h2>
            <form onSubmit={createWorkspace} className="inline-form">
              <label>
                Name
                <input
                  name="workspace_name"
                  defaultValue={workspaceName}
                  onChange={(event) => setWorkspaceName(event.target.value)}
                  required
                />
              </label>
              <button type="submit" disabled={busy}>
                <Users size={18} aria-hidden />
                Create
              </button>
            </form>
          </section>
        ) : activeWorkspace ? (
          <div className="content-flow">
            <StateBanner state={state} />
            {view === 'overview' && (
              <Overview
                dashboard={dashboard}
                range={range}
                customFrom={customFrom}
                customTo={customTo}
                compare={compare}
                trendMetric={trendMetric}
                topSort={topSort}
                rangeInvalid={rangeInvalid}
                onRangeChange={(next) => setRange(next)}
                onCustomFromChange={setCustomFrom}
                onCustomToChange={setCustomTo}
                onCompareChange={setCompare}
                onTrendMetricChange={setTrendMetric}
                onTopSortChange={setTopSort}
              />
            )}
            {view === 'content' && contentDetailId ? (
              <ContentDetailView detail={contentDetail} onBack={closeContentDetail} />
            ) : view === 'content' ? (
              <Content
                workspace={activeWorkspace}
                content={content}
                sort={contentSort}
                direction={contentDir}
                search={contentSearch}
                page={contentPage}
                pageSize={contentPageSize}
                rangeQuery={rangeQuery}
                onSort={(sort, direction) => {
                  setContentSort(sort);
                  setContentDir(direction);
                  setContentPage(1);
                }}
                onSearch={(value) => {
                  setContentSearch(value);
                  setContentPage(1);
                }}
                onPageChange={setContentPage}
                onPageSizeChange={(size) => {
                  setContentPageSize(size);
                  setContentPage(1);
                }}
                onOpenDetail={openContentDetail}
              />
            ) : null}
            {view === 'connections' && (
              <Connections
                role={activeWorkspace.role}
                dashboard={dashboard}
                busy={busy}
                disconnectOpen={disconnectOpen}
                onConnect={startConnection}
                onDisconnectRequest={() => setDisconnectOpen(true)}
                onDisconnectCancel={() => setDisconnectOpen(false)}
                onDisconnectConfirm={disconnectConnection}
              />
            )}
            {view === 'members' && (
              <Members
                role={activeWorkspace.role}
                members={members}
                invitations={invitations}
                busy={busy}
                onInvite={inviteMember}
                onRoleChange={updateMemberRole}
                onRemove={removeMember}
              />
            )}
            {view === 'sync' && (
              <SyncHistory
                syncData={syncData}
                page={syncPage}
                expandedRun={expandedRun}
                onPageChange={setSyncPage}
                onToggleRun={(runId) => setExpandedRun((current) => (current === runId ? '' : runId))}
              />
            )}
            {view === 'account' && <Account user={user} onSignOut={signOut} />}
          </div>
        ) : null}
      </section>

      <nav className="bottom-nav" aria-label="Primary navigation">
        {views.map((item) => {
          const Icon = item.icon;
          return (
            <button
              key={item.id}
              type="button"
              aria-current={view === item.id ? 'page' : undefined}
              className={view === item.id ? 'active' : ''}
              onClick={() => {
                setView(item.id);
                setContentDetailId('');
              }}
            >
              <Icon size={18} aria-hidden />
              <span>{item.label}</span>
            </button>
          );
        })}
      </nav>
    </main>
  );
}

function WorkspaceSelect({
  workspaces,
  activeWorkspaceId,
  onChange
}: {
  workspaces: Workspace[];
  activeWorkspaceId: string;
  onChange: (workspaceId: string) => void;
}) {
  return (
    <label className="workspace-select">
      Workspace
      <select value={activeWorkspaceId} onChange={(event) => onChange(event.target.value)}>
        {workspaces.map((workspace) => (
          <option key={workspace.id} value={workspace.id}>
            {workspace.name}
          </option>
        ))}
      </select>
    </label>
  );
}

function Nav({ view, onChange }: { view: View; onChange: (view: View) => void }) {
  return (
    <nav className="side-nav">
      {views.map((item) => {
        const Icon = item.icon;
        return (
          <button
            type="button"
            key={item.id}
            className={view === item.id ? 'active' : ''}
            onClick={() => onChange(item.id)}
            aria-current={view === item.id ? 'page' : undefined}
          >
            <Icon size={18} aria-hidden />
            <span>{item.label}</span>
          </button>
        );
      })}
    </nav>
  );
}

function WorkspaceHeader({
  workspace,
  dashboard,
  busy,
  accountOpen,
  onAccountToggle,
  onManualSync,
  onSignOut
}: {
  workspace?: Workspace;
  dashboard: DashboardData | null;
  busy: boolean;
  accountOpen: boolean;
  onAccountToggle: () => void;
  onManualSync: () => void;
  onSignOut: () => void;
}) {
  const connection = dashboard?.connection;
  const canSync = workspace && roleCanSync(workspace.role) && connection?.status === 'active';
  return (
    <header className="topbar">
      <div>
        <p className="eyebrow">{workspace ? workspace.role : 'No workspace'}</p>
        <h1>{workspace ? workspace.name : 'Create your first workspace'}</h1>
      </div>
      <div className="header-status" aria-label="Workspace status">
        <StatusBadge status={connection?.status || 'disconnected'} />
        <span>Last sync: {formatDate(connection?.last_successful_sync_at)}</span>
        <span>Next: {formatDate(connection?.next_sync_at)}</span>
      </div>
      <div className="top-actions">
        {canSync && (
          <button type="button" onClick={onManualSync} disabled={busy}>
            {busy ? <Loader2 className="spin" size={18} aria-hidden /> : <RefreshCw size={18} aria-hidden />}
            Sync
          </button>
        )}
        <div className="menu-wrap">
          <button
            type="button"
            className="menu-button"
            onClick={onAccountToggle}
            aria-expanded={accountOpen}
            aria-haspopup="menu"
          >
            <Settings size={18} aria-hidden />
            Account
            <ChevronDown size={16} aria-hidden />
          </button>
          {accountOpen && (
            <div className="menu" role="menu">
              <span role="menuitem">Opaque server session</span>
              <button type="button" role="menuitem" onClick={onSignOut}>
                <LogOut size={16} aria-hidden />
                Sign out
              </button>
            </div>
          )}
        </div>
      </div>
    </header>
  );
}

function StateBanner({ state }: { state: LoadState }) {
  if (state === 'ready') return null;
  const map = {
    ready: { icon: CheckCircle2, title: 'Ready', text: 'Dashboard data is available.' },
    loading: { icon: Loader2, title: 'Loading', text: 'Loading workspace data.' },
    empty: {
      icon: AlertCircle,
      title: 'No provider data',
      text: 'Connect TikTok or use labeled local demo data to populate this workspace.'
    },
    stale: { icon: CalendarDays, title: 'Stale data', text: 'Last successful sync is outside the freshness window.' },
    partial: {
      icon: ShieldAlert,
      title: 'Partial sync',
      text: 'Some provider data was unavailable during the latest run.'
    },
    permission: { icon: Lock, title: 'Permission denied', text: 'This role cannot perform the selected action.' },
    error: { icon: AlertCircle, title: 'Sync error', text: 'The previous operation failed with a sanitized error.' },
    reconnect: {
      icon: ShieldAlert,
      title: 'Reconnect required',
      text: 'The provider connection needs authorization before sync can resume.'
    }
  };
  const item = map[state];
  const Icon = item.icon;
  return (
    <section className={`state-banner ${state}`} role={state === 'error' ? 'alert' : 'status'}>
      <Icon className={state === 'loading' ? 'spin' : ''} size={20} aria-hidden />
      <div>
        <strong>{item.title}</strong>
        <span>{item.text}</span>
      </div>
    </section>
  );
}

function Overview({
  dashboard,
  range,
  customFrom,
  customTo,
  compare,
  trendMetric,
  topSort,
  rangeInvalid,
  onRangeChange,
  onCustomFromChange,
  onCustomToChange,
  onCompareChange,
  onTrendMetricChange,
  onTopSortChange
}: {
  dashboard: DashboardData | null;
  range: RangeKey;
  customFrom: string;
  customTo: string;
  compare: boolean;
  trendMetric: string;
  topSort: ContentSort;
  rangeInvalid: string;
  onRangeChange: (range: RangeKey) => void;
  onCustomFromChange: (value: string) => void;
  onCustomToChange: (value: string) => void;
  onCompareChange: (value: boolean) => void;
  onTrendMetricChange: (value: string) => void;
  onTopSortChange: (value: ContentSort) => void;
}) {
  const metrics =
    dashboard?.metrics ||
    Object.entries(metricLabels).map(([key, label]) => ({
      key,
      label,
      value: null,
      baseline: null,
      delta: null,
      percent_change: null
    }));
  const trendData = (dashboard?.trend || []).map((point) => ({
    ...point,
    label: formatDate(point.observed_at, { month: 'short', day: 'numeric' })
  }));
  return (
    <>
      <section className="control-bar" aria-labelledby="date-controls-title">
        <h2 id="date-controls-title" className="sr-only">
          Date and comparison controls
        </h2>
        <div className="segmented" aria-label="Date range">
          {(['7d', '30d', '90d'] as RangeKey[]).map((option) => (
            <button
              key={option}
              type="button"
              className={range === option ? 'active' : ''}
              onClick={() => onRangeChange(option)}
            >
              {option.replace('d', ' days')}
            </button>
          ))}
          <button type="button" className={range === 'custom' ? 'active' : ''} onClick={() => onRangeChange('custom')}>
            Custom
          </button>
        </div>
        {range === 'custom' && (
          <div className="date-pair">
            <label>
              From
              <input
                type="date"
                value={customFrom}
                onChange={(event) => onCustomFromChange(event.target.value)}
                aria-invalid={Boolean(rangeInvalid)}
              />
            </label>
            <label>
              To
              <input
                type="date"
                value={customTo}
                onChange={(event) => onCustomToChange(event.target.value)}
                aria-invalid={Boolean(rangeInvalid)}
              />
            </label>
          </div>
        )}
        <label className="toggle">
          <input type="checkbox" checked={compare} onChange={(event) => onCompareChange(event.target.checked)} />
          Previous-period comparison
        </label>
        {rangeInvalid && <p className="form-error">{rangeInvalid}</p>}
      </section>

      <section className="metric-grid" aria-label="Summary metrics">
        {metrics.map((metric) => (
          <MetricCard key={metric.key} metric={metric} compare={compare} />
        ))}
      </section>

      <section className="panel chart-panel" aria-labelledby="trend-title">
        <div className="panel-title between">
          <div>
            <h2 id="trend-title">Trend</h2>
            <p>Followers and total likes over the selected range.</p>
          </div>
          <select
            aria-label="Trend metric"
            value={trendMetric}
            onChange={(event) => onTrendMetricChange(event.target.value)}
          >
            <option value="both">Followers and likes</option>
            <option value="followers">Followers</option>
            <option value="likes">Total likes</option>
          </select>
        </div>
        {trendData.length > 1 ? (
          <>
            <div className="chart-box" role="img" aria-label="Line chart of followers and total likes over time">
              <ResponsiveContainer width="100%" height={300}>
                <LineChart data={trendData} margin={{ top: 12, right: 24, bottom: 12, left: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="label" minTickGap={24} />
                  <YAxis yAxisId="followers" tickFormatter={formatCompact} />
                  <YAxis yAxisId="likes" orientation="right" tickFormatter={formatCompact} />
                  <Tooltip formatter={formatTooltipNumber} labelFormatter={(label) => `Observed ${label}`} />
                  <Legend />
                  {(trendMetric === 'both' || trendMetric === 'followers') && (
                    <Line
                      yAxisId="followers"
                      type="monotone"
                      dataKey="follower_count"
                      name="Followers"
                      stroke="var(--chart-a)"
                      strokeWidth={2.5}
                      dot={false}
                      connectNulls
                    />
                  )}
                  {(trendMetric === 'both' || trendMetric === 'likes') && (
                    <Line
                      yAxisId="likes"
                      type="monotone"
                      dataKey="likes_count"
                      name="Total likes"
                      stroke="var(--chart-b)"
                      strokeWidth={2.5}
                      dot={false}
                      connectNulls
                    />
                  )}
                </LineChart>
              </ResponsiveContainer>
            </div>
            <ChartSummary trend={dashboard?.trend || []} />
          </>
        ) : (
          <div className="chart-empty">
            {trendData.length === 1
              ? 'One snapshot is available. More points are needed for a trend line.'
              : 'No profile snapshots stored for the selected range.'}
          </div>
        )}
      </section>

      <section className="panel chart-panel" aria-labelledby="top-content-title">
        <div className="panel-title between">
          <div>
            <h2 id="top-content-title">Top content</h2>
            <p>Ranked by selected metric. Missing metrics remain visibly unavailable.</p>
          </div>
          <select
            aria-label="Top content metric"
            value={topSort}
            onChange={(event) => onTopSortChange(event.target.value as ContentSort)}
          >
            <option value="views">Views</option>
            <option value="likes">Likes</option>
            <option value="comments">Comments</option>
            <option value="shares">Shares</option>
            <option value="engagement">Engagement rate</option>
          </select>
        </div>
        {dashboard?.top_content.length ? (
          <div
            className="chart-box"
            role="img"
            aria-label={`Bar chart of top content by ${contentSortLabels[topSort]}`}
          >
            <ResponsiveContainer width="100%" height={300}>
              <BarChart
                data={dashboard.top_content.map((row) => ({
                  name: contentLabel(row),
                  value: topSort === 'engagement' ? row.engagement_rate : metricForSort(row, topSort)
                }))}
                layout="vertical"
                margin={{ top: 8, right: 24, bottom: 8, left: 24 }}
              >
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis
                  type="number"
                  tickFormatter={topSort === 'engagement' ? (value) => `${value}%` : formatCompact}
                />
                <YAxis type="category" dataKey="name" width={140} tick={{ fontSize: 12 }} />
                <Tooltip formatter={topSort === 'engagement' ? formatTooltipPercent : formatTooltipNumber} />
                <Bar dataKey="value" name={contentSortLabels[topSort]} fill="var(--chart-c)" radius={[0, 4, 4, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        ) : (
          <div className="chart-empty">No content matches this range.</div>
        )}
      </section>
    </>
  );
}

function MetricCard({ metric, compare }: { metric: DashboardMetric; compare: boolean }) {
  const direction =
    metric.delta === null ? 'unavailable' : metric.delta > 0 ? 'positive' : metric.delta < 0 ? 'negative' : 'neutral';
  return (
    <article className={`metric-card ${direction}`}>
      <span>{metric.label}</span>
      <strong>{formatNumber(metric.value)}</strong>
      {compare ? (
        <small>
          {metric.delta === null ? (
            <span title="No comparable baseline snapshot is available.">Comparison unavailable</span>
          ) : (
            `${metric.delta >= 0 ? '+' : ''}${formatNumber(metric.delta)} ${metric.percent_change === null ? '(percent N/A)' : `(${metric.percent_change.toFixed(1)}%)`}`
          )}
        </small>
      ) : (
        <small>Comparison hidden</small>
      )}
    </article>
  );
}

function ChartSummary({ trend }: { trend: DashboardData['trend'] }) {
  return (
    <details className="sr-summary">
      <summary>Data table for trend chart</summary>
      <table>
        <thead>
          <tr>
            <th>Observed</th>
            <th>Followers</th>
            <th>Total likes</th>
          </tr>
        </thead>
        <tbody>
          {trend.map((point) => (
            <tr key={point.observed_at}>
              <td>{formatDate(point.observed_at)}</td>
              <td>{formatNumber(point.follower_count)}</td>
              <td>{formatNumber(point.likes_count)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </details>
  );
}

function metricForSort(row: ContentRow, sort: ContentSort) {
  if (sort === 'published_at') return row.published_at ? new Date(row.published_at).getTime() : null;
  if (sort === 'views') return row.view_count;
  if (sort === 'likes') return row.like_count;
  if (sort === 'comments') return row.comment_count;
  if (sort === 'shares') return row.share_count;
  return row.engagement_rate;
}

function contentLabel(row: ContentRow) {
  return row.title || row.description || row.provider_content_id;
}

function Content({
  workspace,
  content,
  sort,
  direction,
  search,
  page,
  pageSize,
  rangeQuery,
  onSort,
  onSearch,
  onPageChange,
  onPageSizeChange,
  onOpenDetail
}: {
  workspace: Workspace;
  content: ContentData | null;
  sort: ContentSort;
  direction: SortDirection;
  search: string;
  page: number;
  pageSize: number;
  rangeQuery: URLSearchParams;
  onSort: (sort: ContentSort, direction: SortDirection) => void;
  onSearch: (search: string) => void;
  onPageChange: (page: number) => void;
  onPageSizeChange: (size: number) => void;
  onOpenDetail: (contentId: string) => void;
}) {
  const total = content?.total || 0;
  const totalPages = Math.max(Math.ceil(total / pageSize), 1);
  const exportParams = new URLSearchParams(rangeQuery);
  exportParams.set('sort', sort);
  exportParams.set('direction', direction);
  if (search.trim()) exportParams.set('search', search.trim());
  const exportHref = `/api/workspaces/${workspace.id}/exports/content.csv?${exportParams.toString()}`;
  return (
    <section className="panel" aria-labelledby="content-title">
      <div className="panel-title between">
        <div>
          <h2 id="content-title">Content performance</h2>
          <p>
            {total} result{total === 1 ? '' : 's'} using active filters.
          </p>
        </div>
        <a
          className={`button-link ${workspace.role === 'viewer' ? 'disabled' : ''}`}
          href={workspace.role === 'viewer' ? undefined : exportHref}
        >
          <Download size={18} aria-hidden /> CSV
        </a>
      </div>
      <div className="toolbar content-toolbar">
        <label>
          Search
          <span className="input-icon">
            <Search size={16} aria-hidden />
            <input
              value={search}
              onChange={(event) => onSearch(event.target.value)}
              placeholder="Title, description, provider id"
            />
          </span>
        </label>
        <label>
          Page size
          <select value={pageSize} onChange={(event) => onPageSizeChange(Number(event.target.value))}>
            {[10, 25, 50].map((size) => (
              <option key={size} value={size}>
                {size}
              </option>
            ))}
          </select>
        </label>
      </div>
      {content && content.rows.length > 0 ? (
        <>
          <div className="table-wrap">
            <table className="content-table">
              <thead>
                <tr>
                  <SortableTh
                    label="Published"
                    value="published_at"
                    sort={sort}
                    direction={direction}
                    onSort={onSort}
                  />
                  <th scope="col">Content</th>
                  <SortableTh label="Views" value="views" sort={sort} direction={direction} onSort={onSort} />
                  <SortableTh label="Likes" value="likes" sort={sort} direction={direction} onSort={onSort} />
                  <SortableTh label="Comments" value="comments" sort={sort} direction={direction} onSort={onSort} />
                  <SortableTh label="Shares" value="shares" sort={sort} direction={direction} onSort={onSort} />
                  <SortableTh label="Engagement" value="engagement" sort={sort} direction={direction} onSort={onSort} />
                </tr>
              </thead>
              <tbody>
                {content.rows.map((row) => (
                  <tr key={row.id}>
                    <td data-label="Published">{formatDate(row.published_at, { dateStyle: 'medium' })}</td>
                    <td data-label="Content">
                      <button type="button" className="link-button" onClick={() => onOpenDetail(row.id)}>
                        {contentLabel(row)}
                      </button>
                      <small>{row.provider_content_id}</small>
                    </td>
                    <MetricCell label="Views" value={row.view_count} />
                    <MetricCell label="Likes" value={row.like_count} />
                    <MetricCell label="Comments" value={row.comment_count} />
                    <MetricCell label="Shares" value={row.share_count} />
                    <td data-label="Engagement">
                      {row.engagement_rate === null ? (
                        <span className="muted">N/A</span>
                      ) : (
                        formatPercent(row.engagement_rate)
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <Pagination page={page} totalPages={totalPages} onPageChange={onPageChange} />
        </>
      ) : (
        <div className="table-empty">
          {search ? 'No content matches the active filters.' : 'No content snapshots available.'}
        </div>
      )}
    </section>
  );
}

function SortableTh({
  label,
  value,
  sort,
  direction,
  onSort
}: {
  label: string;
  value: ContentSort;
  sort: ContentSort;
  direction: SortDirection;
  onSort: (sort: ContentSort, direction: SortDirection) => void;
}) {
  const active = sort === value;
  const nextDirection: SortDirection = active && direction === 'desc' ? 'asc' : 'desc';
  return (
    <th scope="col" aria-sort={active ? (direction === 'asc' ? 'ascending' : 'descending') : 'none'}>
      <button type="button" className="sort-button" onClick={() => onSort(value, nextDirection)}>
        {label}
        <span aria-hidden>{active ? (direction === 'asc' ? ' ↑' : ' ↓') : ''}</span>
      </button>
    </th>
  );
}

function MetricCell({ label, value }: { label: string; value: number | null }) {
  return <td data-label={label}>{value === null ? <span className="muted">N/A</span> : formatNumber(value)}</td>;
}

function ContentDetailView({ detail, onBack }: { detail: ContentDetail | null; onBack: () => void }) {
  if (!detail) {
    return (
      <section className="panel">
        <button type="button" className="ghost-button" onClick={onBack}>
          <ArrowLeft size={18} aria-hidden /> Back to content
        </button>
        <div className="chart-empty">Loading content detail or the item was not found in this workspace.</div>
      </section>
    );
  }
  const metadata = detail.item.provider_metadata || {};
  const historyData = detail.history.map((point) => ({
    ...point,
    label: formatDate(point.observed_at, { month: 'short', day: 'numeric' })
  }));
  return (
    <section className="detail-layout" aria-labelledby="content-detail-title">
      <button type="button" className="ghost-button" onClick={onBack}>
        <ArrowLeft size={18} aria-hidden /> Back to content
      </button>
      <div className="panel detail-hero">
        <div>
          <p className="eyebrow">Content detail</p>
          <h2 id="content-detail-title">{contentLabel(detail.item)}</h2>
          <p>{detail.item.description || 'No provider description available.'}</p>
        </div>
        <div className="thumbnail-fallback">
          <Video size={32} aria-hidden />
          <span>{String(metadata.thumbnail_state || 'thumbnail unavailable')}</span>
        </div>
      </div>
      <section className="metric-grid compact">
        <article className="metric-card">
          <span>Views</span>
          <strong>{formatNumber(detail.current_metrics?.view_count)}</strong>
        </article>
        <article className="metric-card">
          <span>Likes</span>
          <strong>{formatNumber(detail.current_metrics?.like_count)}</strong>
        </article>
        <article className="metric-card">
          <span>Comments</span>
          <strong>{formatNumber(detail.current_metrics?.comment_count)}</strong>
        </article>
        <article className="metric-card">
          <span>Engagement</span>
          <strong>{formatPercent(detail.current_metrics?.engagement_rate)}</strong>
        </article>
      </section>
      <section className="panel">
        <div className="settings-list">
          <span>Published: {formatDate(detail.item.published_at)}</span>
          <span>Observed: {formatDate(detail.current_metrics?.observed_at)}</span>
          <span>Duration: {detail.item.duration_seconds ? `${detail.item.duration_seconds}s` : 'N/A'}</span>
          <span>
            Size: {detail.item.width && detail.item.height ? `${detail.item.width}×${detail.item.height}` : 'N/A'}
          </span>
          {detail.item.share_url && (
            <a href={detail.item.share_url} target="_blank" rel="noreferrer">
              Open provider link
            </a>
          )}
        </div>
        <p className="muted">
          Engagement rate = (likes + comments + shares) / views. It is unavailable when views are zero or missing.
        </p>
      </section>
      <section className="panel chart-panel" aria-labelledby="history-title">
        <div className="panel-title">
          <h2 id="history-title">Metric history</h2>
        </div>
        {historyData.length > 1 ? (
          <div className="chart-box" role="img" aria-label="Line chart of content views and engagement over time">
            <ResponsiveContainer width="100%" height={280}>
              <LineChart data={historyData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="label" />
                <YAxis yAxisId="views" tickFormatter={formatCompact} />
                <YAxis yAxisId="engagement" orientation="right" tickFormatter={(value) => `${value}%`} />
                <Tooltip formatter={formatTooltipNumber} />
                <Legend />
                <Line yAxisId="views" dataKey="view_count" name="Views" stroke="var(--chart-a)" strokeWidth={2.5} />
                <Line
                  yAxisId="engagement"
                  dataKey="engagement_rate"
                  name="Engagement rate"
                  stroke="var(--chart-c)"
                  strokeWidth={2.5}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        ) : (
          <div className="chart-empty">A single observation is available for this content item.</div>
        )}
      </section>
    </section>
  );
}

function Connections({
  role,
  dashboard,
  busy,
  disconnectOpen,
  onConnect,
  onDisconnectRequest,
  onDisconnectCancel,
  onDisconnectConfirm
}: {
  role: Role;
  dashboard: DashboardData | null;
  busy: boolean;
  disconnectOpen: boolean;
  onConnect: () => void;
  onDisconnectRequest: () => void;
  onDisconnectCancel: () => void;
  onDisconnectConfirm: () => void;
}) {
  const allowed = roleCanManage(role);
  const status = dashboard?.connection.status || 'disconnected';
  const latestError = dashboard?.latest_sync?.error_category;
  return (
    <section className="panel" aria-labelledby="connection-title">
      <div className="panel-title between">
        <div>
          <h2 id="connection-title">TikTok connection</h2>
          <p>Provider credentials are stored only on the server and are never shown here.</p>
        </div>
        <StatusBadge status={status} />
      </div>
      <div className="connection-states">
        {['disconnected', 'connecting', 'active', 'missing scopes', 'reconnect_required', 'revoking', 'revoked'].map(
          (item) => (
            <span
              key={item}
              className={
                status === item || (item === 'missing scopes' && latestError === 'scope')
                  ? 'state-chip active'
                  : 'state-chip'
              }
            >
              {item.replace(/_/g, ' ')}
            </span>
          )
        )}
      </div>
      {dashboard?.connection.reconnect_reason && (
        <p className="notice error">{dashboard.connection.reconnect_reason}</p>
      )}
      <div className="toolbar">
        <button type="button" disabled={!allowed || busy} onClick={onConnect}>
          <ExternalLink size={18} aria-hidden /> Connect or reconnect
        </button>
        <button type="button" disabled={!allowed || busy || status === 'disconnected'} onClick={onDisconnectRequest}>
          <Lock size={18} aria-hidden /> Disconnect
        </button>
      </div>
      {!allowed && <p className="muted">Connection management requires owner or admin access.</p>}
      {disconnectOpen && (
        <div className="dialog-backdrop" role="presentation">
          <section className="dialog" role="dialog" aria-modal="true" aria-labelledby="disconnect-title">
            <h3 id="disconnect-title">Disconnect TikTok?</h3>
            <p>
              This attempts provider revocation first, then disables local credentials. Existing snapshots remain in the
              workspace.
            </p>
            <div className="dialog-actions">
              <button type="button" onClick={onDisconnectCancel}>
                Cancel
              </button>
              <button type="button" className="danger" onClick={onDisconnectConfirm} disabled={busy}>
                Disconnect
              </button>
            </div>
          </section>
        </div>
      )}
    </section>
  );
}

function Members({
  role,
  members,
  invitations,
  busy,
  onInvite,
  onRoleChange,
  onRemove
}: {
  role: Role;
  members: Member[];
  invitations: Invitation[];
  busy: boolean;
  onInvite: (email: string, role: Exclude<Role, 'owner'>) => void;
  onRoleChange: (member: Member, role: Role) => void;
  onRemove: (member: Member) => void;
}) {
  const canManage = roleCanManage(role);
  const [inviteEmail, setInviteEmail] = useState('');
  const [inviteRole, setInviteRole] = useState<Exclude<Role, 'owner'>>('viewer');
  return (
    <section className="panel" aria-labelledby="members-title">
      <div className="panel-title between">
        <div>
          <h2 id="members-title">Members</h2>
          <p>Role changes are enforced again by the server.</p>
        </div>
      </div>
      <form
        className="toolbar"
        onSubmit={(event) => {
          event.preventDefault();
          if (inviteEmail.trim()) {
            onInvite(inviteEmail.trim(), inviteRole);
            setInviteEmail('');
          }
        }}
      >
        <label>
          Invite email
          <input
            value={inviteEmail}
            onChange={(event) => setInviteEmail(event.target.value)}
            type="email"
            disabled={!canManage || busy}
            placeholder="teammate@example.com"
          />
        </label>
        <label>
          Role
          <select
            value={inviteRole}
            onChange={(event) => setInviteRole(event.target.value as Exclude<Role, 'owner'>)}
            disabled={!canManage || busy}
          >
            <option value="viewer">Viewer</option>
            <option value="analyst">Analyst</option>
            <option value="admin">Admin</option>
          </select>
        </label>
        <button type="submit" disabled={!canManage || busy || !inviteEmail.trim()}>
          <UserPlus size={18} aria-hidden /> Invite
        </button>
      </form>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th scope="col">Member</th>
              <th scope="col">Role</th>
              <th scope="col">Status</th>
              <th scope="col">Actions</th>
            </tr>
          </thead>
          <tbody>
            {members.map((member) => (
              <tr key={member.user_id}>
                <td data-label="Member">{member.email}</td>
                <td data-label="Role">
                  <select
                    value={member.role}
                    aria-label={`Role for ${member.email}`}
                    disabled={!canManage || busy}
                    onChange={(event) => onRoleChange(member, event.target.value as Role)}
                  >
                    <option value="owner">Owner</option>
                    <option value="admin">Admin</option>
                    <option value="analyst">Analyst</option>
                    <option value="viewer">Viewer</option>
                  </select>
                </td>
                <td data-label="Status">
                  <StatusBadge status={member.status || 'active'} />
                </td>
                <td data-label="Actions">
                  <button
                    type="button"
                    className="ghost-button"
                    disabled={!canManage || busy}
                    onClick={() => onRemove(member)}
                  >
                    <Trash2 size={16} aria-hidden /> Remove
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <h3>Invitations</h3>
      <div className="settings-list invitation-list">
        {invitations.map((invitation) => (
          <span key={invitation.id}>
            {invitation.email} · {invitation.role} · {invitationStatus(invitation)}
          </span>
        ))}
        {invitations.length === 0 && <span>No invitations loaded.</span>}
      </div>
      <p className="muted">Invitation resend and revoke controls are not exposed by the current API.</p>
      {!canManage && <div className="table-empty">Member management requires owner or admin access.</div>}
    </section>
  );
}

function invitationStatus(invitation: Invitation) {
  if (invitation.revoked_at) return 'revoked';
  if (invitation.accepted_at) return 'accepted';
  if (new Date(invitation.expires_at).getTime() < Date.now()) return 'expired';
  return 'pending';
}

function SyncHistory({
  syncData,
  page,
  expandedRun,
  onPageChange,
  onToggleRun
}: {
  syncData: SyncData;
  page: number;
  expandedRun: string;
  onPageChange: (page: number) => void;
  onToggleRun: (runId: string) => void;
}) {
  const totalPages = Math.max(Math.ceil((syncData.total || 0) / (syncData.limit || 25)), 1);
  return (
    <section className="panel" aria-labelledby="sync-title">
      <div className="panel-title between">
        <div>
          <h2 id="sync-title">Sync history</h2>
          <p>
            {syncData.total} run{syncData.total === 1 ? '' : 's'} recorded.
          </p>
        </div>
      </div>
      {syncData.sync_runs.length > 0 ? (
        <>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th scope="col">Started</th>
                  <th scope="col">Completed</th>
                  <th scope="col">Trigger</th>
                  <th scope="col">Status</th>
                  <th scope="col">Duration</th>
                  <th scope="col">Counts</th>
                  <th scope="col">Details</th>
                </tr>
              </thead>
              <tbody>
                {syncData.sync_runs.map((run) => (
                  <React.Fragment key={run.id}>
                    <tr>
                      <td data-label="Started">{formatDate(run.started_at)}</td>
                      <td data-label="Completed">{formatDate(run.finished_at)}</td>
                      <td data-label="Trigger">{run.trigger_type}</td>
                      <td data-label="Status">
                        <StatusBadge status={run.status} />
                      </td>
                      <td data-label="Duration">{formatDuration(run.duration_ms)}</td>
                      <td data-label="Counts">
                        {formatNumber(run.profile_count)} profiles · {formatNumber(run.content_seen_count)} seen ·{' '}
                        {formatNumber(run.content_snapshot_count)} snapshots
                      </td>
                      <td data-label="Details">
                        <button
                          type="button"
                          className="ghost-button"
                          onClick={() => onToggleRun(run.id)}
                          aria-expanded={expandedRun === run.id}
                        >
                          {expandedRun === run.id ? 'Hide' : 'Expand'}
                        </button>
                      </td>
                    </tr>
                    {expandedRun === run.id && (
                      <tr className="detail-row">
                        <td colSpan={7}>
                          Attempt {run.attempt || 1}. Error category: {run.error_category || 'N/A'}. Provider code:{' '}
                          {run.provider_code || 'N/A'}. Retryable:{' '}
                          {run.retryable === null || run.retryable === undefined ? 'N/A' : run.retryable ? 'yes' : 'no'}
                          .
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                ))}
              </tbody>
            </table>
          </div>
          <Pagination page={page} totalPages={totalPages} onPageChange={onPageChange} />
        </>
      ) : (
        <div className="table-empty">No sync runs recorded.</div>
      )}
    </section>
  );
}

function Account({ user, onSignOut }: { user: User; onSignOut: () => void }) {
  return (
    <section className="panel" aria-labelledby="account-title">
      <div className="panel-title">
        <Settings size={18} aria-hidden />
        <h2 id="account-title">Account and session</h2>
      </div>
      <div className="settings-list">
        <span>{user.email}</span>
        <span>Opaque server session</span>
        <span>HttpOnly session cookie</span>
        <span>SameSite=Lax CSRF cookie</span>
      </div>
      <button type="button" onClick={onSignOut}>
        <LogOut size={18} aria-hidden /> Sign out
      </button>
    </section>
  );
}

function StatusBadge({ status }: { status: string }) {
  return <span className={`status-badge ${status.replace(/_/g, '-')}`}>{status.replace(/_/g, ' ')}</span>;
}

function Pagination({
  page,
  totalPages,
  onPageChange
}: {
  page: number;
  totalPages: number;
  onPageChange: (page: number) => void;
}) {
  return (
    <div className="pagination" aria-label="Pagination">
      <button type="button" disabled={page <= 1} onClick={() => onPageChange(page - 1)}>
        Previous
      </button>
      <span>
        Page {page} of {totalPages}
      </span>
      <button type="button" disabled={page >= totalPages} onClick={() => onPageChange(page + 1)}>
        Next
      </button>
    </div>
  );
}

createRoot(document.getElementById('root')!).render(<App />);
