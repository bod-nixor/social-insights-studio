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
  Facebook,
  FileText,
  Instagram,
  Link2,
  Loader2,
  Lock,
  LogOut,
  Mail,
  KeyRound,
  RefreshCw,
  Search,
  Settings,
  ShieldAlert,
  Trash2,
  Unplug,
  UserPlus,
  Users,
  Video,
  Youtube
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

type View = 'overview' | 'sources' | 'content' | 'reports' | 'connections' | 'members' | 'sync' | 'account';
type LoadState = 'ready' | 'loading' | 'empty' | 'stale' | 'partial' | 'permission' | 'error' | 'reconnect';
type RangeKey = '7d' | '30d' | '90d' | 'custom';
type Role = 'owner' | 'admin' | 'analyst' | 'viewer';
type SortDirection = 'asc' | 'desc';
type ContentSort = 'published_at' | 'views' | 'likes' | 'comments' | 'shares' | 'engagement';
type OverviewProvider = 'tiktok' | 'youtube' | 'facebook_pages' | 'instagram' | 'google_analytics_4';
type SocialContentProvider = 'all' | Exclude<OverviewProvider, 'google_analytics_4'>;

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
  provider: Exclude<OverviewProvider, 'google_analytics_4'>;
  connection_id: string | null;
  resource_name: string | null;
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

type ProviderConnection = {
  id?: string;
  status: string;
  reconnect_reason?: string | null;
  last_sync_at?: string | null;
  last_successful_sync_at?: string | null;
  next_sync_at?: string | null;
  data_through_at?: string | null;
  account?: {
    id: string;
    username?: string | null;
    display_name?: string | null;
    thumbnail_url?: string | null;
    account_name?: string | null;
    timezone?: string | null;
    currency?: string | null;
  } | null;
  capabilities?: Array<{ key: string; status: string; reason?: string | null }>;
};

type ProviderCatalogItem = {
  id: string;
  name: string;
  resourceName: string;
  enabled: boolean;
  implemented: boolean;
  connectable: boolean;
  status: string;
  capabilities: string[];
  requestedScopes: Array<{ name: string; access: string; purpose: string }>;
  connection?: ProviderConnection | null;
  configuration?: { status: string; warnings: string[] };
  authorization?: {
    id: string;
    status: string;
    granted_at?: string | null;
    last_validated_at?: string | null;
    failure_category?: string | null;
    missing_scopes?: string[];
    scopes: Array<{ scope: string; status: string }>;
  } | null;
  resources?: Array<{
    id: string;
    provider_resource_id: string;
    display_name: string;
    thumbnail_url?: string | null;
    username?: string | null;
    source_page_name?: string | null;
    account_name?: string | null;
    timezone?: string | null;
    currency?: string | null;
    subscriber_count_hidden?: boolean;
    attached_elsewhere_count?: number;
    available?: boolean;
    unavailable_reason?: string | null;
    selected: boolean;
  }>;
  connections?: ProviderConnection[];
};

type YouTubeMetric = DashboardMetric & {
  semantics: string;
  available: boolean;
};

type YouTubeContentRow = {
  id: string;
  provider_content_id: string;
  title: string | null;
  thumbnail_url: string | null;
  published_at: string | null;
  share_url: string | null;
  period: { key: string; from: string; to: string };
  data_through_date: string | null;
  views: number | null;
  watch_time_minutes: number | null;
  average_view_duration_seconds: number | null;
  average_view_percentage: number | null;
  likes: number | null;
  comments: number | null;
  shares: number | null;
  availability: Record<string, string>;
};

type YouTubeDashboardData = {
  provider: 'youtube';
  range: {
    key: RangeKey;
    from: string;
    to: string;
    previousFrom: string;
    previousTo: string;
    videoPeriodKey: string | null;
  };
  connection: {
    id?: string;
    status: string;
    reconnect_reason?: string | null;
    last_sync_at?: string | null;
    last_successful_sync_at?: string | null;
    next_sync_at?: string | null;
  };
  channel: {
    id: string;
    display_name: string;
    thumbnail_url: string | null;
    subscriber_count_hidden: boolean;
    availability: Record<string, string>;
  } | null;
  metrics: YouTubeMetric[];
  trend: Array<{
    date: string;
    views: number | null;
    watch_time_minutes: number | null;
    subscribers_gained: number | null;
    subscribers_lost: number | null;
    net_subscribers: number | null;
    availability: Record<string, string>;
  }>;
  content: YouTubeContentRow[];
  availability: {
    state: string;
    data_through_date: string | null;
    requested_through_date: string;
    video_period_supported?: boolean;
    note?: string | null;
  };
};

type MetaDashboardData = {
  provider: 'facebook_pages' | 'instagram';
  range: { from: string; to: string; previous_from?: string; previous_to?: string };
  connection: ProviderConnection;
  account: {
    id: string;
    display_name: string;
    username?: string | null;
    thumbnail_url?: string | null;
    source_page_name?: string | null;
  } | null;
  metrics: Array<DashboardMetric & { available: boolean; semantics: string }>;
  trend: Array<Record<string, unknown> & { date: string }>;
  content: ContentRow[];
  latest_sync: SyncRun | null;
  availability: { state: string; data_through_date?: string | null; note?: string | null };
};

type GoogleAnalyticsMetric = DashboardMetric & {
  unit: 'count' | 'ratio' | 'seconds';
  available: boolean;
  availability_status: string;
  availability_reason?: string | null;
  baseline_availability_status: string;
  definition: string;
  definition_version: string;
};

type GoogleAnalyticsBreakdown = {
  key: string;
  label: string;
  subject_to_thresholding: boolean;
  data_through_date: string | null;
  rows: Array<{
    dimensions: Record<string, string>;
    metrics: Record<string, number | null>;
    availability: Record<string, { status: string; reason?: string | null }>;
    thresholded: boolean;
  }>;
};

type GoogleAnalyticsDashboardData = {
  provider: 'google_analytics_4';
  range: {
    key: RangeKey;
    from: string;
    to: string;
    previousFrom: string;
    previousTo: string;
    timezone: string;
  };
  connection: ProviderConnection;
  property: {
    id: string;
    display_name: string;
    account_name: string | null;
    timezone: string;
    currency: string | null;
    property_type: string | null;
    service_level: string | null;
  } | null;
  metrics: GoogleAnalyticsMetric[];
  trend: Array<{
    date: string;
    active_users?: number | null;
    new_users?: number | null;
    sessions?: number | null;
    screen_page_views?: number | null;
    engagement_rate?: number | null;
    bounce_rate?: number | null;
    availability: Record<string, { status: string; reason?: string | null }>;
  }>;
  breakdowns: GoogleAnalyticsBreakdown[];
  availability: {
    state: string;
    data_through_date: string | null;
    requested_through_date: string;
    subject_to_thresholding?: boolean;
    exact_range_available?: boolean;
    note?: string | null;
  };
};

type CrossPlatformMetric = DashboardMetric & {
  family: string;
  unit: 'count' | 'minutes' | 'ratio' | 'seconds' | string;
  available: boolean;
  availability_status: string;
  availability_reason?: string | null;
  semantics?: string | null;
  definition?: string | null;
  definition_version?: string | null;
};

type CrossPlatformSource = {
  id: string;
  provider: OverviewProvider;
  provider_name: string;
  status: string;
  configuration_status?: string | null;
  connected_resource_count: number;
  resource: {
    connection_id: string | null;
    id: string;
    display_name: string;
    account_name?: string | null;
    timezone?: string | null;
  } | null;
  range: {
    key?: RangeKey | null;
    from: string | null;
    to: string | null;
    previous_from?: string | null;
    previous_to?: string | null;
    timezone?: string | null;
    provider_period_days?: number | null;
  };
  has_data: boolean;
  demo_data: boolean;
  freshness: {
    state: string;
    last_successful_sync_at: string | null;
    data_through_date: string | null;
    next_sync_at: string | null;
  };
  metrics: CrossPlatformMetric[];
  trend: {
    series: Array<{ key: string; label: string; unit: string }>;
    points: Array<{ date: string; values: Record<string, number | null> }>;
  };
  top_content: Array<{
    id: string;
    kind: 'social_content' | 'website_path';
    title: string;
    published_at: string | null;
    share_url: string | null;
    primary_metric: { key: string; label: string; unit: string; value: number | null };
  }>;
  availability: {
    state?: string | null;
    note?: string | null;
    subject_to_thresholding?: boolean;
  };
  alert: {
    severity: 'critical' | 'warning' | 'info';
    code: string;
    message: string;
  } | null;
};

type CrossPlatformDashboardData = {
  range: {
    key: RangeKey;
    from: string;
    to: string;
    previous_from: string;
    previous_to: string;
    comparison: 'previous_period';
  };
  state: 'ready' | 'partial' | 'empty' | 'reconnect';
  demo_data: boolean;
  summary: {
    connected_resources: number;
    resources_with_data: number;
    attention_count: number;
  };
  sources: CrossPlatformSource[];
  alerts: Array<{
    source_id: string;
    severity: 'critical' | 'warning' | 'info';
    code: string;
    message: string;
  }>;
  methodology: string[];
};

type ReportConfiguration = {
  enabled: boolean;
  ready: boolean;
  retention_days: number;
  max_resources: number;
  max_range_days: number;
  supported_sections: string[];
  renderer: string;
};

type ReportResource = {
  connection_id: string;
  provider: OverviewProvider;
  provider_resource_id: string;
  resource_name: string;
  data_through_at: string | null;
};

type ReportRun = {
  id: string;
  definition_id: string;
  title: string;
  subtitle: string | null;
  timezone: string;
  range: { from: string; to: string };
  comparison_enabled: boolean;
  status: 'queued' | 'running' | 'completed' | 'failed' | 'expired';
  progress_percent: number;
  failure_category: string | null;
  failure_code: string | null;
  queued_at: string;
  started_at: string | null;
  finished_at: string | null;
  expires_at: string | null;
  data_through_at: string | null;
  artifact: {
    id: string;
    filename: string;
    byte_size: number;
    page_count: number;
    sha256: string;
  } | null;
  sections: string[];
  resources: ReportResource[];
};

type ReportRequest = {
  title: string;
  subtitle: string;
  timezone: string;
  range: RangeKey;
  from?: string;
  to?: string;
  comparison_enabled: boolean;
  sections: string[];
  resources: Array<{ provider: OverviewProvider; connection_id: string }>;
};

type ReportPreview = {
  title: string;
  subtitle: string | null;
  timezone: string;
  range: { from: string; to: string };
  comparison_enabled: boolean;
  sections: Array<{ key: string; included: boolean }>;
  resources: Array<{
    provider: OverviewProvider;
    provider_name: string;
    connection_id: string;
    resource_name: string;
    status: string;
    data_through_date: string | null;
    available_metric_count: number;
  }>;
  estimated_page_count: number;
  retention_days: number;
};

type DisconnectTarget = {
  provider: 'tiktok' | 'youtube' | 'facebook' | 'instagram' | 'google-analytics';
  connectionId?: string;
  label: string;
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
  last_sent_at: string;
  send_count: number;
  expires_at: string;
  accepted_at: string | null;
  revoked_at: string | null;
  invited_by_email: string;
};

type AccountSession = {
  id: string;
  device_label: string;
  created_at: string;
  last_seen_at: string;
  expires_at: string;
  idle_expires_at: string;
  current: boolean;
};

type DeletionRequest = {
  id: string;
  workspace_id: string | null;
  workspace_name: string | null;
  scope: 'user' | 'workspace' | 'provider_account';
  status: string;
  requested_at: string;
  completed_at: string | null;
};

type AccountData = {
  profile: User & { created_at: string; last_login_at: string | null };
  authentication_methods: Array<{ provider: string; email: string | null; connected_at: string }>;
  sessions: AccountSession[];
  deletion_requests: DeletionRequest[];
};

const views: Array<{ id: View; label: string; icon: React.ComponentType<{ size?: number; 'aria-hidden'?: boolean }> }> =
  [
    { id: 'overview', label: 'Overview', icon: BarChart3 },
    { id: 'sources', label: 'Sources', icon: Activity },
    { id: 'content', label: 'Content', icon: Video },
    { id: 'reports', label: 'Reports', icon: FileText },
    { id: 'connections', label: 'Connections', icon: Link2 },
    { id: 'members', label: 'Members', icon: Users },
    { id: 'sync', label: 'Sync history', icon: RefreshCw },
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

function formatMinutes(value?: number | null) {
  if (value === null || value === undefined) return 'N/A';
  return `${formatNumber(value)} min`;
}

function formatSeconds(value?: number | null) {
  if (value === null || value === undefined) return 'N/A';
  const wholeSeconds = Math.max(0, Math.round(value));
  const minutes = Math.floor(wholeSeconds / 60);
  const seconds = String(wholeSeconds % 60).padStart(2, '0');
  return `${minutes}:${seconds}`;
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

function friendlyError(code: string) {
  const messages: Record<string, string> = {
    already_a_member: 'That person is already a member of this workspace.',
    invitation_pending: 'A pending invitation already exists for that email address.',
    invitation_resend_cooldown: 'Please wait a minute before resending this invitation.',
    invitation_send_limit_reached: 'This invitation has reached its resend limit. Revoke it and create a new one.',
    invitation_email_mismatch: 'Sign in with the email address that received this invitation.',
    invitation_invalid_or_expired: 'This invitation is no longer valid. Ask a workspace owner to send a new one.',
    last_owner_required: 'Every workspace must keep at least one owner.',
    owner_management_requires_owner: 'Only another workspace owner can change or remove an owner.',
    permission_denied: 'Your workspace role does not allow this action.',
    account_deletion_confirmation_invalid: 'Enter your full email address to confirm the request.',
    workspace_deletion_confirmation_invalid: 'Enter the exact workspace name to confirm the request.',
    display_name_too_long: 'Display names must be 100 characters or fewer.',
    session_not_found: 'That session is no longer active.',
    invalid_email: 'Enter a valid email address.',
    mail_send_failed: 'The invitation could not be sent. Try again or contact support.',
    pdf_reports_disabled: 'PDF reports are not enabled for this environment.',
    pdf_reports_not_configured: 'PDF report storage is not configured. Ask an operator to review the reporting setup.',
    invalid_report_resource_count: 'Select at least one connected resource.',
    report_resource_not_found: 'A selected resource is no longer available in this workspace.',
    report_resource_not_connected: 'A selected resource is disconnected and cannot be included.',
    report_source_snapshot_unavailable: 'Stored analytics for a selected resource could not be prepared.',
    report_snapshot_too_large: 'This report selection is too large. Choose fewer resources or a shorter range.',
    report_artifact_not_available: 'This report is not ready to download or has expired.',
    report_artifact_missing: 'The report file is unavailable. Generate the report again.',
    report_artifact_delete_failed: 'The report file could not be removed. Try again or contact support.',
    download_grant_expired: 'The download link expired. Request a new one.',
    download_grant_consumed: 'That one-time download link has already been used.'
  };
  return messages[code] || 'Something went wrong. Try again or contact support if the problem continues.';
}

function pathDetailState() {
  const match = window.location.pathname.match(/^\/workspaces\/([^/]+)\/content\/([^/]+)/);
  return match ? { workspaceId: match[1], contentId: match[2] } : null;
}

function initialUrlState() {
  const params = new URLSearchParams(window.location.search);
  const detail = pathDetailState();
  const contentProvider = params.get('provider');
  return {
    view: (params.get('view') as View) || (detail ? 'content' : 'overview'),
    workspaceId: detail?.workspaceId || params.get('workspace') || '',
    contentId: detail?.contentId || '',
    range: (params.get('range') as RangeKey) || '30d',
    from: params.get('from') || todayInputValue(-30),
    to: params.get('to') || todayInputValue(0),
    metric: params.get('metric') || 'both',
    resource: params.get('resource') || '',
    contentProvider: (['all', 'tiktok', 'youtube', 'facebook_pages', 'instagram'].includes(contentProvider || '')
      ? contentProvider
      : 'all') as SocialContentProvider,
    contentResource: params.get('resource') || '',
    provider: (['youtube', 'facebook_pages', 'instagram', 'google_analytics_4'].includes(params.get('provider') || '')
      ? params.get('provider')
      : 'tiktok') as OverviewProvider,
    youtubeOutcome: params.get('youtube') || '',
    facebookOutcome: params.get('facebook') || '',
    instagramOutcome: params.get('instagram') || '',
    analyticsOutcome: params.get('analytics') || '',
    invitation: params.get('invitation') || '',
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

function resolveYouTubeLoadState(dashboard: YouTubeDashboardData | null): LoadState {
  if (!dashboard || dashboard.connection.status === 'disconnected') return 'empty';
  if (dashboard.connection.status === 'reconnect_required') return 'reconnect';
  const hasStoredData = dashboard.trend.length > 0 || dashboard.metrics.some((metric) => metric.available);
  if (dashboard.connection.status === 'active' && hasStoredData) {
    return dashboard.availability.state === 'delayed' ? 'partial' : 'ready';
  }
  return dashboard.connection.status === 'active' ? 'empty' : 'partial';
}

function resolveMetaLoadState(dashboard: MetaDashboardData | null): LoadState {
  if (!dashboard || dashboard.connection.status === 'disconnected') return 'empty';
  if (dashboard.connection.status === 'reconnect_required') return 'reconnect';
  if (dashboard.latest_sync?.status === 'failed') return 'error';
  if (dashboard.latest_sync?.status === 'partial') return 'partial';
  return dashboard.metrics.some((metric) => metric.available) || dashboard.content.length > 0 ? 'ready' : 'empty';
}

function resolveGoogleAnalyticsLoadState(dashboard: GoogleAnalyticsDashboardData | null): LoadState {
  if (!dashboard || dashboard.connection.status === 'disconnected') return 'empty';
  if (dashboard.connection.status === 'reconnect_required') return 'reconnect';
  if (dashboard.connection.status !== 'active') return 'partial';
  if (dashboard.availability.state === 'ready' && dashboard.metrics.some((metric) => metric.available)) return 'ready';
  if (['partial', 'delayed', 'thresholded'].includes(dashboard.availability.state)) return 'partial';
  return dashboard.metrics.some((metric) => metric.available) ? 'ready' : 'empty';
}

function resolveCrossPlatformLoadState(dashboard: CrossPlatformDashboardData | null): LoadState {
  if (!dashboard) return 'empty';
  if (dashboard.state === 'reconnect') return 'reconnect';
  if (dashboard.state === 'partial') return 'partial';
  if (dashboard.state === 'empty') return 'empty';
  return 'ready';
}

function roleCanManage(role?: Role) {
  return role === 'owner' || role === 'admin';
}

function roleCanSync(role?: Role) {
  return role === 'owner' || role === 'admin' || role === 'analyst';
}

function roleCanReport(role?: Role) {
  return role === 'owner' || role === 'admin' || role === 'analyst';
}

function reportFailureMessage(report: ReportRun) {
  if (report.failure_category === 'storage') {
    return 'Private report storage is temporarily unavailable. Try again after an operator restores it.';
  }
  if (report.failure_category === 'limit') {
    return 'This report exceeded a generation limit. Choose fewer resources or a shorter range.';
  }
  if (report.failure_category === 'cancelled') {
    return 'This report was cancelled and is no longer being prepared.';
  }
  return 'The report could not be generated. Try again or contact support if the problem continues.';
}

function providerAccessLabels(providerId: string) {
  const labels: Record<string, string[]> = {
    tiktok: ['Profile and audience totals', 'Published video performance'],
    youtube: ['Channel and video details', 'Channel analytics'],
    facebook_pages: ['Page identity and published posts', 'Page and post insights'],
    instagram: ['Professional account and media', 'Account and media insights'],
    google_analytics_4: ['Property discovery and website analytics']
  };
  return labels[providerId] || ['Analytics data'];
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
  const [overviewProvider, setOverviewProvider] = useState<OverviewProvider>(initial.provider);
  const [sourceConnectionId, setSourceConnectionId] = useState(initial.resource);
  const [topSort, setTopSort] = useState<ContentSort>(initial.topSort);
  const [contentSort, setContentSort] = useState<ContentSort>(initial.contentSort);
  const [contentDir, setContentDir] = useState<SortDirection>(initial.contentDir);
  const [contentSearch, setContentSearch] = useState(initial.search);
  const [contentProvider, setContentProvider] = useState<SocialContentProvider>(initial.contentProvider);
  const [contentConnectionId, setContentConnectionId] = useState(initial.contentResource);
  const [contentPage, setContentPage] = useState(initial.page);
  const [contentPageSize, setContentPageSize] = useState(initial.pageSize);
  const [syncPage, setSyncPage] = useState(1);
  const [dashboard, setDashboard] = useState<DashboardData | null>(null);
  const [youtubeDashboard, setYouTubeDashboard] = useState<YouTubeDashboardData | null>(null);
  const [facebookDashboard, setFacebookDashboard] = useState<MetaDashboardData | null>(null);
  const [instagramDashboard, setInstagramDashboard] = useState<MetaDashboardData | null>(null);
  const [googleAnalyticsDashboard, setGoogleAnalyticsDashboard] = useState<GoogleAnalyticsDashboardData | null>(null);
  const [crossPlatformDashboard, setCrossPlatformDashboard] = useState<CrossPlatformDashboardData | null>(null);
  const [providerCatalog, setProviderCatalog] = useState<ProviderCatalogItem[]>([]);
  const [reportConfiguration, setReportConfiguration] = useState<ReportConfiguration | null>(null);
  const [reports, setReports] = useState<ReportRun[]>([]);
  const [reportPreview, setReportPreview] = useState<ReportPreview | null>(null);
  const [accountData, setAccountData] = useState<AccountData | null>(null);
  const [invitationToken, setInvitationToken] = useState(initial.invitation);
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
  const [disconnectTarget, setDisconnectTarget] = useState<DisconnectTarget | null>(null);
  const [expandedRun, setExpandedRun] = useState<string>('');

  const activeWorkspace = useMemo(
    () => workspaces.find((workspace) => workspace.id === activeWorkspaceId) || workspaces[0],
    [activeWorkspaceId, workspaces]
  );

  const activeSourceConnection = useMemo(() => {
    if (overviewProvider === 'youtube') return youtubeDashboard?.connection || null;
    if (overviewProvider === 'facebook_pages') return facebookDashboard?.connection || null;
    if (overviewProvider === 'instagram') return instagramDashboard?.connection || null;
    if (overviewProvider === 'google_analytics_4') return googleAnalyticsDashboard?.connection || null;
    return dashboard?.connection || null;
  }, [dashboard, facebookDashboard, googleAnalyticsDashboard, instagramDashboard, overviewProvider, youtubeDashboard]);

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
      setOverviewProvider(next.provider);
      setSourceConnectionId(next.resource);
      setCompare(next.compare);
      setTopSort(next.topSort);
      setContentSort(next.contentSort);
      setContentDir(next.contentDir);
      setContentSearch(next.search);
      setContentProvider(next.contentProvider);
      setContentConnectionId(next.contentResource);
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
    if (view === 'overview') {
      params.set('range', range);
      params.set('compare', String(compare));
    }
    if (view === 'sources') {
      params.set('range', range);
      params.set('metric', trendMetric);
      params.set('provider', overviewProvider);
      params.set('compare', String(compare));
      params.set('topSort', topSort);
      if (sourceConnectionId) params.set('resource', sourceConnectionId);
    }
    if (view === 'content') {
      params.set('range', range);
      params.set('provider', contentProvider);
      if (contentConnectionId) params.set('resource', contentConnectionId);
      params.set('sort', contentSort);
      params.set('direction', contentDir);
      params.set('page', String(contentPage));
      params.set('pageSize', String(contentPageSize));
      if (contentSearch) params.set('search', contentSearch);
    }
    if (view === 'sync' && syncPage > 1) params.set('page', String(syncPage));
    if ((view === 'overview' || view === 'sources' || view === 'content') && range === 'custom') {
      params.set('from', customFrom);
      params.set('to', customTo);
    }
    const path = contentDetailId ? `/workspaces/${activeWorkspace.id}/content/${contentDetailId}` : '/';
    window.history.replaceState({}, '', `${path}?${params.toString()}`);
  }, [
    user,
    activeWorkspace,
    view,
    range,
    customFrom,
    customTo,
    trendMetric,
    overviewProvider,
    sourceConnectionId,
    compare,
    topSort,
    contentSort,
    contentDir,
    contentSearch,
    contentProvider,
    contentConnectionId,
    contentPage,
    contentPageSize,
    contentDetailId,
    syncPage
  ]);

  useEffect(() => {
    if (!toast) return undefined;
    const timeout = window.setTimeout(() => setToast(''), 5000);
    return () => window.clearTimeout(timeout);
  }, [toast]);

  async function loadWorkspaces() {
    const workspaceResult = await api<{ workspaces: Workspace[] }>('/api/workspaces');
    setWorkspaces(workspaceResult.workspaces);
    setActiveWorkspaceId(workspaceResult.workspaces[0]?.id || '');
  }

  const loadAccountData = useCallback(async () => {
    const result = await api<AccountData>('/api/account');
    setAccountData(result);
    setUser(result.profile);
  }, []);

  const currentUserId = user?.id;
  useEffect(() => {
    if (!currentUserId || view !== 'account') return;
    void loadAccountData().catch(() => setMessage('account_load_failed'));
  }, [currentUserId, loadAccountData, view]);

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
      contentParams.set('provider', contentProvider);
      if (contentConnectionId) contentParams.set('connection_id', contentConnectionId);
      contentParams.set('limit', String(contentPageSize));
      contentParams.set('offset', String((contentPage - 1) * contentPageSize));
      if (contentSearch.trim()) contentParams.set('search', contentSearch.trim());
      const syncParams = new URLSearchParams();
      syncParams.set('limit', '25');
      syncParams.set('offset', String((syncPage - 1) * 25));
      const providerParams = (provider: OverviewProvider) => {
        const params = new URLSearchParams(dashboardParams);
        if (view === 'sources' && provider === overviewProvider && sourceConnectionId) {
          params.set('connection_id', sourceConnectionId);
        }
        return params.toString();
      };
      const loadProviderDashboards = view === 'sources';
      const loadTikTokDashboard = ['sources', 'connections'].includes(view);
      const loadCatalog = ['sources', 'content', 'connections', 'reports'].includes(view);
      try {
        const [
          dashboardResult,
          youtubeDashboardResult,
          facebookDashboardResult,
          instagramDashboardResult,
          googleAnalyticsDashboardResult,
          crossPlatformDashboardResult,
          contentResult,
          syncResult,
          catalogResult,
          reportsResult,
          reportConfigurationResult
        ] = await Promise.all([
          loadTikTokDashboard
            ? api<DashboardData>(`/api/workspaces/${workspace.id}/dashboard?${dashboardParams.toString()}`)
            : Promise.resolve<DashboardData | null>(null),
          loadProviderDashboards
            ? api<YouTubeDashboardData>(
                `/api/workspaces/${workspace.id}/providers/youtube/dashboard?${providerParams('youtube')}`
              )
            : Promise.resolve<YouTubeDashboardData | null>(null),
          loadProviderDashboards
            ? api<MetaDashboardData>(
                `/api/workspaces/${workspace.id}/providers/facebook_pages/dashboard?${providerParams('facebook_pages')}`
              )
            : Promise.resolve<MetaDashboardData | null>(null),
          loadProviderDashboards
            ? api<MetaDashboardData>(
                `/api/workspaces/${workspace.id}/providers/instagram/dashboard?${providerParams('instagram')}`
              )
            : Promise.resolve<MetaDashboardData | null>(null),
          loadProviderDashboards
            ? api<GoogleAnalyticsDashboardData>(
                `/api/workspaces/${workspace.id}/providers/google_analytics_4/dashboard?${providerParams('google_analytics_4')}`
              )
            : Promise.resolve<GoogleAnalyticsDashboardData | null>(null),
          view === 'overview'
            ? api<CrossPlatformDashboardData>(
                `/api/workspaces/${workspace.id}/cross-platform-overview?${dashboardParams.toString()}`
              )
            : Promise.resolve<CrossPlatformDashboardData | null>(null),
          view === 'content'
            ? api<ContentData>(`/api/workspaces/${workspace.id}/content?${contentParams.toString()}`)
            : Promise.resolve<ContentData | null>(null),
          view === 'sync'
            ? api<SyncData>(`/api/workspaces/${workspace.id}/sync-runs?${syncParams.toString()}`)
            : Promise.resolve<SyncData | null>(null),
          loadCatalog
            ? api<{ providers: ProviderCatalogItem[] }>(`/api/workspaces/${workspace.id}/provider-catalog`)
            : Promise.resolve<{ providers: ProviderCatalogItem[] } | null>(null),
          view === 'reports' && roleCanReport(workspace.role)
            ? api<{ reports: ReportRun[] }>(`/api/workspaces/${workspace.id}/reports`)
            : Promise.resolve<{ reports: ReportRun[] } | null>(null),
          view === 'reports'
            ? api<{ reporting: ReportConfiguration }>('/api/reports/configuration')
            : Promise.resolve<{ reporting: ReportConfiguration } | null>(null)
        ]);
        if (dashboardResult) setDashboard(dashboardResult);
        if (youtubeDashboardResult) setYouTubeDashboard(youtubeDashboardResult);
        if (facebookDashboardResult) setFacebookDashboard(facebookDashboardResult);
        if (instagramDashboardResult) setInstagramDashboard(instagramDashboardResult);
        if (googleAnalyticsDashboardResult) setGoogleAnalyticsDashboard(googleAnalyticsDashboardResult);
        if (crossPlatformDashboardResult) setCrossPlatformDashboard(crossPlatformDashboardResult);
        if (contentResult) setContent(contentResult);
        if (syncResult) setSyncData(syncResult);
        if (catalogResult) setProviderCatalog(catalogResult.providers);
        if (reportsResult) setReports(reportsResult.reports);
        else if (view === 'reports') setReports([]);
        if (reportConfigurationResult) setReportConfiguration(reportConfigurationResult.reporting);
        if (view === 'members' && roleCanManage(workspace.role)) {
          const memberResult = await api<{ members: Member[]; invitations: Invitation[] }>(
            `/api/workspaces/${workspace.id}/members`
          );
          setMembers(memberResult.members);
          setInvitations(memberResult.invitations || []);
        } else if (view === 'members') {
          setMembers([]);
          setInvitations([]);
        }
        if (view === 'overview') {
          setState(resolveCrossPlatformLoadState(crossPlatformDashboardResult));
        } else if (view === 'sources') {
          setState(
            overviewProvider === 'youtube'
              ? resolveYouTubeLoadState(youtubeDashboardResult)
              : overviewProvider === 'facebook_pages'
                ? resolveMetaLoadState(facebookDashboardResult)
                : overviewProvider === 'instagram'
                  ? resolveMetaLoadState(instagramDashboardResult)
                  : overviewProvider === 'google_analytics_4'
                    ? resolveGoogleAnalyticsLoadState(googleAnalyticsDashboardResult)
                    : resolveLoadState(dashboardResult)
          );
        } else if (view === 'content') {
          setState(contentResult && contentResult.total > 0 ? 'ready' : 'empty');
        } else {
          setState('ready');
        }
      } catch (error) {
        const text = error instanceof Error ? error.message : 'load_failed';
        setMessage(text);
        setState(text === 'permission_denied' ? 'permission' : 'error');
      }
    },
    [
      contentDir,
      contentPage,
      contentPageSize,
      contentProvider,
      contentConnectionId,
      contentSearch,
      contentSort,
      overviewProvider,
      rangeInvalid,
      rangeQuery,
      sourceConnectionId,
      syncPage,
      topSort,
      view
    ]
  );

  useEffect(() => {
    if (!user || !activeWorkspace) return;
    void loadWorkspaceData(activeWorkspace);
  }, [user, activeWorkspace, loadWorkspaceData]);

  useEffect(() => {
    if (
      !user ||
      !activeWorkspace ||
      view !== 'reports' ||
      !roleCanReport(activeWorkspace.role) ||
      !reports.some((report) => report.status === 'queued' || report.status === 'running')
    )
      return undefined;
    const interval = window.setInterval(() => {
      void api<{ reports: ReportRun[] }>(`/api/workspaces/${activeWorkspace.id}/reports`)
        .then((result) => setReports(result.reports))
        .catch((error) => setMessage(error instanceof Error ? error.message : 'report_status_failed'));
    }, 5000);
    return () => window.clearInterval(interval);
  }, [activeWorkspace, reports, user, view]);

  useEffect(() => {
    if (!initial.youtubeOutcome) return;
    const outcomes: Record<string, string> = {
      selection_required: 'YouTube authorized. Select a channel to finish connecting.',
      no_channels: 'YouTube authorized, but no accessible channels were returned.',
      reconnected: 'YouTube authorization restored.',
      denied: 'You cancelled YouTube authorization. No connection was created.',
      missing_scopes: 'YouTube did not grant both required read-only permissions. Authorize again to continue.',
      configuration_error: 'YouTube authorization is temporarily unavailable. Try again later or contact support.',
      provider_error: 'Google or YouTube could not complete authorization. Try again after the provider recovers.',
      failed: 'YouTube authorization did not complete.'
    };
    setToast(outcomes[initial.youtubeOutcome] || 'YouTube authorization returned.');
    setView('connections');
  }, [initial.youtubeOutcome]);

  useEffect(() => {
    if (!initial.analyticsOutcome) return;
    const outcomes: Record<string, string> = {
      selection_required: 'Website Analytics authorized. Select a GA4 property to finish connecting.',
      no_properties: 'Website Analytics authorized, but no selectable GA4 properties were returned.',
      reconnected: 'Website Analytics authorization restored for the selected property.',
      denied: 'Website Analytics authorization was cancelled. No connection was created.',
      missing_scopes: 'Google did not grant the exact read-only Analytics permission.',
      configuration_error: 'Website Analytics authorization is temporarily unavailable. Contact support.',
      provider_error: 'Google Analytics could not complete authorization. Try again after Google recovers.',
      failed: 'Website Analytics authorization did not complete.'
    };
    setToast(outcomes[initial.analyticsOutcome] || 'Website Analytics authorization returned.');
    setView('connections');
  }, [initial.analyticsOutcome]);

  useEffect(() => {
    const providerOutcome = initial.facebookOutcome
      ? { name: 'Facebook Pages', value: initial.facebookOutcome }
      : initial.instagramOutcome
        ? { name: 'Instagram', value: initial.instagramOutcome }
        : null;
    if (!providerOutcome) return;
    const outcomes: Record<string, string> = {
      selection_required: `${providerOutcome.name} authorized. Select a resource to finish connecting.`,
      no_resources: `${providerOutcome.name} authorized, but no eligible accounts were available to select.`,
      reconnected: `${providerOutcome.name} authorization restored.`,
      selected_resource_unavailable: `${providerOutcome.name} authorization completed, but the selected resource was not returned and was not replaced. Choose an available resource explicitly or authorize again.`,
      account_mismatch: `${providerOutcome.name} was authorized with a different Meta user. The existing connection was not replaced; disconnect it before switching Meta users.`,
      denied: `${providerOutcome.name} authorization was cancelled. No connection was created.`,
      missing_scopes: `${providerOutcome.name} did not grant the exact approved read-only permissions.`,
      configuration_error: `${providerOutcome.name} authorization is temporarily unavailable. Try again later or contact support.`,
      provider_error: `Meta could not complete ${providerOutcome.name} authorization. Try again later.`,
      failed: `${providerOutcome.name} authorization did not complete.`
    };
    setToast(outcomes[providerOutcome.value] || `${providerOutcome.name} authorization returned.`);
    setView('connections');
  }, [initial.facebookOutcome, initial.instagramOutcome]);

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
        setMessage('Check your email and enter the verification code to continue.');
      }
    } catch {
      setMessage('We could not send a sign-in code. Try again or contact support.');
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
    } catch {
      setMessage('That sign-in code is invalid or expired. Request a new code and try again.');
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
      setAccountData(null);
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

  async function previewPdfReport(request: ReportRequest) {
    if (!activeWorkspace) return;
    setBusy(true);
    setMessage('');
    try {
      const result = await api<{ preview: ReportPreview }>(`/api/workspaces/${activeWorkspace.id}/reports/preview`, {
        method: 'POST',
        headers: { 'x-csrf-token': csrf },
        body: JSON.stringify(request)
      });
      setReportPreview(result.preview);
      setToast('Report preview updated from stored analytics.');
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'report_preview_failed');
    } finally {
      setBusy(false);
    }
  }

  async function generatePdfReport(request: ReportRequest) {
    if (!activeWorkspace) return;
    setBusy(true);
    setMessage('');
    try {
      const requestId = window.crypto.randomUUID();
      await api<{ report: ReportRun }>(`/api/workspaces/${activeWorkspace.id}/reports`, {
        method: 'POST',
        headers: { 'x-csrf-token': csrf },
        body: JSON.stringify({ ...request, request_id: requestId })
      });
      setReportPreview(null);
      setToast('Report queued. It will be generated in the background from the stored snapshot.');
      await loadWorkspaceData(activeWorkspace);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'report_generation_failed');
    } finally {
      setBusy(false);
    }
  }

  async function downloadPdfReport(report: ReportRun) {
    if (!activeWorkspace) return;
    setBusy(true);
    setMessage('');
    try {
      const grant = await api<{ download_url: string }>(
        `/api/workspaces/${activeWorkspace.id}/reports/${report.id}/download-grants`,
        {
          method: 'POST',
          headers: { 'x-csrf-token': csrf },
          body: JSON.stringify({})
        }
      );
      window.location.assign(grant.download_url);
      setToast('Your one-time report download has started.');
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'report_download_failed');
    } finally {
      setBusy(false);
    }
  }

  async function deletePdfReport(report: ReportRun) {
    if (!activeWorkspace || !window.confirm(`Delete “${report.title}”? Its PDF will no longer be downloadable.`))
      return;
    setBusy(true);
    setMessage('');
    try {
      await api(`/api/workspaces/${activeWorkspace.id}/reports/${report.id}`, {
        method: 'DELETE',
        headers: { 'x-csrf-token': csrf },
        body: JSON.stringify({})
      });
      setToast('Report deleted.');
      await loadWorkspaceData(activeWorkspace);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'report_delete_failed');
    } finally {
      setBusy(false);
    }
  }

  async function manualYouTubeSync(connectionId?: string) {
    if (!activeWorkspace) return;
    setBusy(true);
    setMessage('');
    try {
      const result = await api<{ status?: string; error?: { message?: string; category?: string } }>(
        `/api/workspaces/${activeWorkspace.id}/providers/youtube/sync-runs`,
        {
          method: 'POST',
          headers: { 'x-csrf-token': csrf },
          body: JSON.stringify({ connection_id: connectionId || null })
        }
      );
      await loadWorkspaceData(activeWorkspace);
      if (result.status === 'failed' || result.status === 'disabled') {
        setMessage(result.error?.message || result.error?.category || 'youtube_sync_failed');
      } else if (result.status === 'partial') {
        setToast('YouTube sync completed with partial data.');
      } else if (result.status === 'queued') {
        setToast('YouTube sync scheduled.');
      } else {
        setToast('YouTube sync completed.');
      }
    } catch (error) {
      const code = error instanceof Error ? error.message : 'youtube_sync_failed';
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
          body: JSON.stringify({ return_path: `/?workspace=${activeWorkspace.id}&view=connections` })
        }
      );
      window.location.href = result.authorization_url;
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'connection_failed');
      setBusy(false);
    }
  }

  async function startYouTubeConnection(connectionId?: string) {
    if (!activeWorkspace) return;
    setBusy(true);
    setMessage('');
    try {
      const result = await api<{ authorization_url: string }>(
        `/api/workspaces/${activeWorkspace.id}/connections/youtube/start`,
        {
          method: 'POST',
          headers: { 'x-csrf-token': csrf },
          body: JSON.stringify({
            return_path: `/?workspace=${activeWorkspace.id}&view=connections&provider=youtube`,
            connection_id: connectionId || null
          })
        }
      );
      window.location.href = result.authorization_url;
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'youtube_connection_failed');
      setBusy(false);
    }
  }

  async function selectYouTubeResource(resourceId: string) {
    if (!activeWorkspace) return;
    setBusy(true);
    setMessage('');
    try {
      await api(`/api/workspaces/${activeWorkspace.id}/connections/youtube/select`, {
        method: 'POST',
        headers: { 'x-csrf-token': csrf },
        body: JSON.stringify({ resource_id: resourceId })
      });
      setToast('YouTube channel connected. Its first sync is queued.');
      await loadWorkspaceData(activeWorkspace);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'youtube_channel_selection_failed');
    } finally {
      setBusy(false);
    }
  }

  async function startGoogleAnalyticsConnection(connectionId?: string) {
    if (!activeWorkspace) return;
    setBusy(true);
    setMessage('');
    try {
      const result = await api<{ authorization_url: string }>(
        `/api/workspaces/${activeWorkspace.id}/connections/google-analytics/start`,
        {
          method: 'POST',
          headers: { 'x-csrf-token': csrf },
          body: JSON.stringify({
            return_path: `/?workspace=${activeWorkspace.id}&view=connections&provider=google_analytics_4`,
            connection_id: connectionId || null
          })
        }
      );
      window.location.href = result.authorization_url;
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'ga4_connection_failed');
      setBusy(false);
    }
  }

  async function selectGoogleAnalyticsResource(resourceId: string) {
    if (!activeWorkspace) return;
    setBusy(true);
    setMessage('');
    try {
      await api(`/api/workspaces/${activeWorkspace.id}/connections/google-analytics/select`, {
        method: 'POST',
        headers: { 'x-csrf-token': csrf },
        body: JSON.stringify({ resource_id: resourceId })
      });
      setToast('GA4 property connected. Its first read-only sync is queued.');
      await loadWorkspaceData(activeWorkspace);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'ga4_property_selection_failed');
    } finally {
      setBusy(false);
    }
  }

  async function manualGoogleAnalyticsSync(connectionId?: string) {
    if (!activeWorkspace) return;
    setBusy(true);
    setMessage('');
    try {
      const result = await api<{ status?: string; error?: { message?: string; category?: string } }>(
        `/api/workspaces/${activeWorkspace.id}/providers/google_analytics_4/sync-runs`,
        {
          method: 'POST',
          headers: { 'x-csrf-token': csrf },
          body: JSON.stringify({ connection_id: connectionId || null })
        }
      );
      await loadWorkspaceData(activeWorkspace);
      if (result.status === 'failed' || result.status === 'disabled') {
        setMessage(result.error?.message || result.error?.category || 'ga4_sync_failed');
      } else {
        setToast(result.status === 'queued' ? 'Website Analytics sync scheduled.' : 'Website Analytics sync updated.');
      }
    } catch (error) {
      const code = error instanceof Error ? error.message : 'ga4_sync_failed';
      setMessage(code === 'manual_sync_cooldown' ? 'Manual sync is cooling down. Try again later.' : code);
    } finally {
      setBusy(false);
    }
  }

  async function startMetaConnection(provider: 'facebook' | 'instagram', connectionId?: string) {
    if (!activeWorkspace) return;
    setBusy(true);
    setMessage('');
    const providerId = provider === 'facebook' ? 'facebook_pages' : 'instagram';
    try {
      const result = await api<{ authorization_url: string }>(
        `/api/workspaces/${activeWorkspace.id}/connections/${provider}/start`,
        {
          method: 'POST',
          headers: { 'x-csrf-token': csrf },
          body: JSON.stringify({
            return_path: `/?workspace=${activeWorkspace.id}&view=connections&provider=${providerId}`,
            connection_id: connectionId || null
          })
        }
      );
      window.location.href = result.authorization_url;
    } catch (error) {
      setMessage(error instanceof Error ? error.message : `${providerId}_connection_failed`);
      setBusy(false);
    }
  }

  async function selectMetaResource(provider: 'facebook' | 'instagram', resourceId: string) {
    if (!activeWorkspace) return;
    setBusy(true);
    setMessage('');
    try {
      await api(`/api/workspaces/${activeWorkspace.id}/connections/${provider}/select`, {
        method: 'POST',
        headers: { 'x-csrf-token': csrf },
        body: JSON.stringify({ resource_id: resourceId })
      });
      setToast(
        `${provider === 'facebook' ? 'Facebook Page' : 'Instagram account'} connected. Its first sync is queued.`
      );
      await loadWorkspaceData(activeWorkspace);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : `${provider}_resource_selection_failed`);
    } finally {
      setBusy(false);
    }
  }

  async function manualMetaSync(provider: 'facebook_pages' | 'instagram', connectionId?: string) {
    if (!activeWorkspace) return;
    setBusy(true);
    setMessage('');
    try {
      const result = await api<{ status?: string }>(
        `/api/workspaces/${activeWorkspace.id}/providers/${provider}/sync-runs`,
        {
          method: 'POST',
          headers: { 'x-csrf-token': csrf },
          body: JSON.stringify({ connection_id: connectionId || null })
        }
      );
      setToast(result.status === 'queued' ? 'Meta sync scheduled.' : 'Meta sync updated.');
      await loadWorkspaceData(activeWorkspace);
    } catch (error) {
      const code = error instanceof Error ? error.message : `${provider}_sync_failed`;
      setMessage(code === 'manual_sync_cooldown' ? 'Manual sync is cooling down. Try again later.' : code);
    } finally {
      setBusy(false);
    }
  }

  async function disconnectConnection() {
    if (!activeWorkspace || !disconnectTarget) return;
    setBusy(true);
    setMessage('');
    try {
      const result = await api<{
        provider_revoke?: { attempted?: boolean; success?: boolean; outcome_category?: string };
        provider_grant_preserved?: boolean;
      }>(`/api/workspaces/${activeWorkspace.id}/connections/${disconnectTarget.provider}`, {
        method: 'DELETE',
        headers: { 'x-csrf-token': csrf },
        body: JSON.stringify({ connection_id: disconnectTarget.connectionId || null })
      });
      if (disconnectTarget.provider === 'youtube') {
        setToast(
          result.provider_revoke?.success
            ? 'Google access revoked and locally stored YouTube data deleted.'
            : 'Locally stored YouTube data deleted. Google revocation did not complete; review Google Account connections.'
        );
      } else if (disconnectTarget.provider === 'google-analytics') {
        setToast(
          result.provider_revoke?.success
            ? 'Google Analytics access revoked and locally stored property data deleted.'
            : 'Locally stored GA4 data deleted. Google revocation did not complete; review Google Account connections.'
        );
      } else if (disconnectTarget.provider === 'facebook' || disconnectTarget.provider === 'instagram') {
        setToast(
          result.provider_grant_preserved
            ? 'Locally stored provider data deleted. Meta access remains active for another selected Meta resource.'
            : result.provider_revoke?.success
              ? 'Meta access revoked and locally stored provider data deleted.'
              : 'Locally stored provider data deleted. Meta revocation did not complete; review Facebook app connections.'
        );
      } else {
        setToast('TikTok connection disabled locally. Provider revocation was attempted first.');
      }
      setDisconnectTarget(null);
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
      setToast('Invitation sent.');
      await loadWorkspaceData(activeWorkspace);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'invite_failed');
    } finally {
      setBusy(false);
    }
  }

  async function resendMemberInvitation(invitation: Invitation) {
    if (!activeWorkspace) return;
    setBusy(true);
    setMessage('');
    try {
      await api(`/api/workspaces/${activeWorkspace.id}/invitations/${invitation.id}/resend`, {
        method: 'POST',
        headers: { 'x-csrf-token': csrf },
        body: JSON.stringify({})
      });
      setToast(`A new invitation was sent to ${invitation.email}.`);
      await loadWorkspaceData(activeWorkspace);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'invitation_resend_failed');
    } finally {
      setBusy(false);
    }
  }

  async function revokeMemberInvitation(invitation: Invitation) {
    if (!activeWorkspace) return;
    setBusy(true);
    setMessage('');
    try {
      await api(`/api/workspaces/${activeWorkspace.id}/invitations/${invitation.id}`, {
        method: 'DELETE',
        headers: { 'x-csrf-token': csrf },
        body: JSON.stringify({})
      });
      setToast(`Invitation for ${invitation.email} revoked.`);
      await loadWorkspaceData(activeWorkspace);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'invitation_revoke_failed');
    } finally {
      setBusy(false);
    }
  }

  async function acceptInvitation() {
    if (!invitationToken) return;
    setBusy(true);
    setMessage('');
    try {
      const result = await api<{ workspace: { id: string; name: string } }>('/api/invitations/accept', {
        method: 'POST',
        headers: { 'x-csrf-token': csrf },
        body: JSON.stringify({ token: invitationToken })
      });
      setInvitationToken('');
      await loadWorkspaces();
      setActiveWorkspaceId(result.workspace.id);
      setToast(`You joined ${result.workspace.name}.`);
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'invitation_accept_failed');
    } finally {
      setBusy(false);
    }
  }

  async function saveAccountProfile(displayName: string) {
    setBusy(true);
    setMessage('');
    try {
      const result = await api<{ profile: User }>('/api/account/profile', {
        method: 'PATCH',
        headers: { 'x-csrf-token': csrf },
        body: JSON.stringify({ display_name: displayName })
      });
      setUser(result.profile);
      await loadAccountData();
      setToast('Profile updated.');
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'profile_update_failed');
    } finally {
      setBusy(false);
    }
  }

  async function revokeAccountSessionById(session: AccountSession) {
    setBusy(true);
    setMessage('');
    try {
      const result = await api<{ signed_out: boolean }>(`/api/account/sessions/${session.id}`, {
        method: 'DELETE',
        headers: { 'x-csrf-token': csrf },
        body: JSON.stringify({})
      });
      if (result.signed_out) {
        await signOut();
        return;
      }
      await loadAccountData();
      setToast('Session signed out.');
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'session_revoke_failed');
    } finally {
      setBusy(false);
    }
  }

  async function revokeOtherSessions() {
    setBusy(true);
    setMessage('');
    try {
      const result = await api<{ revoked: number }>('/api/account/sessions/revoke-others', {
        method: 'POST',
        headers: { 'x-csrf-token': csrf },
        body: JSON.stringify({})
      });
      await loadAccountData();
      setToast(
        result.revoked
          ? `${result.revoked} other session${result.revoked === 1 ? '' : 's'} signed out.`
          : 'No other active sessions.'
      );
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'session_revoke_failed');
    } finally {
      setBusy(false);
    }
  }

  async function revokeAllSessions() {
    setBusy(true);
    setMessage('');
    try {
      await api('/api/account/sessions/revoke-all', {
        method: 'POST',
        headers: { 'x-csrf-token': csrf },
        body: JSON.stringify({})
      });
    } catch {
      // Clear local state even if this session was already revoked.
    } finally {
      setUser(null);
      setCsrf('');
      setWorkspaces([]);
      setActiveWorkspaceId('');
      setAccountData(null);
      setBusy(false);
    }
  }

  async function requestAccountDeletionFromUi(confirmation: string) {
    setBusy(true);
    setMessage('');
    try {
      const result = await api<{ existing?: boolean }>('/api/account/deletion-requests', {
        method: 'POST',
        headers: { 'x-csrf-token': csrf },
        body: JSON.stringify({ confirmation })
      });
      await loadAccountData();
      setToast(
        result.existing
          ? 'Your account deletion request is already being reviewed.'
          : 'Account deletion request submitted.'
      );
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'account_deletion_request_failed');
    } finally {
      setBusy(false);
    }
  }

  async function requestWorkspaceDeletionFromUi(workspace: Workspace, confirmation: string) {
    setBusy(true);
    setMessage('');
    try {
      const result = await api<{ existing?: boolean }>(`/api/workspaces/${workspace.id}/deletion-requests`, {
        method: 'POST',
        headers: { 'x-csrf-token': csrf },
        body: JSON.stringify({ confirmation })
      });
      await loadAccountData();
      setToast(
        result.existing
          ? 'This workspace deletion request is already being reviewed.'
          : 'Workspace deletion request submitted.'
      );
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'workspace_deletion_request_failed');
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
      <div className="public-page">
        <header className="public-header">
          <a className="public-brand" href="/" aria-label="Social Insights Studio home">
            <img src="/logo.png" alt="Social Insights Studio logo" />
            <strong>Social Insights Studio</strong>
          </a>
          <nav className="public-nav" aria-label="Public information">
            <a href="/privacy">Privacy</a>
            <a href="/terms">Terms</a>
            <a href="/support">Support</a>
          </nav>
        </header>

        <main className="public-shell">
          <section className="product-panel" aria-labelledby="product-title">
            <div>
              <p className="eyebrow">Multiplatform analytics workspace</p>
              <h1 id="product-title">Social Insights Studio</h1>
              <p className="lede">
                Bring social and website performance into one private workspace. Follow audience growth, content
                results, and meaningful changes over time without jumping between analytics tools.
              </p>
            </div>

            <section className="public-section" aria-labelledby="platforms-title">
              <h2 id="platforms-title">Supported analytics sources</h2>
              <p>
                Social Insights Studio is built for TikTok accounts, Facebook Pages, Instagram professional accounts,
                YouTube channels, and Google Analytics 4 properties. Available connections depend on the access enabled
                for your workspace.
              </p>
              <div className="platform-status" aria-label="Provider availability">
                <span>
                  <CheckCircle2 size={16} aria-hidden /> Scheduled updates
                </span>
                <span>
                  <CheckCircle2 size={16} aria-hidden /> Range comparisons
                </span>
                <span>
                  <CheckCircle2 size={16} aria-hidden /> CSV exports
                </span>
              </div>
            </section>

            <section className="public-disclosure" aria-labelledby="data-use-title">
              <h2 id="data-use-title">How provider data is used</h2>
              <p>
                Authorized account, content, and analytics data is used only to operate requested connections, workspace
                dashboards, synchronization, and reports. Connections request analytics access only. You can disconnect
                a source or request deletion of stored data at any time.
              </p>
            </section>
          </section>

          <section className="auth-panel" aria-labelledby="signin-title">
            <div className="auth-heading">
              <p className="eyebrow">Workspace access</p>
              <h2 id="signin-title">Sign in</h2>
              <p>Use the email address associated with your Social Insights Studio workspace.</p>
            </div>
            {invitationToken && (
              <p className="notice" role="status">
                Sign in with the email address that received the invitation. You can review it before joining.
              </p>
            )}
            <form onSubmit={requestLink} className="stack">
              <label>
                Email address
                <input
                  value={email}
                  onChange={(event) => setEmail(event.target.value)}
                  type="email"
                  autoComplete="email"
                  required
                />
              </label>
              <button type="submit" disabled={busy}>
                {busy ? <Loader2 className="spin" size={18} aria-hidden /> : <Mail size={18} aria-hidden />}
                Send sign-in code
              </button>
            </form>
            <form onSubmit={verifyLink} className="stack">
              <label>
                Verification code
                <input value={token} onChange={(event) => setToken(event.target.value)} autoComplete="one-time-code" />
              </label>
              <button type="submit" disabled={busy || !token}>
                <KeyRound size={18} aria-hidden />
                Sign in
              </button>
            </form>
            {message && (
              <p className="notice" role="status">
                {message}
              </p>
            )}
            <p className="auth-help">
              Need help? <a href="/support">Contact support</a>.
            </p>
          </section>
        </main>

        <footer className="public-footer">
          <span>&copy; 2026 Social Insights Studio</span>
          <nav aria-label="Legal and support">
            <a href="/privacy">Privacy Policy</a>
            <a href="/terms">Terms of Service</a>
            <a href="/support">Support</a>
            <a href="/data-deletion">Data Deletion</a>
            <a href="/status">Status</a>
          </nav>
        </footer>
      </div>
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
          onChange={(workspaceId) => {
            setActiveWorkspaceId(workspaceId);
            setSourceConnectionId('');
          }}
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
          connection={view === 'sources' ? activeSourceConnection : null}
          busy={busy}
          accountOpen={accountOpen}
          onAccountToggle={() => setAccountOpen((open) => !open)}
          onAccountNavigate={() => {
            setView('account');
            setContentDetailId('');
            setAccountOpen(false);
          }}
          onManualSync={
            view === 'sources' && overviewProvider === 'youtube'
              ? () => manualYouTubeSync(youtubeDashboard?.connection.id)
              : view === 'sources' && overviewProvider === 'facebook_pages'
                ? () => manualMetaSync('facebook_pages', facebookDashboard?.connection.id)
                : view === 'sources' && overviewProvider === 'instagram'
                  ? () => manualMetaSync('instagram', instagramDashboard?.connection.id)
                  : view === 'sources' && overviewProvider === 'google_analytics_4'
                    ? () => manualGoogleAnalyticsSync(googleAnalyticsDashboard?.connection.id)
                    : manualSync
          }
          onSignOut={signOut}
        />

        <div aria-live="polite" className="live-region">
          {toast || (message ? friendlyError(message) : '')}
        </div>
        {toast && (
          <p className="notice success" role="status">
            {toast}
          </p>
        )}
        {message && (
          <p className="notice error" role="alert">
            {friendlyError(message)}
          </p>
        )}
        {invitationToken && (
          <section className="invitation-banner" aria-labelledby="pending-invitation-title">
            <div>
              <strong id="pending-invitation-title">Workspace invitation</strong>
              <p>Accept only if you recognize the invitation and signed in with the invited email address.</p>
            </div>
            <div className="button-row">
              <button type="button" onClick={acceptInvitation} disabled={busy}>
                Accept invitation
              </button>
              <button type="button" className="ghost-button" onClick={() => setInvitationToken('')} disabled={busy}>
                Dismiss
              </button>
            </div>
          </section>
        )}
        {(view === 'overview' ? crossPlatformDashboard?.demo_data : dashboard?.demo_data) && (
          <p className="notice">
            Sample data is shown for this local demonstration. It does not come from a connected provider account.
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
            {(view === 'overview' || view === 'sources' || view === 'content') && <StateBanner state={state} />}
            {view === 'overview' && (
              <CrossPlatformOverview
                dashboard={crossPlatformDashboard}
                range={range}
                customFrom={customFrom}
                customTo={customTo}
                compare={compare}
                rangeInvalid={rangeInvalid}
                busy={busy}
                canSync={roleCanSync(activeWorkspace.role)}
                onRangeChange={setRange}
                onCustomFromChange={setCustomFrom}
                onCustomToChange={setCustomTo}
                onCompareChange={setCompare}
                onOpenSource={(provider, connectionId) => {
                  setOverviewProvider(provider);
                  setSourceConnectionId(connectionId || '');
                  setView('sources');
                }}
                onSyncSource={(source) => {
                  const connectionId = source.resource?.connection_id || undefined;
                  if (source.provider === 'youtube') return manualYouTubeSync(connectionId);
                  if (source.provider === 'facebook_pages' || source.provider === 'instagram') {
                    return manualMetaSync(source.provider, connectionId);
                  }
                  if (source.provider === 'google_analytics_4') return manualGoogleAnalyticsSync(connectionId);
                  return manualSync();
                }}
              />
            )}
            {view === 'sources' && (
              <ProviderOverview
                dashboard={dashboard}
                youtubeDashboard={youtubeDashboard}
                facebookDashboard={facebookDashboard}
                instagramDashboard={instagramDashboard}
                googleAnalyticsDashboard={googleAnalyticsDashboard}
                providers={providerCatalog}
                provider={overviewProvider}
                connectionId={sourceConnectionId}
                range={range}
                customFrom={customFrom}
                customTo={customTo}
                compare={compare}
                trendMetric={trendMetric}
                topSort={topSort}
                rangeInvalid={rangeInvalid}
                busy={busy}
                canSync={roleCanSync(activeWorkspace.role)}
                onProviderChange={(provider) => {
                  setOverviewProvider(provider);
                  setSourceConnectionId('');
                }}
                onConnectionChange={setSourceConnectionId}
                onRangeChange={(next) => setRange(next)}
                onCustomFromChange={setCustomFrom}
                onCustomToChange={setCustomTo}
                onCompareChange={setCompare}
                onTrendMetricChange={setTrendMetric}
                onTopSortChange={setTopSort}
                onYouTubeSync={() => manualYouTubeSync(youtubeDashboard?.connection.id)}
                onMetaSync={(provider) =>
                  manualMetaSync(
                    provider,
                    provider === 'facebook_pages' ? facebookDashboard?.connection.id : instagramDashboard?.connection.id
                  )
                }
                onGoogleAnalyticsSync={() => manualGoogleAnalyticsSync(googleAnalyticsDashboard?.connection.id)}
              />
            )}
            {view === 'content' && contentDetailId ? (
              <ContentDetailView detail={contentDetail} onBack={closeContentDetail} />
            ) : view === 'content' ? (
              <Content
                workspace={activeWorkspace}
                content={content}
                providers={providerCatalog}
                provider={contentProvider}
                connectionId={contentConnectionId}
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
                onProviderChange={(provider) => {
                  setContentProvider(provider);
                  setContentConnectionId('');
                  setContentPage(1);
                }}
                onConnectionChange={(connectionId) => {
                  setContentConnectionId(connectionId);
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
            {view === 'reports' && (
              <Reports
                role={activeWorkspace.role}
                configuration={reportConfiguration}
                providers={providerCatalog}
                reports={reports}
                preview={reportPreview}
                busy={busy}
                onPreview={previewPdfReport}
                onGenerate={generatePdfReport}
                onDownload={downloadPdfReport}
                onDelete={deletePdfReport}
              />
            )}
            {view === 'connections' && (
              <Connections
                role={activeWorkspace.role}
                dashboard={dashboard}
                providers={providerCatalog}
                busy={busy}
                disconnectTarget={disconnectTarget}
                onTikTokConnect={startConnection}
                onYouTubeConnect={startYouTubeConnection}
                onYouTubeSelect={selectYouTubeResource}
                onYouTubeSync={manualYouTubeSync}
                onGoogleAnalyticsConnect={startGoogleAnalyticsConnection}
                onGoogleAnalyticsSelect={selectGoogleAnalyticsResource}
                onGoogleAnalyticsSync={manualGoogleAnalyticsSync}
                onMetaConnect={startMetaConnection}
                onMetaSelect={selectMetaResource}
                onMetaSync={manualMetaSync}
                onDisconnectRequest={setDisconnectTarget}
                onDisconnectCancel={() => setDisconnectTarget(null)}
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
                onResendInvitation={resendMemberInvitation}
                onRevokeInvitation={revokeMemberInvitation}
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
            {view === 'account' && (
              <Account
                user={user}
                account={accountData}
                workspaces={workspaces}
                busy={busy}
                onSaveProfile={saveAccountProfile}
                onRevokeSession={revokeAccountSessionById}
                onRevokeOthers={revokeOtherSessions}
                onRevokeAll={revokeAllSessions}
                onRequestAccountDeletion={requestAccountDeletionFromUi}
                onRequestWorkspaceDeletion={requestWorkspaceDeletionFromUi}
                onSignOut={signOut}
              />
            )}
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
  connection,
  busy,
  accountOpen,
  onAccountToggle,
  onAccountNavigate,
  onManualSync,
  onSignOut
}: {
  workspace?: Workspace;
  connection: ProviderConnection | DashboardData['connection'] | null;
  busy: boolean;
  accountOpen: boolean;
  onAccountToggle: () => void;
  onAccountNavigate: () => void;
  onManualSync: () => void;
  onSignOut: () => void;
}) {
  const canSync = workspace && roleCanSync(workspace.role) && connection?.status === 'active';
  return (
    <header className="topbar">
      <div>
        <p className="eyebrow">{workspace ? workspace.role : 'No workspace'}</p>
        <h1>{workspace ? workspace.name : 'Create your first workspace'}</h1>
      </div>
      {connection ? (
        <div className="header-status" aria-label="Selected source status">
          <StatusBadge status={connection.status} />
          <span>Last sync: {formatDate(connection.last_successful_sync_at)}</span>
          <span>Next: {formatDate(connection.next_sync_at)}</span>
        </div>
      ) : (
        <div />
      )}
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
              <button type="button" role="menuitem" onClick={onAccountNavigate}>
                <Settings size={16} aria-hidden />
                Account settings
              </button>
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
      text: 'Connect a supported provider or use labeled local demo data to populate this workspace.'
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

function CrossProviderIcon({ provider }: { provider: OverviewProvider }) {
  if (provider === 'youtube') return <Youtube size={20} aria-hidden />;
  if (provider === 'facebook_pages') return <Facebook size={20} aria-hidden />;
  if (provider === 'instagram') return <Instagram size={20} aria-hidden />;
  if (provider === 'google_analytics_4') return <BarChart3 size={20} aria-hidden />;
  return <Video size={20} aria-hidden />;
}

function formatCrossPlatformValue(metric: Pick<CrossPlatformMetric, 'unit' | 'value'>) {
  if (metric.value === null) return 'N/A';
  if (metric.unit === 'ratio') return `${(metric.value * 100).toFixed(1)}%`;
  if (metric.unit === 'minutes') return formatMinutes(metric.value);
  if (metric.unit === 'seconds') return formatSeconds(metric.value);
  return formatNumber(metric.value);
}

function CrossPlatformMetricCard({ metric, compare }: { metric: CrossPlatformMetric; compare: boolean }) {
  const direction = !metric.available
    ? 'unavailable'
    : metric.delta === null
      ? 'neutral'
      : metric.delta > 0
        ? 'positive'
        : metric.delta < 0
          ? 'negative'
          : 'neutral';
  const comparison = !metric.available
    ? (metric.availability_reason || 'Not reported by this provider').replaceAll('_', ' ')
    : !compare
      ? 'Comparison hidden'
      : metric.delta === null
        ? 'Previous period unavailable'
        : metric.unit === 'ratio'
          ? `${metric.delta >= 0 ? '+' : ''}${(metric.delta * 100).toFixed(1)} percentage points`
          : metric.percent_change === null
            ? `${metric.delta >= 0 ? '+' : ''}${formatNumber(metric.delta)}`
            : `${metric.delta >= 0 ? '+' : ''}${metric.percent_change.toFixed(1)}%`;
  return (
    <article
      className={`metric-card cross-metric ${direction}`}
      title={metric.definition || metric.semantics || undefined}
    >
      <span>{metric.label}</span>
      <strong>{formatCrossPlatformValue(metric)}</strong>
      <small>{comparison}</small>
    </article>
  );
}

function CrossPlatformOverview({
  dashboard,
  range,
  customFrom,
  customTo,
  compare,
  rangeInvalid,
  busy,
  canSync,
  onRangeChange,
  onCustomFromChange,
  onCustomToChange,
  onCompareChange,
  onOpenSource,
  onSyncSource
}: {
  dashboard: CrossPlatformDashboardData | null;
  range: RangeKey;
  customFrom: string;
  customTo: string;
  compare: boolean;
  rangeInvalid: string;
  busy: boolean;
  canSync: boolean;
  onRangeChange: (range: RangeKey) => void;
  onCustomFromChange: (value: string) => void;
  onCustomToChange: (value: string) => void;
  onCompareChange: (value: boolean) => void;
  onOpenSource: (provider: OverviewProvider, connectionId: string | null) => void;
  onSyncSource: (source: CrossPlatformSource) => void;
}) {
  const trendColors = ['var(--chart-a)', 'var(--chart-b)', 'var(--chart-c)'];
  return (
    <>
      <section className="source-switcher" aria-labelledby="cross-platform-title">
        <div>
          <p className="eyebrow">Cross-platform overview</p>
          <h2 id="cross-platform-title">Source health and performance</h2>
        </div>
        {dashboard && (
          <p className="muted cross-range-summary">
            {formatDate(dashboard.range.from, { dateStyle: 'medium' })} –{' '}
            {formatDate(dashboard.range.to, { dateStyle: 'medium' })}
          </p>
        )}
      </section>

      <section className="control-bar" aria-labelledby="cross-date-controls-title">
        <h2 id="cross-date-controls-title" className="sr-only">
          Cross-platform date and comparison controls
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

      {dashboard ? (
        <>
          <section className="cross-health-grid" aria-label="Cross-platform source health summary">
            <article className="panel health-summary-card">
              <span>Active resources</span>
              <strong>{formatNumber(dashboard.summary.connected_resources)}</strong>
              <small>Connected resources, not an analytics total</small>
            </article>
            <article className="panel health-summary-card">
              <span>Resources with data</span>
              <strong>{formatNumber(dashboard.summary.resources_with_data)}</strong>
              <small>Stored observations in this workspace</small>
            </article>
            <article className="panel health-summary-card">
              <span>Needs attention</span>
              <strong>{formatNumber(dashboard.summary.attention_count)}</strong>
              <small>Freshness, setup, or availability notices</small>
            </article>
          </section>

          {dashboard.alerts.length > 0 && (
            <section className="panel cross-alerts" aria-labelledby="cross-alerts-title">
              <div className="panel-title">
                <ShieldAlert size={20} aria-hidden />
                <div>
                  <h2 id="cross-alerts-title">Source alerts</h2>
                  <p>Actionable connection and data-quality states.</p>
                </div>
              </div>
              <div className="cross-alert-list">
                {dashboard.alerts.map((alert) => (
                  <div key={`${alert.source_id}:${alert.code}`} className={`cross-alert ${alert.severity}`}>
                    <StatusBadge status={alert.severity} />
                    <span>{alert.message}</span>
                  </div>
                ))}
              </div>
            </section>
          )}

          <section className="cross-source-grid" aria-label="Provider summaries">
            {dashboard.sources.map((source) => {
              const trendPoints = source.trend.points.map((point) => ({
                date: point.date,
                label: formatDate(point.date, { month: 'short', day: 'numeric' }),
                ...point.values
              }));
              return (
                <article className="panel cross-source-card" key={source.id}>
                  <div className="cross-source-heading">
                    <div className="cross-source-identity">
                      <span className="provider-mark">
                        <CrossProviderIcon provider={source.provider} />
                      </span>
                      <div>
                        <p className="eyebrow">{source.provider_name}</p>
                        <h3>{source.resource?.display_name || 'Not connected'}</h3>
                        {source.resource?.account_name && <small>{source.resource.account_name}</small>}
                      </div>
                    </div>
                    <StatusBadge status={source.freshness.state} />
                  </div>

                  <div className="freshness-row">
                    <span>Last sync {formatDate(source.freshness.last_successful_sync_at)}</span>
                    <span>Data through {formatDate(source.freshness.data_through_date, { dateStyle: 'medium' })}</span>
                    {source.resource?.timezone && <span>Timezone {source.resource.timezone}</span>}
                  </div>

                  {source.alert && <p className={`source-note ${source.alert.severity}`}>{source.alert.message}</p>}
                  {source.availability.note && <p className="source-note">{source.availability.note}</p>}

                  <div className="cross-metric-grid" aria-label={`${source.provider_name} metrics`}>
                    {source.metrics.map((metric) => (
                      <CrossPlatformMetricCard key={metric.key} metric={metric} compare={compare} />
                    ))}
                  </div>

                  {(source.has_data || source.status === 'active') && (
                    <>
                      <div className="cross-section">
                        <div>
                          <h4>Provider trends</h4>
                          <p>Each series uses its own scale and provider definition.</p>
                        </div>
                        {source.trend.series.length > 0 && trendPoints.length > 1 ? (
                          <div className="cross-trend-grid">
                            {source.trend.series.slice(0, 3).map((series, index) => (
                              <div className="mini-trend" key={series.key}>
                                <span>{series.label}</span>
                                <div role="img" aria-label={`${source.provider_name} ${series.label} trend`}>
                                  <ResponsiveContainer width="100%" height={110}>
                                    <LineChart data={trendPoints} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
                                      <XAxis dataKey="label" hide />
                                      <YAxis domain={['auto', 'auto']} hide />
                                      <Tooltip
                                        formatter={series.unit === 'ratio' ? formatTooltipPercent : formatTooltipNumber}
                                        labelFormatter={(label) => String(label)}
                                      />
                                      <Line
                                        type="monotone"
                                        dataKey={series.key}
                                        name={series.label}
                                        stroke={trendColors[index]}
                                        strokeWidth={2.25}
                                        dot={false}
                                        connectNulls
                                        isAnimationActive={false}
                                      />
                                    </LineChart>
                                  </ResponsiveContainer>
                                </div>
                              </div>
                            ))}
                          </div>
                        ) : (
                          <p className="compact-chart-empty">
                            {trendPoints.length === 1
                              ? 'One stored point is available; a trend needs at least two.'
                              : source.provider === 'instagram'
                                ? 'Instagram account insights are stored as provider-reported period totals.'
                                : 'No trend points are stored for this range.'}
                          </p>
                        )}
                      </div>

                      <div className="cross-section">
                        <div>
                          <h4>{source.provider === 'google_analytics_4' ? 'Top landing pages' : 'Top content'}</h4>
                          <p>
                            {source.provider === 'google_analytics_4'
                              ? 'Website paths remain distinct from social posts.'
                              : 'Provider-reported views.'}
                          </p>
                        </div>
                        {source.top_content.length > 0 ? (
                          <ol className="cross-content-list">
                            {source.top_content.map((item) => (
                              <li key={item.id}>
                                <div>
                                  {item.share_url ? (
                                    <a href={item.share_url} target="_blank" rel="noreferrer">
                                      {item.title} <ExternalLink size={13} aria-hidden />
                                    </a>
                                  ) : (
                                    <span>{item.title}</span>
                                  )}
                                  {item.published_at && (
                                    <small>{formatDate(item.published_at, { dateStyle: 'medium' })}</small>
                                  )}
                                </div>
                                <strong>{formatNumber(item.primary_metric.value)}</strong>
                                <small>{item.primary_metric.label}</small>
                              </li>
                            ))}
                          </ol>
                        ) : (
                          <p className="compact-chart-empty">No ranked items are stored for this range.</p>
                        )}
                      </div>
                    </>
                  )}

                  <div className="button-row start cross-source-actions">
                    <button
                      type="button"
                      className="ghost-button"
                      onClick={() => onOpenSource(source.provider, source.resource?.connection_id || null)}
                    >
                      Open source
                    </button>
                    {canSync && source.status === 'active' && (
                      <button type="button" disabled={busy} onClick={() => onSyncSource(source)}>
                        {busy ? (
                          <Loader2 className="spin" size={17} aria-hidden />
                        ) : (
                          <RefreshCw size={17} aria-hidden />
                        )}
                        Sync
                      </button>
                    )}
                  </div>
                </article>
              );
            })}
          </section>

          <details className="panel methodology-panel">
            <summary>Metric comparison methodology</summary>
            <ul>
              {dashboard.methodology.map((item) => (
                <li key={item}>{item}</li>
              ))}
            </ul>
          </details>
        </>
      ) : (
        <section className="panel chart-empty">Source summaries will appear after workspace data loads.</section>
      )}
    </>
  );
}

function ProviderOverview({
  dashboard,
  youtubeDashboard,
  facebookDashboard,
  instagramDashboard,
  googleAnalyticsDashboard,
  providers,
  provider,
  connectionId,
  range,
  customFrom,
  customTo,
  compare,
  trendMetric,
  topSort,
  rangeInvalid,
  busy,
  canSync,
  onProviderChange,
  onConnectionChange,
  onRangeChange,
  onCustomFromChange,
  onCustomToChange,
  onCompareChange,
  onTrendMetricChange,
  onTopSortChange,
  onYouTubeSync,
  onMetaSync,
  onGoogleAnalyticsSync
}: {
  dashboard: DashboardData | null;
  youtubeDashboard: YouTubeDashboardData | null;
  facebookDashboard: MetaDashboardData | null;
  instagramDashboard: MetaDashboardData | null;
  googleAnalyticsDashboard: GoogleAnalyticsDashboardData | null;
  providers: ProviderCatalogItem[];
  provider: OverviewProvider;
  connectionId: string;
  range: RangeKey;
  customFrom: string;
  customTo: string;
  compare: boolean;
  trendMetric: string;
  topSort: ContentSort;
  rangeInvalid: string;
  busy: boolean;
  canSync: boolean;
  onProviderChange: (provider: OverviewProvider) => void;
  onConnectionChange: (connectionId: string) => void;
  onRangeChange: (range: RangeKey) => void;
  onCustomFromChange: (value: string) => void;
  onCustomToChange: (value: string) => void;
  onCompareChange: (value: boolean) => void;
  onTrendMetricChange: (value: string) => void;
  onTopSortChange: (value: ContentSort) => void;
  onYouTubeSync: () => void;
  onMetaSync: (provider: 'facebook_pages' | 'instagram') => void;
  onGoogleAnalyticsSync: () => void;
}) {
  const providerCatalog = providers.find((item) => item.id === provider);
  const resourceConnections = (providerCatalog?.connections || []).filter((connection) => Boolean(connection.id));
  const loadedConnectionId =
    provider === 'youtube'
      ? youtubeDashboard?.connection.id
      : provider === 'facebook_pages'
        ? facebookDashboard?.connection.id
        : provider === 'instagram'
          ? instagramDashboard?.connection.id
          : provider === 'google_analytics_4'
            ? googleAnalyticsDashboard?.connection.id
            : undefined;
  const selectedConnectionId = connectionId || loadedConnectionId || resourceConnections[0]?.id || '';
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
      <section className="source-switcher" aria-labelledby="source-title">
        <div>
          <p className="eyebrow">Data source</p>
          <h2 id="source-title">Channel performance</h2>
        </div>
        <div className="segmented" aria-label="Source provider">
          <button
            type="button"
            className={provider === 'tiktok' ? 'active' : ''}
            aria-pressed={provider === 'tiktok'}
            onClick={() => onProviderChange('tiktok')}
          >
            <Video size={17} aria-hidden /> TikTok
          </button>
          <button
            type="button"
            className={provider === 'youtube' ? 'active' : ''}
            aria-pressed={provider === 'youtube'}
            onClick={() => onProviderChange('youtube')}
          >
            <Youtube size={17} aria-hidden /> YouTube
          </button>
          <button
            type="button"
            className={provider === 'facebook_pages' ? 'active' : ''}
            aria-pressed={provider === 'facebook_pages'}
            onClick={() => onProviderChange('facebook_pages')}
          >
            <Facebook size={17} aria-hidden /> Facebook
          </button>
          <button
            type="button"
            className={provider === 'instagram' ? 'active' : ''}
            aria-pressed={provider === 'instagram'}
            onClick={() => onProviderChange('instagram')}
          >
            <Instagram size={17} aria-hidden /> Instagram
          </button>
          <button
            type="button"
            className={provider === 'google_analytics_4' ? 'active' : ''}
            aria-pressed={provider === 'google_analytics_4'}
            onClick={() => onProviderChange('google_analytics_4')}
          >
            <BarChart3 size={17} aria-hidden /> Website
          </button>
        </div>
      </section>
      {provider !== 'tiktok' && resourceConnections.length > 0 && (
        <section className="source-resource-filter" aria-label="Connected resource filter">
          <label>
            Resource
            <select value={selectedConnectionId} onChange={(event) => onConnectionChange(event.target.value)}>
              {resourceConnections.map((connection) => (
                <option key={connection.id} value={connection.id}>
                  {connection.account?.display_name ||
                    connection.account?.account_name ||
                    connection.account?.id ||
                    connection.id}
                </option>
              ))}
            </select>
          </label>
          <span className="muted">
            {resourceConnections.length} connected {resourceConnections.length === 1 ? 'resource' : 'resources'}
          </span>
        </section>
      )}
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

      {provider === 'youtube' ? (
        <YouTubeOverview
          dashboard={youtubeDashboard}
          compare={compare}
          busy={busy}
          canSync={canSync}
          onSync={onYouTubeSync}
        />
      ) : provider === 'google_analytics_4' ? (
        <GoogleAnalyticsOverview
          dashboard={googleAnalyticsDashboard}
          compare={compare}
          busy={busy}
          canSync={canSync}
          onSync={onGoogleAnalyticsSync}
        />
      ) : provider === 'facebook_pages' || provider === 'instagram' ? (
        <MetaOverview
          dashboard={provider === 'facebook_pages' ? facebookDashboard : instagramDashboard}
          compare={compare}
          busy={busy}
          canSync={canSync}
          onSync={() => onMetaSync(provider)}
        />
      ) : (
        <>
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
                  ? 'One data point is available. More points are needed for a trend line.'
                  : 'No audience history is available for the selected range.'}
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
                    <Bar
                      dataKey="value"
                      name={contentSortLabels[topSort]}
                      fill="var(--chart-c)"
                      radius={[0, 4, 4, 0]}
                    />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            ) : (
              <div className="chart-empty">No content matches this range.</div>
            )}
          </section>
        </>
      )}
    </>
  );
}

function formatGoogleAnalyticsValue(metric: Pick<GoogleAnalyticsMetric, 'key' | 'unit' | 'value'>) {
  if (metric.value === null || metric.value === undefined) return 'N/A';
  if (metric.unit === 'seconds') return formatSeconds(metric.value);
  if (metric.unit === 'ratio') {
    if (metric.key === 'ga4.engagement_rate' || metric.key === 'ga4.bounce_rate') {
      return `${(metric.value * 100).toFixed(1)}%`;
    }
    return metric.value.toFixed(2);
  }
  return formatNumber(metric.value);
}

function formatGoogleAnalyticsBreakdownValue(key: string, value: number | null) {
  if (value === null) return 'N/A';
  if (key === 'ga4.engagement_rate' || key === 'ga4.bounce_rate') return `${(value * 100).toFixed(1)}%`;
  if (key === 'ga4.average_session_duration') return formatSeconds(value);
  if (key.endsWith('_per_user')) return value.toFixed(2);
  return formatNumber(value);
}

function googleAnalyticsMetricLabel(key: string) {
  const labels: Record<string, string> = {
    'ga4.active_users': 'Active users',
    'ga4.new_users': 'New users',
    'ga4.sessions': 'Sessions',
    'ga4.screen_page_views': 'Views',
    'ga4.engagement_rate': 'Engagement rate',
    'ga4.bounce_rate': 'Bounce rate',
    'ga4.average_session_duration': 'Avg. session duration',
    'ga4.sessions_per_user': 'Sessions per user',
    'ga4.screen_page_views_per_user': 'Views per user'
  };
  return labels[key] || key.replace('ga4.', '').replaceAll('_', ' ');
}

function GoogleAnalyticsMetricCard({ metric, compare }: { metric: GoogleAnalyticsMetric; compare: boolean }) {
  const direction = !metric.available
    ? 'unavailable'
    : metric.delta === null
      ? 'neutral'
      : metric.delta > 0
        ? 'positive'
        : metric.delta < 0
          ? 'negative'
          : 'neutral';
  return (
    <article className={`metric-card ${direction}`} title={metric.definition}>
      <span>{metric.label}</span>
      <strong>{formatGoogleAnalyticsValue(metric)}</strong>
      <small>
        {!metric.available
          ? (metric.availability_reason || 'Unavailable from GA4').replaceAll('_', ' ')
          : !compare
            ? 'Comparison hidden'
            : metric.delta === null
              ? 'Previous period unavailable'
              : `${metric.delta >= 0 ? '+' : ''}${metric.percent_change === null ? 'Change available; percent N/A' : `${metric.percent_change.toFixed(1)}%`}`}
      </small>
    </article>
  );
}

function GoogleAnalyticsOverview({
  dashboard,
  compare,
  busy,
  canSync,
  onSync
}: {
  dashboard: GoogleAnalyticsDashboardData | null;
  compare: boolean;
  busy: boolean;
  canSync: boolean;
  onSync: () => void;
}) {
  const connected = dashboard?.connection.status === 'active';
  const trend = (dashboard?.trend || []).map((point) => ({
    ...point,
    label: formatDate(point.date, { month: 'short', day: 'numeric' })
  }));
  const availabilityMessage =
    dashboard?.availability.state === 'thresholded'
      ? 'Google privacy thresholding applies to at least one breakdown. Summary metrics remain as reported; withheld rows are not estimated.'
      : dashboard?.availability.state === 'delayed'
        ? `GA4 data is currently available through ${formatDate(dashboard.availability.data_through_date, { dateStyle: 'medium' })}.`
        : dashboard?.availability.state === 'partial'
          ? 'Some GA4 metrics or breakdowns are unavailable for this property and range. Missing values remain N/A.'
          : null;

  return (
    <>
      <section className="panel youtube-channel" aria-labelledby="ga4-property-title">
        <div className="channel-identity">
          <span className="channel-placeholder" aria-hidden>
            <BarChart3 size={24} />
          </span>
          <div>
            <p className="eyebrow">Google Analytics 4 property</p>
            <h2 id="ga4-property-title">{dashboard?.property?.display_name || 'No property connected'}</h2>
            <p className="muted">
              {connected
                ? `Last synced ${formatDate(dashboard?.connection.last_successful_sync_at)}`
                : 'Authorize Google Analytics and explicitly select a property in Connections.'}
            </p>
            {dashboard?.property && (
              <p className="muted">
                {dashboard.property.account_name || dashboard.property.id} · {dashboard.property.timezone}
                {dashboard.property.currency ? ` · ${dashboard.property.currency}` : ''}
              </p>
            )}
          </div>
        </div>
        <div className="button-row">
          <StatusBadge status={dashboard?.connection.status || 'disconnected'} />
          <button type="button" onClick={onSync} disabled={!connected || !canSync || busy}>
            <RefreshCw className={busy ? 'spin' : ''} size={18} aria-hidden /> Sync now
          </button>
        </div>
      </section>

      {availabilityMessage && <p className="notice">{availabilityMessage}</p>}

      <section className="metric-grid youtube-metrics" aria-label="Website Analytics summary metrics">
        {(dashboard?.metrics || []).map((metric) => (
          <GoogleAnalyticsMetricCard key={metric.key} metric={metric} compare={compare} />
        ))}
        {!dashboard?.metrics.length && (
          <article className="metric-card unavailable">
            <span>Website Analytics</span>
            <strong>N/A</strong>
            <small>No stored GA4 report is available</small>
          </article>
        )}
      </section>

      <section className="panel chart-panel" aria-labelledby="ga4-traffic-title">
        <div className="panel-title between">
          <div>
            <h2 id="ga4-traffic-title">Daily traffic</h2>
            <p>Sessions, views, and active users in the property timezone.</p>
          </div>
          <span className="muted">
            Data through {formatDate(dashboard?.availability.data_through_date, { dateStyle: 'medium' })}
          </span>
        </div>
        {trend.length > 0 ? (
          <div className="chart-box" role="img" aria-label="Line chart of daily GA4 sessions, views, and active users">
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={trend} margin={{ top: 12, right: 24, bottom: 12, left: 0 }}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="label" minTickGap={24} />
                <YAxis tickFormatter={formatCompact} />
                <Tooltip formatter={formatTooltipNumber} />
                <Legend />
                <Line
                  type="monotone"
                  dataKey="sessions"
                  name="Sessions"
                  stroke="var(--chart-a)"
                  strokeWidth={2.5}
                  dot={false}
                  connectNulls
                />
                <Line
                  type="monotone"
                  dataKey="screen_page_views"
                  name="Views"
                  stroke="var(--chart-b)"
                  strokeWidth={2.5}
                  dot={false}
                  connectNulls
                />
                <Line
                  type="monotone"
                  dataKey="active_users"
                  name="Active users"
                  stroke="var(--chart-c)"
                  strokeWidth={2.5}
                  dot={false}
                  connectNulls
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        ) : (
          <div className="chart-empty">No daily GA4 rows are stored for this range.</div>
        )}
      </section>

      <section className="panel" aria-labelledby="ga4-breakdowns-title">
        <div className="panel-title">
          <div>
            <h2 id="ga4-breakdowns-title">Traffic breakdowns</h2>
            <p>Top provider-reported rows. “(not set)” and privacy-threshold states are preserved.</p>
          </div>
        </div>
        {dashboard?.breakdowns.length ? (
          <div className="ga4-breakdown-grid">
            {dashboard.breakdowns.map((breakdown) => (
              <section key={breakdown.key} className="ga4-breakdown" aria-labelledby={`${breakdown.key}-title`}>
                <div className="between">
                  <h3 id={`${breakdown.key}-title`}>{breakdown.label}</h3>
                  {breakdown.subject_to_thresholding && <StatusBadge status="thresholded" />}
                </div>
                <div className="table-wrap">
                  <table>
                    <thead>
                      <tr>
                        <th scope="col">Value</th>
                        <th scope="col">Metrics</th>
                      </tr>
                    </thead>
                    <tbody>
                      {breakdown.rows.slice(0, 10).map((row, index) => (
                        <tr key={`${breakdown.key}-${Object.values(row.dimensions).join('-')}-${index}`}>
                          <td data-label="Value">{Object.values(row.dimensions).join(' · ') || '(not set)'}</td>
                          <td data-label="Metrics">
                            {Object.entries(row.metrics)
                              .map(
                                ([key, value]) =>
                                  `${googleAnalyticsMetricLabel(key)}: ${formatGoogleAnalyticsBreakdownValue(key, value)}`
                              )
                              .join(' · ')}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </section>
            ))}
          </div>
        ) : (
          <div className="table-empty">No compatible GA4 breakdown rows are stored for this range.</div>
        )}
      </section>
    </>
  );
}

function MetaOverview({
  dashboard,
  compare,
  busy,
  canSync,
  onSync
}: {
  dashboard: MetaDashboardData | null;
  compare: boolean;
  busy: boolean;
  canSync: boolean;
  onSync: () => void;
}) {
  const provider = dashboard?.provider || 'facebook_pages';
  const connected = dashboard?.connection.status === 'active';
  const isInstagram = provider === 'instagram';
  const PrimaryIcon = isInstagram ? Instagram : Facebook;
  const trend = (dashboard?.trend || []).map((point) => ({
    ...point,
    label: formatDate(point.date, { month: 'short', day: 'numeric' })
  }));
  const primaryKey = isInstagram ? 'views' : 'page_media_view';
  const secondaryKey = isInstagram ? 'reach' : 'page_post_engagements';

  return (
    <>
      <section className="panel youtube-channel" aria-labelledby="meta-account-title">
        <div className="channel-identity">
          {dashboard?.account?.thumbnail_url ? (
            <img src={dashboard.account.thumbnail_url} alt="" referrerPolicy="no-referrer" />
          ) : (
            <span className="channel-placeholder" aria-hidden>
              <PrimaryIcon size={24} />
            </span>
          )}
          <div>
            <p className="eyebrow">{isInstagram ? 'Instagram professional account' : 'Facebook Page'}</p>
            <h2 id="meta-account-title">{dashboard?.account?.display_name || 'No resource connected'}</h2>
            <p className="muted">
              {connected
                ? `Last synced ${formatDate(dashboard?.connection.last_successful_sync_at)}`
                : 'Authorize Meta and explicitly select a resource in Connections.'}
            </p>
            {dashboard?.account?.source_page_name && (
              <p className="muted">Linked Facebook Page: {dashboard.account.source_page_name}</p>
            )}
          </div>
        </div>
        <div className="button-row">
          <StatusBadge status={dashboard?.connection.status || 'disconnected'} />
          <button type="button" onClick={onSync} disabled={!connected || !canSync || busy}>
            <RefreshCw className={busy ? 'spin' : ''} size={18} aria-hidden /> Sync now
          </button>
        </div>
      </section>

      {dashboard?.availability.note && <p className="notice">{dashboard.availability.note}</p>}

      <section className="metric-grid youtube-metrics" aria-label="Meta summary metrics">
        {(dashboard?.metrics || []).map((metric) => (
          <MetricCard key={metric.key} metric={metric} compare={compare} />
        ))}
        {!dashboard?.metrics.length && (
          <article className="metric-card unavailable">
            <span>Read-only insights</span>
            <strong>N/A</strong>
            <small>No data available</small>
          </article>
        )}
      </section>

      <section className="panel chart-panel" aria-labelledby="meta-performance-title">
        <div className="panel-title">
          <div>
            <h2 id="meta-performance-title">Daily read-only insights</h2>
            <p>Provider-reported daily values; unavailable metrics remain visibly N/A.</p>
          </div>
        </div>
        {trend.length > 0 ? (
          <div className="chart-box" role="img" aria-label="Line chart of daily Meta insights">
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={trend} margin={{ top: 12, right: 24, bottom: 12, left: 0 }}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="label" minTickGap={24} />
                <YAxis tickFormatter={formatCompact} />
                <Tooltip formatter={formatTooltipNumber} />
                <Legend />
                <Line
                  type="monotone"
                  dataKey={primaryKey}
                  name={isInstagram ? 'Views' : 'Media views'}
                  stroke="var(--chart-a)"
                  strokeWidth={2.5}
                  dot={false}
                  connectNulls
                />
                <Line
                  type="monotone"
                  dataKey={secondaryKey}
                  name={isInstagram ? 'Reach' : 'Post engagements'}
                  stroke="var(--chart-b)"
                  strokeWidth={2.5}
                  dot={false}
                  connectNulls
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        ) : (
          <div className="chart-empty">No daily Meta insight rows are stored for this range.</div>
        )}
      </section>

      <section className="panel" aria-labelledby="meta-content-title">
        <div className="panel-title">
          <div>
            <h2 id="meta-content-title">Content performance</h2>
            <p>
              {isInstagram ? 'Feed, carousel, and Reels media only.' : 'Published Page posts and available engagement.'}
            </p>
          </div>
        </div>
        {dashboard?.content.length ? (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th scope="col">Content</th>
                  <th scope="col">Published</th>
                  <th scope="col">Views</th>
                  <th scope="col">Likes</th>
                  <th scope="col">Comments</th>
                  <th scope="col">Shares</th>
                </tr>
              </thead>
              <tbody>
                {dashboard.content.map((row) => (
                  <tr key={row.id}>
                    <td data-label="Content">
                      {row.share_url ? (
                        <a href={row.share_url} target="_blank" rel="noreferrer">
                          {row.title || row.provider_content_id} <ExternalLink size={14} aria-hidden />
                        </a>
                      ) : (
                        row.title || row.provider_content_id
                      )}
                    </td>
                    <td data-label="Published">{formatDate(row.published_at, { dateStyle: 'medium' })}</td>
                    <td data-label="Views">{formatNumber(row.view_count)}</td>
                    <td data-label="Likes">{formatNumber(row.like_count)}</td>
                    <td data-label="Comments">{formatNumber(row.comment_count)}</td>
                    <td data-label="Shares">{formatNumber(row.share_count)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="table-empty">No content performance is available for this period.</div>
        )}
      </section>
    </>
  );
}

function YouTubeOverview({
  dashboard,
  compare,
  busy,
  canSync,
  onSync
}: {
  dashboard: YouTubeDashboardData | null;
  compare: boolean;
  busy: boolean;
  canSync: boolean;
  onSync: () => void;
}) {
  const trend = (dashboard?.trend || []).map((point) => ({
    ...point,
    label: formatDate(point.date, { month: 'short', day: 'numeric' })
  }));
  const connected = dashboard?.connection.status === 'active';
  const delayed = dashboard?.availability.state === 'delayed';

  return (
    <>
      <section className="panel youtube-channel" aria-labelledby="youtube-channel-title">
        <div className="channel-identity">
          {dashboard?.channel?.thumbnail_url ? (
            <img src={dashboard.channel.thumbnail_url} alt="" referrerPolicy="no-referrer" />
          ) : (
            <span className="channel-placeholder" aria-hidden>
              <Youtube size={24} />
            </span>
          )}
          <div>
            <p className="eyebrow">YouTube channel</p>
            <h2 id="youtube-channel-title">{dashboard?.channel?.display_name || 'No channel connected'}</h2>
            <p className="muted">
              {connected
                ? `Last synced ${formatDate(dashboard?.connection.last_successful_sync_at)}`
                : 'Choose an authorized channel in Connections to begin syncing.'}
            </p>
          </div>
        </div>
        <div className="button-row">
          <StatusBadge status={dashboard?.connection.status || 'disconnected'} />
          <button type="button" onClick={onSync} disabled={!connected || !canSync || busy}>
            <RefreshCw className={busy ? 'spin' : ''} size={18} aria-hidden /> Sync now
          </button>
        </div>
      </section>

      {delayed && (
        <p className="notice" role="status">
          YouTube reporting is available through{' '}
          {dashboard?.availability.data_through_date
            ? formatDate(dashboard.availability.data_through_date, { dateStyle: 'medium' })
            : 'an earlier date'}
          . The requested range ends{' '}
          {formatDate(dashboard?.availability.requested_through_date, { dateStyle: 'medium' })}.
        </p>
      )}

      <section className="metric-grid youtube-metrics" aria-label="YouTube summary metrics">
        {(dashboard?.metrics || []).map((metric) => (
          <YouTubeMetricCard key={metric.key} metric={metric} compare={compare} />
        ))}
        {!dashboard?.metrics.length &&
          ['Subscribers', 'Channel views', 'Videos', 'Views', 'Watch time', 'Net subscribers'].map((label) => (
            <article key={label} className="metric-card unavailable">
              <span>{label}</span>
              <strong>N/A</strong>
              <small>No data available</small>
            </article>
          ))}
      </section>

      <section className="panel chart-panel" aria-labelledby="youtube-performance-title">
        <div className="panel-title between">
          <div>
            <h2 id="youtube-performance-title">Views and watch time</h2>
            <p>Daily values reported by YouTube Analytics for the selected range.</p>
          </div>
          <span className="muted">
            Data through {formatDate(dashboard?.availability.data_through_date, { dateStyle: 'medium' })}
          </span>
        </div>
        {trend.length > 0 ? (
          <div className="chart-box" role="img" aria-label="Line chart of daily YouTube views and watch time">
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={trend} margin={{ top: 12, right: 24, bottom: 12, left: 0 }}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="label" minTickGap={24} />
                <YAxis yAxisId="views" tickFormatter={formatCompact} />
                <YAxis yAxisId="watch" orientation="right" tickFormatter={formatCompact} />
                <Tooltip formatter={formatTooltipNumber} />
                <Legend />
                <Line
                  yAxisId="views"
                  type="monotone"
                  dataKey="views"
                  name="Views"
                  stroke="var(--chart-a)"
                  strokeWidth={2.5}
                  dot={false}
                  connectNulls
                />
                <Line
                  yAxisId="watch"
                  type="monotone"
                  dataKey="watch_time_minutes"
                  name="Watch time (minutes)"
                  stroke="var(--chart-b)"
                  strokeWidth={2.5}
                  dot={false}
                  connectNulls
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        ) : (
          <div className="chart-empty">No daily YouTube Analytics rows are stored for this range.</div>
        )}
      </section>

      <section className="panel chart-panel" aria-labelledby="youtube-subscribers-title">
        <div className="panel-title">
          <div>
            <h2 id="youtube-subscribers-title">Subscriber movement</h2>
            <p>Daily subscribers gained, lost, and net change.</p>
          </div>
        </div>
        {trend.length > 0 ? (
          <div className="chart-box" role="img" aria-label="Bar chart of daily YouTube subscriber movement">
            <ResponsiveContainer width="100%" height={280}>
              <BarChart data={trend} margin={{ top: 12, right: 24, bottom: 12, left: 0 }}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="label" minTickGap={24} />
                <YAxis tickFormatter={formatCompact} />
                <Tooltip formatter={formatTooltipNumber} />
                <Legend />
                <Bar dataKey="subscribers_gained" name="Gained" fill="var(--chart-a)" radius={[3, 3, 0, 0]} />
                <Bar dataKey="subscribers_lost" name="Lost" fill="var(--red)" radius={[3, 3, 0, 0]} />
                <Bar dataKey="net_subscribers" name="Net" fill="var(--chart-c)" radius={[3, 3, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        ) : (
          <div className="chart-empty">No subscriber movement is available for this range.</div>
        )}
      </section>

      <section className="panel" aria-labelledby="youtube-content-title">
        <div className="panel-title between">
          <div>
            <h2 id="youtube-content-title">Video performance</h2>
            <p>Period metrics from YouTube Analytics. Missing values remain N/A.</p>
          </div>
          {dashboard?.range.videoPeriodKey && <StatusBadge status={dashboard.range.videoPeriodKey} />}
        </div>
        {!dashboard?.availability.video_period_supported ? (
          <div className="table-empty">Video-level breakdowns are available for the 7, 30, and 90 day ranges.</div>
        ) : dashboard.content.length > 0 ? (
          <div className="table-wrap youtube-content-table">
            <table>
              <thead>
                <tr>
                  <th scope="col">Video</th>
                  <th scope="col">Views</th>
                  <th scope="col">Watch time</th>
                  <th scope="col">Avg. duration</th>
                  <th scope="col">Avg. viewed</th>
                  <th scope="col">Likes</th>
                  <th scope="col">Comments</th>
                  <th scope="col">Shares</th>
                </tr>
              </thead>
              <tbody>
                {dashboard.content.map((row) => (
                  <tr key={row.id}>
                    <td data-label="Video">
                      <div className="youtube-video-cell">
                        {row.thumbnail_url ? (
                          <img src={row.thumbnail_url} alt="" loading="lazy" referrerPolicy="no-referrer" />
                        ) : null}
                        <div>
                          {row.share_url ? (
                            <a href={row.share_url} target="_blank" rel="noreferrer">
                              {row.title || row.provider_content_id} <ExternalLink size={14} aria-hidden />
                            </a>
                          ) : (
                            <strong>{row.title || row.provider_content_id}</strong>
                          )}
                          <small>{formatDate(row.published_at, { dateStyle: 'medium' })}</small>
                        </div>
                      </div>
                    </td>
                    <td data-label="Views">{formatNumber(row.views)}</td>
                    <td data-label="Watch time">{formatMinutes(row.watch_time_minutes)}</td>
                    <td data-label="Avg. duration">{formatSeconds(row.average_view_duration_seconds)}</td>
                    <td data-label="Avg. viewed">{formatPercent(row.average_view_percentage)}</td>
                    <td data-label="Likes">{formatNumber(row.likes)}</td>
                    <td data-label="Comments">{formatNumber(row.comments)}</td>
                    <td data-label="Shares">{formatNumber(row.shares)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="table-empty">No video Analytics rows are stored for this period.</div>
        )}
      </section>
    </>
  );
}

function YouTubeMetricCard({ metric, compare }: { metric: YouTubeMetric; compare: boolean }) {
  const hasPeriodComparison = metric.semantics.startsWith('selected_period');
  const direction = !metric.available
    ? 'unavailable'
    : hasPeriodComparison && metric.delta !== null
      ? metric.delta > 0
        ? 'positive'
        : metric.delta < 0
          ? 'negative'
          : 'neutral'
      : 'neutral';
  const value = metric.key === 'watch_time_period' ? formatMinutes(metric.value) : formatNumber(metric.value);
  return (
    <article className={`metric-card ${direction}`}>
      <span>{metric.label}</span>
      <strong>{value}</strong>
      <small>
        {!metric.available
          ? 'Unavailable from YouTube'
          : !compare
            ? 'Comparison hidden'
            : !hasPeriodComparison
              ? metric.semantics === 'lifetime'
                ? 'Lifetime total'
                : 'Latest available data'
              : metric.delta === null
                ? 'Previous period unavailable'
                : `${metric.delta >= 0 ? '+' : ''}${metric.key === 'watch_time_period' ? formatMinutes(metric.delta) : formatNumber(metric.delta)}${metric.percent_change === null ? ' (percent N/A)' : ` (${metric.percent_change.toFixed(1)}%)`}`}
      </small>
    </article>
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
            <span title="No comparable earlier data is available.">Comparison unavailable</span>
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

const socialProviderNames: Record<Exclude<OverviewProvider, 'google_analytics_4'>, string> = {
  tiktok: 'TikTok',
  youtube: 'YouTube',
  facebook_pages: 'Facebook Pages',
  instagram: 'Instagram'
};

function socialProviderName(provider: Exclude<OverviewProvider, 'google_analytics_4'>) {
  return socialProviderNames[provider];
}

function Content({
  workspace,
  content,
  providers,
  provider,
  connectionId,
  sort,
  direction,
  search,
  page,
  pageSize,
  rangeQuery,
  onSort,
  onSearch,
  onProviderChange,
  onConnectionChange,
  onPageChange,
  onPageSizeChange,
  onOpenDetail
}: {
  workspace: Workspace;
  content: ContentData | null;
  providers: ProviderCatalogItem[];
  provider: SocialContentProvider;
  connectionId: string;
  sort: ContentSort;
  direction: SortDirection;
  search: string;
  page: number;
  pageSize: number;
  rangeQuery: URLSearchParams;
  onSort: (sort: ContentSort, direction: SortDirection) => void;
  onSearch: (search: string) => void;
  onProviderChange: (provider: SocialContentProvider) => void;
  onConnectionChange: (connectionId: string) => void;
  onPageChange: (page: number) => void;
  onPageSizeChange: (size: number) => void;
  onOpenDetail: (contentId: string) => void;
}) {
  const total = content?.total || 0;
  const totalPages = Math.max(Math.ceil(total / pageSize), 1);
  const availableProviders = providers.filter((item) =>
    ['tiktok', 'youtube', 'facebook_pages', 'instagram'].includes(item.id)
  );
  const resourceOptions = availableProviders
    .filter((item) => provider === 'all' || item.id === provider)
    .flatMap((item) =>
      (item.connections || [])
        .filter((connection) => Boolean(connection.id))
        .map((connection) => ({
          id: connection.id,
          provider: item.id as Exclude<OverviewProvider, 'google_analytics_4'>,
          name:
            connection.account?.display_name ||
            connection.account?.username ||
            connection.account?.id ||
            item.resourceName
        }))
    );
  const exportParams = new URLSearchParams(rangeQuery);
  exportParams.set('provider', provider);
  if (connectionId) exportParams.set('connection_id', connectionId);
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
          Provider
          <select value={provider} onChange={(event) => onProviderChange(event.target.value as SocialContentProvider)}>
            <option value="all">All social providers</option>
            {availableProviders.map((item) => (
              <option key={item.id} value={item.id}>
                {item.name}
              </option>
            ))}
          </select>
        </label>
        <label>
          Resource
          <select value={connectionId} onChange={(event) => onConnectionChange(event.target.value)}>
            <option value="">All selected resources</option>
            {resourceOptions.map((resource) => (
              <option key={resource.id} value={resource.id}>
                {provider === 'all' ? `${socialProviderName(resource.provider)} · ` : ''}
                {resource.name}
              </option>
            ))}
          </select>
        </label>
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
                  <th scope="col">Source</th>
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
                    <td data-label="Source">
                      <strong>{socialProviderName(row.provider)}</strong>
                      <small>{row.resource_name || 'Selected resource'}</small>
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
          {search || provider !== 'all' || connectionId
            ? 'No content matches the active filters.'
            : 'No social content performance is available.'}
        </div>
      )}
      <p className="muted">
        Counts retain each provider’s reporting semantics. Missing or unsupported values remain N/A and are never
        converted to zero.
      </p>
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
          <p className="eyebrow">{socialProviderName(detail.item.provider)} content detail</p>
          <h2 id="content-detail-title">{contentLabel(detail.item)}</h2>
          <p>{detail.item.description || 'No provider description available.'}</p>
          <p className="muted">{detail.item.resource_name || 'Selected provider resource'}</p>
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

function Reports({
  role,
  configuration,
  providers,
  reports,
  preview,
  busy,
  onPreview,
  onGenerate,
  onDownload,
  onDelete
}: {
  role: Role;
  configuration: ReportConfiguration | null;
  providers: ProviderCatalogItem[];
  reports: ReportRun[];
  preview: ReportPreview | null;
  busy: boolean;
  onPreview: (request: ReportRequest) => Promise<void>;
  onGenerate: (request: ReportRequest) => Promise<void>;
  onDownload: (report: ReportRun) => Promise<void>;
  onDelete: (report: ReportRun) => Promise<void>;
}) {
  const [title, setTitle] = useState('Monthly performance report');
  const [subtitle, setSubtitle] = useState('Read-only analytics summary');
  const [reportRange, setReportRange] = useState<RangeKey>('30d');
  const [from, setFrom] = useState(todayInputValue(-30));
  const [to, setTo] = useState(todayInputValue(-1));
  const [timezone, setTimezone] = useState(() => Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC');
  const [comparisonEnabled, setComparisonEnabled] = useState(true);
  const [sections, setSections] = useState<string[]>([
    'executive_summary',
    'cross_platform_summary',
    'resource_sections',
    'methodology'
  ]);
  const [selectedResourceKeys, setSelectedResourceKeys] = useState<string[]>([]);
  const [formError, setFormError] = useState('');

  const resources = useMemo(
    () =>
      providers.flatMap((provider) => {
        if (!['tiktok', 'youtube', 'facebook_pages', 'instagram', 'google_analytics_4'].includes(provider.id))
          return [];
        return (provider.connections || [])
          .filter(
            (connection) =>
              Boolean(connection.id) && !['disconnected', 'revoked', 'disabled'].includes(connection.status)
          )
          .map((connection) => ({
            key: `${provider.id}:${connection.id}`,
            provider: provider.id as OverviewProvider,
            providerName: provider.name,
            connectionId: connection.id as string,
            name:
              connection.account?.display_name ||
              connection.account?.username ||
              connection.account?.id ||
              provider.resourceName,
            resourceId: connection.account?.id || '',
            status: connection.status,
            dataThrough: connection.data_through_at || null
          }));
      }),
    [providers]
  );

  if (!roleCanReport(role)) {
    return (
      <section className="empty-band" aria-labelledby="reports-permission-title">
        <Lock size={24} aria-hidden />
        <div>
          <h2 id="reports-permission-title">Reports require Analyst access</h2>
          <p className="muted">
            Ask a workspace owner or admin to change your role before creating or downloading reports.
          </p>
        </div>
      </section>
    );
  }

  if (!configuration) {
    return <StateBanner state="loading" />;
  }

  if (!configuration.enabled || !configuration.ready) {
    return (
      <section className="empty-band" aria-labelledby="reports-configuration-title">
        <AlertCircle size={24} aria-hidden />
        <div>
          <h2 id="reports-configuration-title">PDF reports are unavailable</h2>
          <p className="muted">Report generation and private storage are not available for this environment.</p>
        </div>
      </section>
    );
  }
  const activeReportConfiguration = configuration;

  const selectedResources = resources.filter((resource) => selectedResourceKeys.includes(resource.key));
  const sectionLabels: Record<string, string> = {
    executive_summary: 'Executive summary',
    cross_platform_summary: 'Cross-platform summary',
    resource_sections: 'Provider and resource detail',
    methodology: 'Methodology and data notes'
  };

  function requestFromForm(): ReportRequest | null {
    setFormError('');
    if (!title.trim()) {
      setFormError('Enter a report title.');
      return null;
    }
    if (selectedResources.length < 1) {
      setFormError('Select at least one connected resource.');
      return null;
    }
    if (selectedResources.length > activeReportConfiguration.max_resources) {
      setFormError(`Select no more than ${activeReportConfiguration.max_resources} resources.`);
      return null;
    }
    if (reportRange === 'custom' && (!from || !to || new Date(from) > new Date(to))) {
      setFormError('Choose a valid custom date range.');
      return null;
    }
    return {
      title: title.trim(),
      subtitle: subtitle.trim(),
      timezone,
      range: reportRange,
      ...(reportRange === 'custom' ? { from, to } : {}),
      comparison_enabled: comparisonEnabled,
      sections,
      resources: selectedResources.map((resource) => ({
        provider: resource.provider,
        connection_id: resource.connectionId
      }))
    };
  }

  async function previewReport() {
    const request = requestFromForm();
    if (request) await onPreview(request);
  }

  async function generateReport() {
    const request = requestFromForm();
    if (request) await onGenerate(request);
  }

  return (
    <div className="reports-layout">
      <section className="panel report-builder" aria-labelledby="report-builder-title">
        <div className="panel-title between">
          <div>
            <p className="eyebrow">Stored-data export</p>
            <h2 id="report-builder-title">Create PDF report</h2>
            <p>Choose explicit connected resources. Generation runs securely in the background.</p>
          </div>
          <span className="status-badge ready">7-day retention</span>
        </div>

        <div className="report-form-grid">
          <label>
            Report title
            <input maxLength={180} value={title} onChange={(event) => setTitle(event.target.value)} />
          </label>
          <label>
            Report timezone
            <input maxLength={64} value={timezone} onChange={(event) => setTimezone(event.target.value)} />
          </label>
          <label className="report-subtitle-field">
            Subtitle
            <textarea maxLength={300} rows={3} value={subtitle} onChange={(event) => setSubtitle(event.target.value)} />
          </label>
        </div>

        <fieldset className="report-fieldset">
          <legend>Date range</legend>
          <div className="segmented" aria-label="Report date range">
            {(['7d', '30d', '90d', 'custom'] as RangeKey[]).map((value) => (
              <button
                key={value}
                type="button"
                className={reportRange === value ? 'active' : ''}
                onClick={() => setReportRange(value)}
              >
                {value === 'custom' ? 'Custom' : value.toUpperCase()}
              </button>
            ))}
          </div>
          {reportRange === 'custom' && (
            <div className="date-pair">
              <label>
                From
                <input type="date" value={from} onChange={(event) => setFrom(event.target.value)} />
              </label>
              <label>
                To
                <input type="date" value={to} onChange={(event) => setTo(event.target.value)} />
              </label>
            </div>
          )}
          <label className="toggle">
            <input
              type="checkbox"
              checked={comparisonEnabled}
              onChange={(event) => setComparisonEnabled(event.target.checked)}
            />
            Compare with the previous period where a stored baseline exists
          </label>
        </fieldset>

        <fieldset className="report-fieldset">
          <legend>Resources</legend>
          {resources.length === 0 ? (
            <p className="muted">Connect and select at least one provider resource before creating a report.</p>
          ) : (
            <div className="report-resource-list">
              {resources.map((resource) => (
                <label className="report-resource-option" key={resource.key}>
                  <input
                    type="checkbox"
                    aria-label={`Include ${resource.name} from ${resource.providerName}`}
                    checked={selectedResourceKeys.includes(resource.key)}
                    onChange={(event) =>
                      setSelectedResourceKeys((current) =>
                        event.target.checked
                          ? [...current, resource.key]
                          : current.filter((key) => key !== resource.key)
                      )
                    }
                  />
                  <span>
                    <strong>{resource.name}</strong>
                    <small>
                      {resource.providerName} · {resource.status.replace(/_/g, ' ')}
                      {resource.dataThrough
                        ? ` · data through ${formatDate(resource.dataThrough, { dateStyle: 'medium' })}`
                        : ''}
                    </small>
                  </span>
                </label>
              ))}
            </div>
          )}
        </fieldset>

        <fieldset className="report-fieldset">
          <legend>Sections</legend>
          <div className="report-section-list">
            {Object.entries(sectionLabels).map(([key, label]) => (
              <label className="toggle" key={key}>
                <input
                  type="checkbox"
                  checked={sections.includes(key)}
                  disabled={key === 'resource_sections'}
                  onChange={(event) =>
                    setSections((current) =>
                      event.target.checked ? [...current, key] : current.filter((section) => section !== key)
                    )
                  }
                />
                {label}
              </label>
            ))}
          </div>
        </fieldset>

        {formError && (
          <p className="form-error" role="alert">
            {formError}
          </p>
        )}
        <div className="button-row start">
          <button
            type="button"
            className="ghost-button"
            disabled={busy || resources.length === 0}
            onClick={previewReport}
          >
            <Search size={18} aria-hidden />
            Preview outline
          </button>
          <button type="button" disabled={busy || resources.length === 0} onClick={generateReport}>
            {busy ? <Loader2 className="spin" size={18} aria-hidden /> : <FileText size={18} aria-hidden />}
            Generate PDF
          </button>
        </div>
      </section>

      {preview && (
        <section className="panel report-preview" aria-labelledby="report-preview-title">
          <div className="panel-title between">
            <div>
              <p className="eyebrow">Preview</p>
              <h2 id="report-preview-title">{preview.title}</h2>
              <p>
                {preview.range.from} to {preview.range.to} · {preview.timezone}
              </p>
            </div>
            <span className="status-badge ready">About {preview.estimated_page_count} pages</span>
          </div>
          <div className="report-preview-grid">
            <div>
              <h3>Sections</h3>
              <ul>
                {preview.sections
                  .filter((section) => section.included)
                  .map((section) => (
                    <li key={section.key}>{sectionLabels[section.key] || section.key}</li>
                  ))}
              </ul>
            </div>
            <div>
              <h3>Selected resources</h3>
              <ul>
                {preview.resources.map((resource) => (
                  <li key={`${resource.provider}:${resource.connection_id}`}>
                    {resource.provider_name} · {resource.resource_name} · {resource.available_metric_count} available
                    metrics
                  </li>
                ))}
              </ul>
            </div>
          </div>
          <p className="muted">
            The final artifact uses the immutable stored snapshot captured when generation is queued.
          </p>
        </section>
      )}

      <section className="panel" aria-labelledby="report-history-title">
        <div className="panel-title between">
          <div>
            <p className="eyebrow">Report history</p>
            <h2 id="report-history-title">Generated reports</h2>
            <p>Queued and running reports refresh automatically.</p>
          </div>
          {reports.some((report) => report.status === 'queued' || report.status === 'running') && (
            <span className="status-badge pending">
              <Loader2 className="spin" size={14} aria-hidden /> Preparing
            </span>
          )}
        </div>
        {reports.length === 0 ? (
          <div className="empty-band">
            <FileText size={22} aria-hidden />
            <p>No reports have been generated for this workspace.</p>
          </div>
        ) : (
          <div className="report-history-list">
            {reports.map((report) => (
              <article className="report-history-item" key={report.id}>
                <div className="report-history-heading">
                  <div>
                    <h3>{report.title}</h3>
                    <p className="muted">
                      {report.range.from} to {report.range.to} · {report.resources.length} resource
                      {report.resources.length === 1 ? '' : 's'} · queued {formatDate(report.queued_at)}
                    </p>
                  </div>
                  <span
                    className={`status-badge ${report.status === 'completed' ? 'ready' : report.status === 'running' ? 'connecting' : report.status === 'queued' ? 'pending' : report.status}`}
                  >
                    {report.status === 'running' ? `${report.progress_percent}% running` : report.status}
                  </span>
                </div>
                <div className="report-history-meta">
                  {report.artifact ? (
                    <span>
                      {report.artifact.page_count} pages · {(report.artifact.byte_size / 1024).toFixed(1)} KB
                    </span>
                  ) : report.failure_code ? (
                    <span>{reportFailureMessage(report)}</span>
                  ) : (
                    <span>The report is still being prepared.</span>
                  )}
                  {report.expires_at && report.status === 'completed' && (
                    <span>Expires {formatDate(report.expires_at)}</span>
                  )}
                </div>
                <div className="button-row start">
                  <button
                    type="button"
                    disabled={busy || report.status !== 'completed' || !report.artifact}
                    onClick={() => void onDownload(report)}
                  >
                    <Download size={17} aria-hidden />
                    Download
                  </button>
                  <button type="button" className="danger" disabled={busy} onClick={() => void onDelete(report)}>
                    <Trash2 size={17} aria-hidden />
                    Delete
                  </button>
                </div>
              </article>
            ))}
          </div>
        )}
      </section>
    </div>
  );
}

function Connections({
  role,
  dashboard,
  providers,
  busy,
  disconnectTarget,
  onTikTokConnect,
  onYouTubeConnect,
  onYouTubeSelect,
  onYouTubeSync,
  onGoogleAnalyticsConnect,
  onGoogleAnalyticsSelect,
  onGoogleAnalyticsSync,
  onMetaConnect,
  onMetaSelect,
  onMetaSync,
  onDisconnectRequest,
  onDisconnectCancel,
  onDisconnectConfirm
}: {
  role: Role;
  dashboard: DashboardData | null;
  providers: ProviderCatalogItem[];
  busy: boolean;
  disconnectTarget: DisconnectTarget | null;
  onTikTokConnect: () => void;
  onYouTubeConnect: (connectionId?: string) => void;
  onYouTubeSelect: (resourceId: string) => void;
  onYouTubeSync: (connectionId?: string) => void;
  onGoogleAnalyticsConnect: (connectionId?: string) => void;
  onGoogleAnalyticsSelect: (resourceId: string) => void;
  onGoogleAnalyticsSync: (connectionId?: string) => void;
  onMetaConnect: (provider: 'facebook' | 'instagram', connectionId?: string) => void;
  onMetaSelect: (provider: 'facebook' | 'instagram', resourceId: string) => void;
  onMetaSync: (provider: 'facebook_pages' | 'instagram', connectionId?: string) => void;
  onDisconnectRequest: (target: DisconnectTarget) => void;
  onDisconnectCancel: () => void;
  onDisconnectConfirm: () => void;
}) {
  const allowed = roleCanManage(role);
  const latestError = dashboard?.latest_sync?.error_category;
  const catalog = providers.length > 0 ? providers : [];
  const orderedCatalog = [...catalog].sort((left, right) => {
    if (left.id === 'facebook_pages' && right.id === 'instagram') return -1;
    if (left.id === 'instagram' && right.id === 'facebook_pages') return 1;
    return 0;
  });
  return (
    <section className="panel" aria-labelledby="connection-title">
      <div className="panel-title">
        <div>
          <h2 id="connection-title">Connections</h2>
          <p>Choose the accounts and properties whose analytics should appear in this workspace.</p>
        </div>
      </div>
      <div className="provider-list">
        {orderedCatalog.map((provider) => {
          const providerStatus = provider.connection?.status || provider.status;
          const isTikTok = provider.id === 'tiktok';
          const isYouTube = provider.id === 'youtube';
          const isFacebook = provider.id === 'facebook_pages';
          const isInstagram = provider.id === 'instagram';
          const isMeta = isFacebook || isInstagram;
          const isGoogleAnalytics = provider.id === 'google_analytics_4';
          const metaPath = isFacebook ? 'facebook' : 'instagram';
          const canConnect = allowed && provider.connectable;
          const canDisconnect = allowed && (isTikTok || isMeta) && provider.connection?.status !== 'disconnected';
          const youtubeConnections = provider.connections || [];
          const metaConnections = isMeta ? provider.connections || [] : [];
          const googleAnalyticsConnections = isGoogleAnalytics ? provider.connections || [] : [];
          const unselectedResources = (provider.resources || []).filter((resource) => !resource.selected);
          const grantedScopes = (provider.authorization?.scopes || [])
            .filter((scope) => scope.status === 'granted')
            .map((scope) => scope.scope);
          const canStartYouTube =
            isYouTube && canConnect && (!provider.authorization || (provider.resources || []).length === 0);
          const canStartGoogleAnalytics =
            isGoogleAnalytics && canConnect && (!provider.authorization || (provider.resources || []).length === 0);
          const canStartMeta = isMeta && canConnect && provider.status !== 'authorizing';
          return (
            <article key={provider.id} className="provider-row">
              <div className="provider-main">
                <div className="provider-heading">
                  <strong>{provider.name}</strong>
                  <StatusBadge status={providerStatus} />
                </div>
                <p className="muted">{provider.resourceName}</p>
                {provider.connection?.account && (
                  <p className="muted">
                    Connected resource:{' '}
                    {provider.connection.account.display_name ||
                      provider.connection.account.username ||
                      provider.connection.account.id}
                  </p>
                )}
                {provider.connection?.reconnect_reason && !isYouTube && (
                  <p className="notice error">This connection needs attention before syncing can continue.</p>
                )}
                {!provider.implemented && <p className="muted">This analytics source is not available yet.</p>}
                {(isYouTube || isMeta || isGoogleAnalytics) &&
                  provider.configuration?.warnings.map((warning) => (
                    <p key={warning} className="notice error">
                      This connection is temporarily unavailable because its setup is incomplete. Contact support if you
                      need access.
                    </p>
                  ))}
                {isYouTube && provider.status === 'no_channels' && (
                  <p className="notice">Google returned no channels accessible to this authorization.</p>
                )}
                {isYouTube && provider.status === 'authorization_denied' && (
                  <p className="notice">Authorization was cancelled. No YouTube data was accessed.</p>
                )}
                {isYouTube && provider.status === 'missing_scopes' && (
                  <p className="notice error">
                    Not all required YouTube analytics access was approved. Authorize again and review each requested
                    item.
                  </p>
                )}
                {isYouTube && provider.status === 'provider_error' && (
                  <p className="notice error">The latest authorization attempt failed at Google or YouTube.</p>
                )}
                {isGoogleAnalytics && provider.status === 'no_properties' && (
                  <p className="notice">Google returned no GA4 properties with usable timezone and currency details.</p>
                )}
                {isGoogleAnalytics && provider.status === 'authorization_denied' && (
                  <p className="notice">Authorization was cancelled. No Google Analytics data was accessed.</p>
                )}
                {isGoogleAnalytics && provider.status === 'missing_scopes' && (
                  <p className="notice error">
                    Google did not grant the exact analytics.readonly permission. Authorize again to continue.
                  </p>
                )}
                {isGoogleAnalytics && provider.status === 'provider_error' && (
                  <p className="notice error">The latest Google Analytics authorization attempt failed.</p>
                )}
                {isMeta && provider.status === 'no_resources' && (
                  <p className="notice">No eligible Pages or professional accounts were available to select.</p>
                )}
                {isMeta && provider.status === 'authorization_denied' && (
                  <p className="notice">Authorization was cancelled. No Meta data was accessed.</p>
                )}
                {isMeta && provider.status === 'missing_scopes' && (
                  <p className="notice error">
                    Not all required analytics access was approved. Authorize again and review each requested item.
                  </p>
                )}
                {isMeta && provider.status === 'provider_error' && (
                  <p className="notice error">
                    The latest Meta authorization attempt failed without creating a connection.
                  </p>
                )}
                {isTikTok && latestError === 'scope' && (
                  <p className="notice error">TikTok needs additional analytics access before syncing can continue.</p>
                )}
                <div className="scope-list" aria-label={`${provider.name} analytics access`}>
                  {providerAccessLabels(provider.id).map((label) => (
                    <span key={label}>Read: {label}</span>
                  ))}
                </div>
                {(isYouTube || isMeta || isGoogleAnalytics) && provider.authorization && grantedScopes.length === 0 && (
                  <p className="muted">Analytics access has not been granted.</p>
                )}

                {isYouTube && unselectedResources.length > 0 && (
                  <div className="resource-list" aria-label="YouTube channels available to connect">
                    <h3>Available channels</h3>
                    {unselectedResources.map((resource) => (
                      <div key={resource.id} className="resource-row">
                        <div className="channel-identity compact">
                          {resource.thumbnail_url ? (
                            <img src={resource.thumbnail_url} alt="" referrerPolicy="no-referrer" />
                          ) : (
                            <span className="channel-placeholder" aria-hidden>
                              <Youtube size={18} />
                            </span>
                          )}
                          <div>
                            <strong>{resource.display_name}</strong>
                            <small>{resource.provider_resource_id}</small>
                            {Boolean(resource.attached_elsewhere_count) && (
                              <small>
                                Also connected in {resource.attached_elsewhere_count}{' '}
                                {resource.attached_elsewhere_count === 1 ? 'workspace' : 'workspaces'} you can access
                              </small>
                            )}
                          </div>
                        </div>
                        <button type="button" onClick={() => onYouTubeSelect(resource.id)} disabled={!allowed || busy}>
                          <Link2 size={17} aria-hidden /> Select
                        </button>
                      </div>
                    ))}
                  </div>
                )}

                {isYouTube && youtubeConnections.length > 0 && (
                  <div className="resource-list" aria-label="Connected YouTube channels">
                    <h3>Connected channels</h3>
                    {youtubeConnections.map((connection) => (
                      <div key={connection.id || connection.account?.id} className="resource-row connection-resource">
                        <div className="channel-identity compact">
                          {connection.account?.thumbnail_url ? (
                            <img src={connection.account.thumbnail_url} alt="" referrerPolicy="no-referrer" />
                          ) : (
                            <span className="channel-placeholder" aria-hidden>
                              <Youtube size={18} />
                            </span>
                          )}
                          <div>
                            <strong>
                              {connection.account?.display_name || connection.account?.id || 'YouTube channel'}
                            </strong>
                            <small>
                              Last sync {formatDate(connection.last_successful_sync_at)}; data through{' '}
                              {formatDate(connection.data_through_at, { dateStyle: 'medium' })}
                            </small>
                            {connection.reconnect_reason && (
                              <small className="notice error">
                                {connection.reconnect_reason.startsWith('quota:')
                                  ? 'YouTube quota is currently exhausted; the stored dashboard remains available.'
                                  : connection.reconnect_reason.startsWith('provider:') ||
                                      connection.reconnect_reason.startsWith('timeout:') ||
                                      connection.reconnect_reason.startsWith('network:')
                                    ? 'YouTube is delayed; synchronization will retry within its bounded schedule.'
                                    : 'This channel needs YouTube authorization before synchronization can resume.'}
                              </small>
                            )}
                            <div className="capability-list" aria-label="Channel capabilities">
                              {(connection.capabilities || []).map((capability) => (
                                <span
                                  key={capability.key}
                                  className={capability.status === 'available' ? '' : 'delayed'}
                                >
                                  {capability.key.replaceAll('_', ' ')}: {capability.status}
                                </span>
                              ))}
                            </div>
                          </div>
                        </div>
                        <div className="button-row">
                          <StatusBadge status={connection.status} />
                          <button
                            type="button"
                            className="ghost-button"
                            disabled={!roleCanSync(role) || connection.status !== 'active' || busy}
                            onClick={() => onYouTubeSync(connection.id)}
                            title="Sync this YouTube channel"
                          >
                            <RefreshCw size={17} aria-hidden /> Sync
                          </button>
                          <button
                            type="button"
                            disabled={!canConnect || busy}
                            onClick={() => onYouTubeConnect(connection.id)}
                          >
                            <ExternalLink size={17} aria-hidden /> Reauthorize
                          </button>
                        </div>
                      </div>
                    ))}
                  </div>
                )}

                {isGoogleAnalytics && unselectedResources.length > 0 && (
                  <div className="resource-list" aria-label="GA4 properties available to connect">
                    <h3>Available properties</h3>
                    {unselectedResources.map((resource) => (
                      <div key={resource.id} className="resource-row">
                        <div className="channel-identity compact">
                          <span className="channel-placeholder" aria-hidden>
                            <BarChart3 size={18} />
                          </span>
                          <div>
                            <strong>{resource.display_name}</strong>
                            <small>{resource.account_name || resource.provider_resource_id}</small>
                            <small>
                              {resource.timezone || 'Timezone unavailable'} ·{' '}
                              {resource.currency || 'Currency unavailable'}
                            </small>
                            {resource.available === false && (
                              <small className="notice error">
                                Unavailable:{' '}
                                {(resource.unavailable_reason || 'property_details_unavailable').replaceAll('_', ' ')}
                              </small>
                            )}
                          </div>
                        </div>
                        <button
                          type="button"
                          onClick={() => onGoogleAnalyticsSelect(resource.id)}
                          disabled={!allowed || busy || resource.available === false}
                        >
                          <Link2 size={17} aria-hidden /> Select
                        </button>
                      </div>
                    ))}
                  </div>
                )}

                {isGoogleAnalytics && googleAnalyticsConnections.length > 0 && (
                  <div className="resource-list" aria-label="Connected GA4 properties">
                    <h3>Connected properties</h3>
                    {googleAnalyticsConnections.map((connection) => (
                      <div key={connection.id || connection.account?.id} className="resource-row connection-resource">
                        <div className="channel-identity compact">
                          <span className="channel-placeholder" aria-hidden>
                            <BarChart3 size={18} />
                          </span>
                          <div>
                            <strong>
                              {connection.account?.display_name || connection.account?.id || 'GA4 property'}
                            </strong>
                            <small>
                              {connection.account?.account_name || connection.account?.id} ·{' '}
                              {connection.account?.timezone || 'Timezone unavailable'} ·{' '}
                              {connection.account?.currency || 'Currency unavailable'}
                            </small>
                            <small>
                              Last sync {formatDate(connection.last_successful_sync_at)}; data through{' '}
                              {formatDate(connection.data_through_at, { dateStyle: 'medium' })}
                            </small>
                            {connection.reconnect_reason && (
                              <small className="notice error">
                                Authorize this property again before synchronization can resume.
                              </small>
                            )}
                            <div className="capability-list" aria-label="Property capabilities">
                              {(connection.capabilities || []).map((capability) => (
                                <span
                                  key={capability.key}
                                  className={capability.status === 'available' ? '' : 'delayed'}
                                >
                                  {capability.key.replaceAll('_', ' ')}: {capability.status}
                                </span>
                              ))}
                            </div>
                          </div>
                        </div>
                        <div className="button-row">
                          <StatusBadge status={connection.status} />
                          <button
                            type="button"
                            className="ghost-button"
                            disabled={!roleCanSync(role) || connection.status !== 'active' || busy}
                            onClick={() => onGoogleAnalyticsSync(connection.id)}
                          >
                            <RefreshCw size={17} aria-hidden /> Sync
                          </button>
                          <button
                            type="button"
                            disabled={!canConnect || busy}
                            onClick={() => onGoogleAnalyticsConnect(connection.id)}
                          >
                            <ExternalLink size={17} aria-hidden /> Reauthorize
                          </button>
                          <button
                            type="button"
                            className="ghost-button"
                            disabled={!allowed || busy}
                            onClick={() =>
                              onDisconnectRequest({
                                provider: 'google-analytics',
                                connectionId: connection.id,
                                label: connection.account?.display_name || 'GA4 property'
                              })
                            }
                          >
                            <Unplug size={17} aria-hidden /> Disconnect
                          </button>
                        </div>
                      </div>
                    ))}
                  </div>
                )}

                {isMeta && unselectedResources.length > 0 && (
                  <div className="resource-list" aria-label={`${provider.name} resources available to connect`}>
                    <h3>Available {isFacebook ? 'Pages' : 'professional accounts'}</h3>
                    {unselectedResources.map((resource) => {
                      const ResourceIcon = isFacebook ? Facebook : Instagram;
                      return (
                        <div key={resource.id} className="resource-row">
                          <div className="channel-identity compact">
                            {resource.thumbnail_url ? (
                              <img src={resource.thumbnail_url} alt="" referrerPolicy="no-referrer" />
                            ) : (
                              <span className="channel-placeholder" aria-hidden>
                                <ResourceIcon size={18} />
                              </span>
                            )}
                            <div>
                              <strong>{resource.display_name}</strong>
                              <small>
                                {resource.username ? `@${resource.username}` : resource.provider_resource_id}
                              </small>
                              {resource.source_page_name && <small>Linked Page: {resource.source_page_name}</small>}
                              {resource.available === false && (
                                <small className="notice error">
                                  Unavailable:{' '}
                                  {(resource.unavailable_reason || 'authorization_required').replaceAll('_', ' ')}
                                </small>
                              )}
                            </div>
                          </div>
                          <button
                            type="button"
                            onClick={() => onMetaSelect(metaPath, resource.id)}
                            disabled={!allowed || busy || resource.available === false}
                          >
                            <Link2 size={17} aria-hidden /> Select
                          </button>
                        </div>
                      );
                    })}
                  </div>
                )}

                {isMeta && metaConnections.length > 0 && (
                  <div className="resource-list" aria-label={`Connected ${provider.name} resources`}>
                    <h3>Connected resources</h3>
                    {metaConnections.map((connection) => {
                      const ResourceIcon = isFacebook ? Facebook : Instagram;
                      return (
                        <div key={connection.id || connection.account?.id} className="resource-row connection-resource">
                          <div className="channel-identity compact">
                            {connection.account?.thumbnail_url ? (
                              <img src={connection.account.thumbnail_url} alt="" referrerPolicy="no-referrer" />
                            ) : (
                              <span className="channel-placeholder" aria-hidden>
                                <ResourceIcon size={18} />
                              </span>
                            )}
                            <div>
                              <strong>
                                {connection.account?.display_name || connection.account?.id || provider.name}
                              </strong>
                              <small>
                                Last sync {formatDate(connection.last_successful_sync_at)}; data through{' '}
                                {formatDate(connection.data_through_at, { dateStyle: 'medium' })}
                              </small>
                              {connection.reconnect_reason && (
                                <small className="notice error">Authorize this account again to resume syncing.</small>
                              )}
                              <div className="capability-list" aria-label="Resource capabilities">
                                {(connection.capabilities || []).map((capability) => (
                                  <span
                                    key={capability.key}
                                    className={capability.status === 'available' ? '' : 'delayed'}
                                  >
                                    {capability.key.replaceAll('_', ' ')}: {capability.status}
                                  </span>
                                ))}
                              </div>
                            </div>
                          </div>
                          <div className="button-row">
                            <StatusBadge status={connection.status} />
                            <button
                              type="button"
                              className="ghost-button"
                              disabled={!roleCanSync(role) || connection.status !== 'active' || busy}
                              onClick={() => onMetaSync(provider.id as 'facebook_pages' | 'instagram', connection.id)}
                            >
                              <RefreshCw size={17} aria-hidden /> Sync
                            </button>
                            <button
                              type="button"
                              disabled={!canConnect || busy}
                              onClick={() => onMetaConnect(metaPath, connection.id)}
                            >
                              <ExternalLink size={17} aria-hidden /> Reauthorize
                            </button>
                            <button
                              type="button"
                              className="ghost-button"
                              disabled={!allowed || busy}
                              onClick={() =>
                                onDisconnectRequest({
                                  provider: metaPath,
                                  connectionId: connection.id,
                                  label: connection.account?.display_name || provider.name
                                })
                              }
                            >
                              <Unplug size={17} aria-hidden /> Disconnect
                            </button>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>
              <div className="button-row">
                {isTikTok ? (
                  <>
                    <button type="button" disabled={!canConnect || busy} onClick={onTikTokConnect}>
                      <ExternalLink size={18} aria-hidden />{' '}
                      {provider.connection?.status === 'disconnected' ? 'Connect' : 'Reconnect'}
                    </button>
                    <button
                      type="button"
                      disabled={!canDisconnect || busy}
                      onClick={() => onDisconnectRequest({ provider: 'tiktok', label: 'TikTok' })}
                    >
                      <Unplug size={18} aria-hidden /> Disconnect
                    </button>
                  </>
                ) : isYouTube ? (
                  <>
                    <button type="button" disabled={!canStartYouTube || busy} onClick={() => onYouTubeConnect()}>
                      <ExternalLink size={18} aria-hidden />{' '}
                      {provider.status === 'missing_scopes' ||
                      provider.status === 'authorization_denied' ||
                      provider.status === 'provider_error'
                        ? 'Authorize again'
                        : provider.status === 'authorizing'
                          ? 'Restart authorization'
                          : 'Connect YouTube'}
                    </button>
                    <button
                      type="button"
                      disabled={
                        !allowed || !provider.authorization || provider.authorization.status === 'revoked' || busy
                      }
                      onClick={() => onDisconnectRequest({ provider: 'youtube', label: 'YouTube' })}
                    >
                      <Unplug size={18} aria-hidden /> Disconnect
                    </button>
                  </>
                ) : isGoogleAnalytics ? (
                  <>
                    <button
                      type="button"
                      disabled={!canStartGoogleAnalytics || busy}
                      onClick={() => onGoogleAnalyticsConnect()}
                    >
                      <ExternalLink size={18} aria-hidden />{' '}
                      {provider.status === 'missing_scopes' ||
                      provider.status === 'authorization_denied' ||
                      provider.status === 'provider_error'
                        ? 'Authorize again'
                        : provider.status === 'authorizing'
                          ? 'Restart authorization'
                          : provider.authorization
                            ? 'Refresh discovery'
                            : 'Connect Website Analytics'}
                    </button>
                    <button
                      type="button"
                      disabled={
                        !allowed || !provider.authorization || provider.authorization.status === 'revoked' || busy
                      }
                      onClick={() => onDisconnectRequest({ provider: 'google-analytics', label: 'Website Analytics' })}
                    >
                      <Unplug size={18} aria-hidden /> Disconnect all
                    </button>
                  </>
                ) : isMeta ? (
                  <>
                    <button type="button" disabled={!canStartMeta || busy} onClick={() => onMetaConnect(metaPath)}>
                      <ExternalLink size={18} aria-hidden />{' '}
                      {provider.status === 'missing_scopes' ||
                      provider.status === 'authorization_denied' ||
                      provider.status === 'provider_error' ||
                      provider.status === 'reconnect_required'
                        ? 'Authorize again'
                        : provider.authorization
                          ? 'Refresh discovery'
                          : `Connect ${isFacebook ? 'Facebook' : 'Instagram'}`}
                    </button>
                    <button
                      type="button"
                      disabled={
                        !allowed || !provider.authorization || provider.authorization.status === 'revoked' || busy
                      }
                      onClick={() => onDisconnectRequest({ provider: metaPath, label: provider.name })}
                    >
                      <Unplug size={18} aria-hidden /> Disconnect all
                    </button>
                  </>
                ) : (
                  <button type="button" disabled>
                    <Lock size={18} aria-hidden /> Not enabled
                  </button>
                )}
              </div>
            </article>
          );
        })}
        {catalog.length === 0 && <div className="table-empty">Connections are temporarily unavailable.</div>}
      </div>
      {!allowed && <p className="muted">Connection management requires owner or admin access.</p>}
      {disconnectTarget && (
        <div className="dialog-backdrop" role="presentation">
          <section className="dialog" role="dialog" aria-modal="true" aria-labelledby="disconnect-title">
            <h3 id="disconnect-title">Disconnect {disconnectTarget.label}?</h3>
            {disconnectTarget.provider === 'youtube' ? (
              <p>
                Social Insights Studio will ask Google to revoke access, then remove this YouTube connection and its
                stored analytics data from the workspace.
              </p>
            ) : disconnectTarget.provider === 'google-analytics' ? (
              <p>
                Removing one of several selected properties preserves the shared read-only Google grant. Removing the
                final property asks Google to revoke access and deletes the locally stored GA4 observations.
              </p>
            ) : disconnectTarget.provider === 'facebook' || disconnectTarget.provider === 'instagram' ? (
              <p>
                Facebook and Instagram accounts selected through the same Meta sign-in can be disconnected separately.
                Removing the final connected account also asks Meta to revoke access and removes its stored analytics
                data.
              </p>
            ) : (
              <p>
                Social Insights Studio will ask the provider to revoke access and disable this connection. Previously
                synchronized analytics remain in the workspace until they are deleted under the retention policy.
              </p>
            )}
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
  onResendInvitation,
  onRevokeInvitation,
  onRoleChange,
  onRemove
}: {
  role: Role;
  members: Member[];
  invitations: Invitation[];
  busy: boolean;
  onInvite: (email: string, role: Exclude<Role, 'owner'>) => void;
  onResendInvitation: (invitation: Invitation) => void;
  onRevokeInvitation: (invitation: Invitation) => void;
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
          <p>Invite teammates and choose the access each person needs.</p>
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
                    disabled={!canManage || busy || (member.role === 'owner' && role !== 'owner')}
                    onChange={(event) => onRoleChange(member, event.target.value as Role)}
                  >
                    {(role === 'owner' || member.role === 'owner') && <option value="owner">Owner</option>}
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
                    disabled={!canManage || busy || (member.role === 'owner' && role !== 'owner')}
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
      <div className="invitation-list">
        {invitations.map((invitation) => {
          const status = invitationStatus(invitation);
          const pending = status === 'pending' || status === 'expired';
          return (
            <article className="invitation-row" key={invitation.id}>
              <div>
                <strong>{invitation.email}</strong>
                <p>
                  {invitation.role} · sent {formatDate(invitation.last_sent_at || invitation.created_at)} ·{' '}
                  {invitation.send_count} attempt{invitation.send_count === 1 ? '' : 's'}
                </p>
              </div>
              <StatusBadge status={status} />
              <div className="button-row">
                <button
                  type="button"
                  className="ghost-button"
                  disabled={!canManage || busy || !pending}
                  onClick={() => onResendInvitation(invitation)}
                >
                  Resend
                </button>
                <button
                  type="button"
                  className="ghost-button"
                  disabled={!canManage || busy || !pending}
                  onClick={() => onRevokeInvitation(invitation)}
                >
                  Revoke
                </button>
              </div>
            </article>
          );
        })}
        {invitations.length === 0 && <div className="table-empty compact-empty">No invitations yet.</div>}
      </div>
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
                  <th scope="col">Started by</th>
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
                      <td data-label="Started by">{run.trigger_type === 'manual' ? 'A team member' : 'Schedule'}</td>
                      <td data-label="Status">
                        <StatusBadge status={run.status} />
                      </td>
                      <td data-label="Duration">{formatDuration(run.duration_ms)}</td>
                      <td data-label="Counts">
                        {formatNumber(run.profile_count)} profiles · {formatNumber(run.content_seen_count)} seen ·{' '}
                        {formatNumber(run.content_snapshot_count)} performance updates
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
                          Attempt {run.attempt || 1}.{' '}
                          {run.error_category
                            ? 'The provider did not complete this sync.'
                            : 'No failure was reported for this sync.'}{' '}
                          {run.retryable ? 'It will be retried automatically.' : ''}
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

function Account({
  user,
  account,
  workspaces,
  busy,
  onSaveProfile,
  onRevokeSession,
  onRevokeOthers,
  onRevokeAll,
  onRequestAccountDeletion,
  onRequestWorkspaceDeletion,
  onSignOut
}: {
  user: User;
  account: AccountData | null;
  workspaces: Workspace[];
  busy: boolean;
  onSaveProfile: (displayName: string) => void;
  onRevokeSession: (session: AccountSession) => void;
  onRevokeOthers: () => void;
  onRevokeAll: () => void;
  onRequestAccountDeletion: (confirmation: string) => void;
  onRequestWorkspaceDeletion: (workspace: Workspace, confirmation: string) => void;
  onSignOut: () => void;
}) {
  const [displayName, setDisplayName] = useState(user.display_name || '');
  const [accountConfirmation, setAccountConfirmation] = useState('');
  const ownerWorkspaces = workspaces.filter((workspace) => workspace.role === 'owner');
  const [deletionWorkspaceId, setDeletionWorkspaceId] = useState(ownerWorkspaces[0]?.id || '');
  const [workspaceConfirmation, setWorkspaceConfirmation] = useState('');
  const effectiveDeletionWorkspaceId = ownerWorkspaces.some((workspace) => workspace.id === deletionWorkspaceId)
    ? deletionWorkspaceId
    : ownerWorkspaces[0]?.id || '';
  const deletionWorkspace = ownerWorkspaces.find((workspace) => workspace.id === effectiveDeletionWorkspaceId);

  return (
    <div className="account-layout">
      <section className="panel" aria-labelledby="account-title">
        <div className="panel-title between">
          <div>
            <h2 id="account-title">Profile</h2>
            <p>Choose how your name appears to workspace members.</p>
          </div>
        </div>
        <form
          className="account-form"
          onSubmit={(event) => {
            event.preventDefault();
            onSaveProfile(displayName);
          }}
        >
          <label>
            Display name
            <input
              value={displayName}
              onChange={(event) => setDisplayName(event.target.value)}
              autoComplete="name"
              maxLength={100}
            />
          </label>
          <label>
            Email address
            <input value={user.email} readOnly aria-describedby="email-help" />
          </label>
          <p id="email-help" className="muted">
            Contact support if your sign-in email needs to change.
          </p>
          <div className="button-row start">
            <button type="submit" disabled={busy || displayName === (user.display_name || '')}>
              Save profile
            </button>
            <button type="button" onClick={onSignOut} disabled={busy}>
              <LogOut size={18} aria-hidden /> Sign out
            </button>
          </div>
        </form>
        {account && (
          <div>
            <h3>Sign-in methods</h3>
            <div className="settings-list">
              {account.authentication_methods.map((method) => (
                <span key={`${method.provider}-${method.connected_at}`}>
                  {method.provider === 'google' ? 'Google' : 'Email code'}
                  {method.email ? ` · ${method.email}` : ''}
                </span>
              ))}
            </div>
          </div>
        )}
      </section>

      <section className="panel" aria-labelledby="sessions-title">
        <div className="panel-title between">
          <div>
            <h2 id="sessions-title">Active sessions</h2>
            <p>Review devices signed in to your account. Approximate locations are not collected.</p>
          </div>
          <button type="button" onClick={onRevokeOthers} disabled={busy || !account || account.sessions.length < 2}>
            Sign out other sessions
          </button>
        </div>
        <div className="session-list">
          {account?.sessions.map((session) => (
            <article className="session-row" key={session.id}>
              <div>
                <strong>
                  {session.device_label} {session.current && <span className="current-label">Current</span>}
                </strong>
                <p>
                  Last active {formatDate(session.last_seen_at)} · signed in {formatDate(session.created_at)}
                </p>
              </div>
              <button type="button" className="ghost-button" disabled={busy} onClick={() => onRevokeSession(session)}>
                {session.current ? 'Sign out this session' : 'Sign out'}
              </button>
            </article>
          ))}
          {!account && <div className="table-empty compact-empty">Loading sessions…</div>}
        </div>
        <button type="button" className="danger account-danger-button" onClick={onRevokeAll} disabled={busy}>
          Sign out everywhere
        </button>
      </section>

      <section className="panel" aria-labelledby="deletion-title">
        <div className="panel-title between">
          <div>
            <h2 id="deletion-title">Deletion requests</h2>
            <p>Requests are verified and reviewed before data is permanently removed.</p>
          </div>
        </div>
        {account?.deletion_requests.length ? (
          <div className="deletion-request-list">
            {account.deletion_requests.map((request) => (
              <article key={request.id}>
                <span>
                  {request.scope === 'workspace' ? request.workspace_name || 'Workspace' : 'Account'} ·{' '}
                  {formatDate(request.requested_at)}
                </span>
                <StatusBadge status={request.status} />
              </article>
            ))}
          </div>
        ) : (
          <p className="muted">No deletion requests.</p>
        )}

        <details className="danger-zone">
          <summary>Request account deletion</summary>
          <div>
            <p>
              This starts a reviewed deletion request for your account and associated personal data. Enter your full
              email address to confirm.
            </p>
            <label>
              Email confirmation
              <input
                value={accountConfirmation}
                onChange={(event) => setAccountConfirmation(event.target.value)}
                autoComplete="off"
              />
            </label>
            <button
              type="button"
              className="danger"
              disabled={busy || accountConfirmation.trim().toLowerCase() !== user.email.toLowerCase()}
              onClick={() => onRequestAccountDeletion(accountConfirmation)}
            >
              Submit account deletion request
            </button>
          </div>
        </details>

        {ownerWorkspaces.length > 0 && (
          <details className="danger-zone">
            <summary>Request workspace deletion</summary>
            <div>
              <p>Only workspace owners can request deletion. Enter the exact workspace name to confirm.</p>
              <label>
                Workspace
                <select
                  value={effectiveDeletionWorkspaceId}
                  onChange={(event) => {
                    setDeletionWorkspaceId(event.target.value);
                    setWorkspaceConfirmation('');
                  }}
                >
                  {ownerWorkspaces.map((workspace) => (
                    <option key={workspace.id} value={workspace.id}>
                      {workspace.name}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                Workspace name confirmation
                <input
                  value={workspaceConfirmation}
                  onChange={(event) => setWorkspaceConfirmation(event.target.value)}
                  autoComplete="off"
                />
              </label>
              <button
                type="button"
                className="danger"
                disabled={busy || !deletionWorkspace || workspaceConfirmation.trim() !== deletionWorkspace.name}
                onClick={() =>
                  deletionWorkspace && onRequestWorkspaceDeletion(deletionWorkspace, workspaceConfirmation)
                }
              >
                Submit workspace deletion request
              </button>
            </div>
          </details>
        )}
      </section>
    </div>
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
