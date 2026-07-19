const fs = require('fs');
const PDFDocument = require('pdfkit');

const PAGE = Object.freeze({ width: 595.28, height: 841.89, left: 48, right: 48, top: 48, bottom: 58 });
const COLORS = Object.freeze({
  ink: '#17202A',
  muted: '#64748B',
  line: '#D8E0E8',
  panel: '#F5F7FA',
  accent: '#6D28D9',
  accentSoft: '#EDE9FE',
  good: '#0F766E',
  warning: '#B45309',
  danger: '#B91C1C',
  white: '#FFFFFF'
});

const PROVIDER_COLORS = Object.freeze({
  tiktok: '#111827',
  youtube: '#DC2626',
  facebook_pages: '#2563EB',
  instagram: '#C026D3',
  google_analytics_4: '#D97706'
});

class ReportRenderError extends Error {
  constructor(code) {
    super(code);
    this.code = code;
  }
}

function safePdfText(value, fallback = 'N/A') {
  if (value === null || value === undefined || value === '') return fallback;
  const source = String(value)
    .replace(/[\u2010-\u2015]/g, '-')
    .replace(/[\u2018\u2019]/g, "'")
    .replace(/[\u201C\u201D]/g, '"')
    .replace(/(?:https?|file|ftp):\/\/\S+/gi, '[link removed]')
    .split('')
    .map(character => {
      const code = character.charCodeAt(0);
      return code <= 31 || code === 127 ? ' ' : character;
    })
    .join('')
    .normalize('NFKD')
    .replace(/[\u0300-\u036F]/g, '')
    .replace(/[^\x20-\x7E]/g, '?')
    .replace(/\s+/g, ' ')
    .trim();
  return source || fallback;
}

function dateText(value) {
  const match = String(value || '').match(/^\d{4}-\d{2}-\d{2}/);
  return match ? match[0] : 'Not available';
}

function formatNumber(value, unit) {
  if (value === null || value === undefined || value === '' || !Number.isFinite(Number(value))) {
    return 'Not available';
  }
  const numeric = Number(value);
  if (unit === 'ratio') return `${(numeric * 100).toFixed(1)}%`;
  if (unit === 'percent') return `${numeric.toFixed(1)}%`;
  if (unit === 'minutes') return `${new Intl.NumberFormat('en-US', { maximumFractionDigits: 1 }).format(numeric)} min`;
  if (unit === 'seconds') return `${new Intl.NumberFormat('en-US', { maximumFractionDigits: 1 }).format(numeric)} sec`;
  return new Intl.NumberFormat('en-US', { maximumFractionDigits: 2 }).format(numeric);
}

function comparisonText(metric) {
  if (!metric || metric.percent_change === null || metric.percent_change === undefined) return 'No comparable baseline';
  const value = Number(metric.percent_change);
  if (!Number.isFinite(value)) return 'No comparable baseline';
  return `${value > 0 ? '+' : ''}${value.toFixed(1)}% vs previous period`;
}

function statusLabel(source) {
  const state = source && source.freshness && source.freshness.state || source && source.status;
  const labels = {
    ready: 'Ready',
    sample: 'Sample data',
    stale: 'Stale',
    partial: 'Partial',
    delayed: 'Delayed',
    thresholded: 'Thresholded',
    empty: 'No stored data',
    reconnect_required: 'Reconnect required',
    failed: 'Latest sync failed',
    pending: 'Setup pending',
    active: 'Connected'
  };
  return labels[state] || safePdfText(state, 'Unavailable');
}

function renderReportPdf({ snapshot, outputPath, limits = {}, now = new Date() }) {
  const maxPages = Number(limits.maxPages || 80);
  const maxContentRowsPerResource = Number(limits.maxContentRowsPerResource || 30);
  const deadline = Number(limits.deadlineMs || 0);
  const report = snapshot && snapshot.report || {};
  const dashboard = snapshot && snapshot.dashboard || {};
  const sources = Array.isArray(dashboard.sources) ? dashboard.sources : [];
  const selectedSections = new Set(Array.isArray(report.sections) ? report.sections : [
    'executive_summary',
    'cross_platform_summary',
    'resource_sections',
    'methodology'
  ]);
  let pageCount = 0;
  let currentTitle = '';
  let currentKicker = '';
  let settled = false;

  return new Promise((resolve, reject) => {
    const output = fs.createWriteStream(outputPath, { flags: 'wx', mode: 0o600 });
    const doc = new PDFDocument({
      autoFirstPage: false,
      bufferPages: true,
      compress: true,
      size: 'A4',
      margins: { top: PAGE.top, right: PAGE.right, bottom: PAGE.bottom, left: PAGE.left },
      info: {
        Title: safePdfText(report.title, 'Analytics report'),
        Author: 'Social Insights Studio',
        Subject: 'Stored analytics report',
        Creator: 'Social Insights Studio report worker',
        CreationDate: now
      }
    });

    const finishWithError = error => {
      if (settled) return;
      settled = true;
      output.destroy();
      reject(error);
    };
    output.on('error', finishWithError);
    doc.on('error', finishWithError);
    output.on('finish', () => {
      if (settled) return;
      settled = true;
      resolve({ pageCount });
    });
    doc.pipe(output);

    function checkBudget() {
      if (deadline && Date.now() > deadline) throw new ReportRenderError('report_render_time_limit_exceeded');
    }

    function addPage(title, kicker = 'SOCIAL INSIGHTS STUDIO') {
      checkBudget();
      if (pageCount >= maxPages) throw new ReportRenderError('report_page_limit_exceeded');
      doc.addPage();
      pageCount += 1;
      currentTitle = title;
      currentKicker = kicker;
      doc.fillColor(COLORS.accent).font('Helvetica-Bold').fontSize(8).text(safePdfText(kicker), PAGE.left, 36, {
        width: PAGE.width - PAGE.left - PAGE.right,
        characterSpacing: 1.1
      });
      doc.fillColor(COLORS.ink).font('Helvetica-Bold').fontSize(19).text(safePdfText(title), PAGE.left, 53, {
        width: PAGE.width - PAGE.left - PAGE.right
      });
      doc.moveTo(PAGE.left, 82).lineTo(PAGE.width - PAGE.right, 82).lineWidth(1).strokeColor(COLORS.line).stroke();
      doc.y = 96;
    }

    function addContinuation() {
      addPage(`${currentTitle.replace(/ \(continued\)$/i, '')} (continued)`, currentKicker);
    }

    function ensureSpace(height, onContinuation) {
      checkBudget();
      if (doc.y + height <= PAGE.height - PAGE.bottom - 8) return;
      addContinuation();
      if (onContinuation) onContinuation();
    }

    function paragraph(text, options = {}) {
      const value = safePdfText(text, options.fallback || 'Not available');
      const width = options.width || PAGE.width - PAGE.left - PAGE.right;
      const fontSize = options.fontSize || 9.5;
      doc.font(options.bold ? 'Helvetica-Bold' : 'Helvetica').fontSize(fontSize);
      const height = doc.heightOfString(value, { width, lineGap: options.lineGap || 2 });
      ensureSpace(height + (options.after === undefined ? 8 : options.after));
      doc.fillColor(options.color || COLORS.ink).text(value, options.x || PAGE.left, doc.y, {
        width,
        lineGap: options.lineGap || 2,
        align: options.align || 'left'
      });
      doc.y += options.after === undefined ? 8 : options.after;
    }

    function sectionHeading(text) {
      ensureSpace(30);
      doc.fillColor(COLORS.ink).font('Helvetica-Bold').fontSize(13).text(safePdfText(text), PAGE.left, doc.y, {
        width: PAGE.width - PAGE.left - PAGE.right
      });
      doc.y += 8;
    }

    function labelValue(label, value, options = {}) {
      ensureSpace(25);
      const x = options.x || PAGE.left;
      const width = options.width || PAGE.width - PAGE.left - PAGE.right;
      doc.fillColor(COLORS.muted).font('Helvetica-Bold').fontSize(7.5).text(safePdfText(label).toUpperCase(), x, doc.y, { width });
      doc.fillColor(COLORS.ink).font('Helvetica').fontSize(9.5).text(safePdfText(value, 'Not available'), x, doc.y + 11, { width });
      doc.y += 29;
    }

    function metricCards(metrics) {
      const rows = Array.isArray(metrics) ? metrics : [];
      const cardWidth = (PAGE.width - PAGE.left - PAGE.right - 12) / 2;
      for (let index = 0; index < rows.length; index += 2) {
        ensureSpace(78);
        const y = doc.y;
        for (let offset = 0; offset < 2; offset += 1) {
          const metric = rows[index + offset];
          if (!metric) continue;
          const x = PAGE.left + offset * (cardWidth + 12);
          doc.roundedRect(x, y, cardWidth, 68, 6).fillAndStroke(COLORS.panel, COLORS.line);
          doc.fillColor(COLORS.muted).font('Helvetica-Bold').fontSize(8).text(safePdfText(metric.label, metric.key), x + 11, y + 10, {
            width: cardWidth - 22
          });
          const available = metric.available !== false && metric.value !== null && metric.value !== undefined;
          doc.fillColor(available ? COLORS.ink : COLORS.muted).font('Helvetica-Bold').fontSize(16).text(
            available ? formatNumber(metric.value, metric.unit) : 'Unavailable',
            x + 11,
            y + 27,
            { width: cardWidth - 22 }
          );
          doc.fillColor(COLORS.muted).font('Helvetica').fontSize(7.5).text(
            available ? comparisonText(metric) : safePdfText(metric.availability_reason, 'Not reported by provider'),
            x + 11,
            y + 50,
            { width: cardWidth - 22, ellipsis: true }
          );
        }
        doc.y = y + 78;
      }
    }

    function sourceTable(rows) {
      const entries = Array.isArray(rows) ? rows : [];
      const columns = [PAGE.left, 190, 337, 442];
      const widths = [134, 139, 97, 105];
      const drawHeader = () => {
        const y = doc.y;
        doc.rect(PAGE.left, y, PAGE.width - PAGE.left - PAGE.right, 22).fill(COLORS.accentSoft);
        ['Source', 'Resource', 'Status', 'Data through'].forEach((label, index) => {
          doc.fillColor(COLORS.ink).font('Helvetica-Bold').fontSize(7.5).text(label, columns[index] + 6, y + 7, { width: widths[index] - 12 });
        });
        doc.y = y + 26;
      };
      drawHeader();
      for (const source of entries) {
        ensureSpace(35, drawHeader);
        const y = doc.y;
        const values = [
          source.provider_name,
          source.resource && source.resource.display_name,
          statusLabel(source),
          source.freshness && source.freshness.data_through_date
        ];
        values.forEach((value, index) => {
          doc.fillColor(COLORS.ink).font('Helvetica').fontSize(8).text(safePdfText(value, 'Not available'), columns[index] + 6, y + 3, {
            width: widths[index] - 12,
            height: 27,
            ellipsis: true
          });
        });
        doc.moveTo(PAGE.left, y + 32).lineTo(PAGE.width - PAGE.right, y + 32).strokeColor(COLORS.line).lineWidth(0.5).stroke();
        doc.y = y + 35;
      }
    }

    function trendChart(source) {
      const trend = source && source.trend || {};
      const series = Array.isArray(trend.series) ? trend.series : [];
      const points = Array.isArray(trend.points) ? trend.points : [];
      const selected = series.find(item => points.some(point => Number.isFinite(Number(point.values && point.values[item.key]))));
      if (!selected) {
        paragraph('No stored daily trend is available for this resource and range.', { color: COLORS.muted });
        return;
      }
      const values = points.map(point => Number(point.values && point.values[selected.key])).filter(Number.isFinite);
      if (values.length < 2) {
        paragraph('Not enough stored observations are available to draw a trend.', { color: COLORS.muted });
        return;
      }
      ensureSpace(145);
      const x = PAGE.left;
      const y = doc.y + 6;
      const width = PAGE.width - PAGE.left - PAGE.right;
      const height = 105;
      const minimum = Math.min(...values);
      const maximum = Math.max(...values);
      const span = Math.max(maximum - minimum, 1);
      doc.roundedRect(x, y, width, height, 5).fillAndStroke(COLORS.panel, COLORS.line);
      doc.fillColor(COLORS.muted).font('Helvetica-Bold').fontSize(7.5).text(safePdfText(selected.label), x + 10, y + 8, { width: width - 20 });
      const plot = { x: x + 12, y: y + 27, width: width - 24, height: 58 };
      const usable = points.filter(point => Number.isFinite(Number(point.values && point.values[selected.key])));
      doc.strokeColor(PROVIDER_COLORS[source.provider] || COLORS.accent).lineWidth(2);
      usable.forEach((point, index) => {
        const value = Number(point.values[selected.key]);
        const px = plot.x + (index / Math.max(usable.length - 1, 1)) * plot.width;
        const py = plot.y + plot.height - ((value - minimum) / span) * plot.height;
        if (index === 0) doc.moveTo(px, py);
        else doc.lineTo(px, py);
      });
      doc.stroke();
      doc.fillColor(COLORS.muted).font('Helvetica').fontSize(7).text(dateText(usable[0].date), plot.x, y + 89, { width: 100 });
      doc.text(dateText(usable[usable.length - 1].date), x + width - 112, y + 89, { width: 100, align: 'right' });
      doc.y = y + height + 12;
    }

    function contentTable(source) {
      const rows = (Array.isArray(source.top_content) ? source.top_content : []).slice(0, maxContentRowsPerResource);
      if (rows.length === 0) {
        paragraph('No eligible stored content or website paths were available for this resource and range.', { color: COLORS.muted });
        return;
      }
      const drawHeader = () => {
        const y = doc.y;
        doc.rect(PAGE.left, y, PAGE.width - PAGE.left - PAGE.right, 22).fill(COLORS.accentSoft);
        doc.fillColor(COLORS.ink).font('Helvetica-Bold').fontSize(7.5).text('Title or path', PAGE.left + 6, y + 7, { width: 330 });
        doc.text('Published', 390, y + 7, { width: 82 });
        doc.text('Primary metric', 475, y + 7, { width: 67, align: 'right' });
        doc.y = y + 25;
      };
      drawHeader();
      for (const row of rows) {
        const title = safePdfText(row.title, 'Untitled content');
        doc.font('Helvetica').fontSize(8);
        const rowHeight = Math.max(30, Math.min(52, doc.heightOfString(title, { width: 330 }) + 12));
        ensureSpace(rowHeight + 2, drawHeader);
        const y = doc.y;
        doc.fillColor(COLORS.ink).text(title, PAGE.left + 6, y + 4, { width: 330, height: rowHeight - 8, ellipsis: true });
        doc.fillColor(COLORS.muted).text(row.published_at ? dateText(row.published_at) : 'Not applicable', 390, y + 4, { width: 82 });
        const metric = row.primary_metric || {};
        doc.fillColor(COLORS.ink).font('Helvetica-Bold').text(formatNumber(metric.value, metric.unit), 475, y + 4, { width: 67, align: 'right' });
        doc.moveTo(PAGE.left, y + rowHeight).lineTo(PAGE.width - PAGE.right, y + rowHeight).strokeColor(COLORS.line).lineWidth(0.5).stroke();
        doc.y = y + rowHeight + 2;
      }
    }

    function cover() {
      if (pageCount >= maxPages) throw new ReportRenderError('report_page_limit_exceeded');
      doc.addPage();
      pageCount += 1;
      doc.rect(0, 0, PAGE.width, PAGE.height).fill('#111827');
      doc.roundedRect(PAGE.left, 52, 44, 44, 9).fill(COLORS.accent);
      doc.fillColor(COLORS.white).font('Helvetica-Bold').fontSize(22).text('SI', PAGE.left, 64, { width: 44, align: 'center' });
      doc.fillColor('#C4B5FD').font('Helvetica-Bold').fontSize(9).text('SOCIAL INSIGHTS STUDIO', PAGE.left, 120, {
        width: PAGE.width - PAGE.left - PAGE.right,
        characterSpacing: 1.5
      });
      const title = safePdfText(report.title, 'Analytics report');
      const titleFont = title.length > 130 ? 26 : title.length > 80 ? 31 : 38;
      doc.fillColor(COLORS.white).font('Helvetica-Bold').fontSize(titleFont).text(title, PAGE.left, 158, {
        width: PAGE.width - PAGE.left - PAGE.right,
        height: 205,
        ellipsis: true,
        lineGap: 4
      });
      if (report.subtitle) {
        doc.fillColor('#CBD5E1').font('Helvetica').fontSize(13).text(safePdfText(report.subtitle), PAGE.left, 378, {
          width: PAGE.width - PAGE.left - PAGE.right,
          height: 78,
          ellipsis: true,
          lineGap: 3
        });
      }
      doc.roundedRect(PAGE.left, 520, PAGE.width - PAGE.left - PAGE.right, 166, 10).fill('#1F2937');
      const leftWidth = 210;
      doc.fillColor('#94A3B8').font('Helvetica-Bold').fontSize(8).text('REPORTING PERIOD', PAGE.left + 18, 542, { width: leftWidth });
      doc.fillColor(COLORS.white).font('Helvetica').fontSize(12).text(
        `${dateText(report.range && report.range.from)} to ${dateText(report.range && report.range.to)}`,
        PAGE.left + 18,
        558,
        { width: leftWidth }
      );
      doc.fillColor('#94A3B8').font('Helvetica-Bold').fontSize(8).text('REPORT TIMEZONE', PAGE.left + 18, 602, { width: leftWidth });
      doc.fillColor(COLORS.white).font('Helvetica').fontSize(11).text(safePdfText(report.timezone, 'UTC'), PAGE.left + 18, 618, { width: leftWidth });
      doc.fillColor('#94A3B8').font('Helvetica-Bold').fontSize(8).text('RESOURCES', PAGE.left + 280, 542, { width: 180 });
      doc.fillColor(COLORS.white).font('Helvetica-Bold').fontSize(28).text(String(sources.length), PAGE.left + 280, 558, { width: 180 });
      doc.fillColor('#94A3B8').font('Helvetica-Bold').fontSize(8).text('GENERATED', PAGE.left + 280, 602, { width: 180 });
      doc.fillColor(COLORS.white).font('Helvetica').fontSize(11).text(dateText(now.toISOString()), PAGE.left + 280, 618, { width: 180 });
      doc.fillColor('#94A3B8').font('Helvetica').fontSize(8).text(
        'Read-only analytics from stored observations. Provider metrics retain their own definitions and are not summed.',
        PAGE.left,
        760,
        { width: PAGE.width - PAGE.left - PAGE.right }
      );
    }

    function executiveSummary() {
      addPage('Executive summary');
      paragraph(
        `${sources.length} selected resource${sources.length === 1 ? '' : 's'} across ${new Set(sources.map(source => source.provider)).size} provider${new Set(sources.map(source => source.provider)).size === 1 ? '' : 's'}. Metrics are shown independently using each provider's reporting semantics.`
      );
      const ready = sources.filter(source => ['ready', 'sample'].includes(source.freshness && source.freshness.state)).length;
      const attention = sources.filter(source => source.alert).length;
      metricCards([
        { label: 'Selected resources', value: sources.length, unit: 'count', available: true },
        { label: 'Resources ready', value: ready, unit: 'count', available: true },
        { label: 'Needs attention', value: attention, unit: 'count', available: true },
        { label: 'Comparison', value: null, available: false, availability_reason: report.comparison_enabled ? 'Previous period enabled where available' : 'Previous period disabled' }
      ]);
      sectionHeading('Source health');
      sourceTable(sources);
      const alerts = Array.isArray(dashboard.alerts) ? dashboard.alerts : [];
      sectionHeading('Attention notes');
      if (alerts.length === 0) paragraph('No source health alerts were recorded when this report was queued.', { color: COLORS.good });
      else alerts.forEach(alert => paragraph(alert.message, { color: alert.severity === 'critical' ? COLORS.danger : COLORS.warning, after: 5 }));
    }

    function crossPlatformSummary() {
      addPage('Cross-platform summary');
      paragraph('This is a side-by-side comparison. Values from different providers are not treated as interchangeable and are never combined into a universal total.');
      for (const source of sources) {
        ensureSpace(66);
        const y = doc.y;
        const color = PROVIDER_COLORS[source.provider] || COLORS.accent;
        doc.roundedRect(PAGE.left, y, PAGE.width - PAGE.left - PAGE.right, 55, 6).fillAndStroke(COLORS.panel, COLORS.line);
        doc.rect(PAGE.left, y, 5, 55).fill(color);
        doc.fillColor(COLORS.ink).font('Helvetica-Bold').fontSize(10).text(
          `${safePdfText(source.provider_name)} - ${safePdfText(source.resource && source.resource.display_name, 'Selected resource')}`,
          PAGE.left + 15,
          y + 9,
          { width: 305, ellipsis: true }
        );
        const firstMetric = (source.metrics || []).find(metric => metric.available && metric.value !== null && metric.value !== undefined);
        doc.fillColor(COLORS.muted).font('Helvetica').fontSize(8).text(statusLabel(source), PAGE.left + 15, y + 31, { width: 160 });
        doc.fillColor(COLORS.ink).font('Helvetica-Bold').fontSize(9).text(
          firstMetric ? `${safePdfText(firstMetric.label)}: ${formatNumber(firstMetric.value, firstMetric.unit)}` : 'No metric available',
          350,
          y + 18,
          { width: 190, align: 'right', ellipsis: true }
        );
        doc.y = y + 66;
      }
    }

    function resourceSection(source) {
      addPage(
        `${safePdfText(source.provider_name)} - ${safePdfText(source.resource && source.resource.display_name, 'Selected resource')}`,
        'RESOURCE DETAIL'
      );
      const resource = source.resource || {};
      const startY = doc.y;
      labelValue('Provider', source.provider_name, { width: 235 });
      const afterProviderY = doc.y;
      doc.y = startY;
      labelValue('Provider resource ID', resource.id, { x: 310, width: 237 });
      doc.y = Math.max(afterProviderY, doc.y);
      labelValue('Reporting range', `${dateText(source.range && source.range.from)} to ${dateText(source.range && source.range.to)}`, { width: 235 });
      const afterRangeY = doc.y;
      doc.y -= 29;
      labelValue('Data through', source.freshness && source.freshness.data_through_date, { x: 310, width: 237 });
      doc.y = Math.max(afterRangeY, doc.y);
      if (source.availability && source.availability.note) {
        sectionHeading('Availability note');
        paragraph(source.availability.note, { color: COLORS.muted, fontSize: 8.5, after: 5 });
      }
      sectionHeading('Key metrics');
      metricCards(source.metrics || []);
      sectionHeading('Stored trend');
      trendChart(source);
      sectionHeading(source.provider === 'google_analytics_4' ? 'Top website paths' : 'Top content');
      contentTable(source);
    }

    function methodology() {
      addPage('Methodology and data notes');
      const notes = Array.isArray(dashboard.methodology) ? dashboard.methodology : [];
      sectionHeading('How to read this report');
      (notes.length ? notes : [
        'Metrics retain their provider definitions, units, reporting ranges, and availability state.',
        'Previous-period changes appear only when a matching stored baseline exists.',
        'Missing, delayed, thresholded, and unsupported values remain explicitly unavailable.'
      ]).forEach(note => paragraph(`- ${safePdfText(note)}`, { after: 5 }));
      sectionHeading('Metric definitions');
      for (const source of sources) {
        paragraph(`${safePdfText(source.provider_name)} - ${safePdfText(source.resource && source.resource.display_name, 'Selected resource')}`, {
          bold: true,
          fontSize: 8.5,
          after: 3
        });
        for (const metric of source.metrics || []) {
          paragraph(
            `${safePdfText(metric.label, metric.key)}: ${safePdfText(metric.definition, 'Definition supplied by the provider-specific metric registry.')}`,
            { fontSize: 7.7, color: COLORS.muted, after: 3 }
          );
        }
      }
      sectionHeading('Snapshot boundary');
      paragraph('This artifact was generated asynchronously from the immutable, stored-data snapshot captured when the report was queued. The renderer made no provider API or remote network requests.');
      labelValue('Snapshot version', snapshot.snapshot_version || '1');
      labelValue('Renderer version', snapshot.renderer_version || 'pdfkit-v1');
      labelValue('Report timezone', report.timezone || 'UTC');
      labelValue('Retention', 'Artifact expires seven days after generation');
      sectionHeading('Data availability');
      paragraph('Provider delays, privacy thresholding, missing grants, unsupported media, and incomplete historical coverage can make a value unavailable. An unavailable value is not interpreted as zero.');
      sectionHeading('Security boundary');
      paragraph('The PDF contains text and vector graphics only. Remote images, external links, embedded files, scripts, and user-supplied local file paths are not rendered.');
    }

    try {
      if (!snapshot || !snapshot.report || !snapshot.dashboard) throw new ReportRenderError('invalid_report_snapshot');
      if (sources.length === 0) throw new ReportRenderError('report_has_no_resources');
      cover();
      if (selectedSections.has('executive_summary')) executiveSummary();
      if (selectedSections.has('cross_platform_summary') && new Set(sources.map(source => source.provider)).size > 1) {
        crossPlatformSummary();
      }
      if (selectedSections.has('resource_sections')) sources.forEach(resourceSection);
      if (selectedSections.has('methodology')) methodology();

      const range = doc.bufferedPageRange();
      for (let pageIndex = range.start; pageIndex < range.start + range.count; pageIndex += 1) {
        doc.switchToPage(pageIndex);
        const originalBottomMargin = doc.page.margins.bottom;
        doc.page.margins.bottom = 0;
        const pageNumber = pageIndex - range.start + 1;
        doc.moveTo(PAGE.left, PAGE.height - 38).lineTo(PAGE.width - PAGE.right, PAGE.height - 38).strokeColor(COLORS.line).lineWidth(0.5).stroke();
        doc.fillColor(COLORS.muted).font('Helvetica').fontSize(7.5).text('Social Insights Studio', PAGE.left, PAGE.height - 29, { width: 200, lineBreak: false });
        doc.text(`${pageNumber} / ${range.count}`, PAGE.width - PAGE.right - 70, PAGE.height - 29, { width: 70, align: 'right', lineBreak: false });
        doc.page.margins.bottom = originalBottomMargin;
      }
      doc.end();
    } catch (error) {
      finishWithError(error);
      try {
        doc.end();
      } catch (endError) {
        void endError;
      }
    }
  });
}

module.exports = {
  COLORS,
  ReportRenderError,
  formatNumber,
  renderReportPdf,
  safePdfText
};
