const nodemailer = require('nodemailer');

let transportFactory = options => nodemailer.createTransport(options);

function createHttpError(status, code) {
  const error = new Error(code);
  error.status = status;
  error.code = code;
  return error;
}

function isProduction(env = process.env) {
  return String(env.NODE_ENV || '').toLowerCase() === 'production';
}

function isPlaceholder(value) {
  return /replace_with|your_|placeholder|changeme|unused/i.test(String(value || ''));
}

function getMailAdapter(env = process.env) {
  return String(env.MAIL_ADAPTER || 'development').trim().toLowerCase();
}

function requireMailValue(env, name) {
  const value = String(env[name] || '').trim();
  if (!value || (isProduction(env) && isPlaceholder(value))) {
    throw createHttpError(503, 'mail_not_configured');
  }
  return value;
}

function getSmtpOptions(env = process.env) {
  const host = requireMailValue(env, 'SMTP_HOST');
  const portValue = requireMailValue(env, 'SMTP_PORT');
  const port = Number(portValue);
  if (!Number.isInteger(port) || port <= 0 || port > 65535) {
    throw createHttpError(503, 'mail_not_configured');
  }

  const user = String(env.SMTP_USER || '').trim();
  const pass = String(env.SMTP_PASSWORD || '');
  if ((user && !pass) || (!user && pass)) {
    throw createHttpError(503, 'mail_not_configured');
  }
  if (isProduction(env) && (isPlaceholder(user) || isPlaceholder(pass))) {
    throw createHttpError(503, 'mail_not_configured');
  }

  const secure = String(env.SMTP_SECURE || '').toLowerCase() === 'true' || port === 465;
  const options = {
    host,
    port,
    secure,
    requireTLS: String(env.SMTP_REQUIRE_TLS || '').toLowerCase() === 'true',
    connectionTimeout: Number(env.SMTP_CONNECTION_TIMEOUT_MS || 10000),
    greetingTimeout: Number(env.SMTP_GREETING_TIMEOUT_MS || 10000),
    socketTimeout: Number(env.SMTP_SOCKET_TIMEOUT_MS || 15000),
    disableFileAccess: true,
    disableUrlAccess: true
  };
  if (user) {
    options.auth = { user, pass };
  }
  return options;
}

function validateMailConfiguration(env = process.env) {
  const adapter = getMailAdapter(env);
  if (isProduction(env) && (!adapter || adapter === 'development')) {
    throw createHttpError(503, 'mail_not_configured');
  }
  if (adapter === 'smtp') {
    getSmtpOptions(env);
    requireMailValue(env, 'MAIL_FROM');
    return adapter;
  }
  if (isProduction(env)) {
    throw createHttpError(503, 'mail_not_configured');
  }
  return adapter;
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function loginUrl(env = process.env) {
  const baseUrl = String(env.BASE_URL || '').replace(/\/+$/, '');
  return baseUrl ? `${baseUrl}/` : '/';
}

async function sendMagicLinkEmail({ email, token }, env = process.env) {
  const developmentTokenReturn =
    !isProduction(env) && String(env.AUTH_DEV_MAGIC_LINKS || '').toLowerCase() === 'true';
  if (developmentTokenReturn) {
    return { sent: false, development: true };
  }

  const adapter = validateMailConfiguration(env);
  if (adapter !== 'smtp') {
    return { sent: false, suppressed: true };
  }

  const transporter = transportFactory(getSmtpOptions(env));
  const url = loginUrl(env);
  try {
    await transporter.sendMail({
      from: requireMailValue(env, 'MAIL_FROM'),
      to: email,
      subject: env.MAIL_SUBJECT || 'Your Social Insights Studio sign-in code',
      text: [
        'Use this one-time sign-in code for Social Insights Studio:',
        '',
        token,
        '',
        `Open ${url} and paste the code to continue.`,
        'This code expires in 15 minutes. If you did not request it, ignore this email.'
      ].join('\n'),
      html: [
        '<p>Use this one-time sign-in code for Social Insights Studio:</p>',
        `<p><code>${escapeHtml(token)}</code></p>`,
        `<p>Open <a href="${escapeHtml(url)}">${escapeHtml(url)}</a> and paste the code to continue.</p>`,
        '<p>This code expires in 15 minutes. If you did not request it, ignore this email.</p>'
      ].join(''),
      disableFileAccess: true,
      disableUrlAccess: true
    });
    return { sent: true };
  } catch (error) {
    throw createHttpError(503, 'mail_send_failed');
  }
}

function setMailTransportFactory(factory) {
  transportFactory = factory || (options => nodemailer.createTransport(options));
}

module.exports = {
  getMailAdapter,
  getSmtpOptions,
  sendMagicLinkEmail,
  setMailTransportFactory,
  validateMailConfiguration
};
