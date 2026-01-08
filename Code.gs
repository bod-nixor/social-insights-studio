/**
 * Looker Studio Community Connector for TikTok User Info & Video Info API
 * Version: 9.0.0 (Production-ready with enhanced security)
 * 
 * Description: This connector retrieves user and video information from the TikTok API
 * and makes it available in Looker Studio. It uses a backend service for TikTok
 * authentication, data fetching, and schema definition with improved security and reliability.
 */

// Global connector instance
var cc = DataStudioApp.createCommunityConnector();

// ------------------------ Constants & Configuration ------------------------

// API Configuration
const BACKEND_API_BASE_URL = null;
const MAX_VIDEOS_TO_FETCH = 200; // Safe default limit to prevent excessive API usage
const MAX_API_RETRIES = 3;
const API_RATE_LIMIT_DELAY_MS = 500;

// ------------------------ Connector Configuration ------------------------

/**
 * Returns the authentication method required by the connector.
 * @return {object} The AuthType response.
 */
function getAuthType() {
  Logger.log('getAuthType called');
  return cc
    .newAuthTypeResponse()
    .setAuthType(cc.AuthType.NONE)
    .build();
}

/**
 * Returns the configuration for the connector.
 * @param {object} request The request parameters.
 * @return {object} The configuration object.
 */
function getConfig(request) {
  Logger.log('getConfig called. Request: ' + JSON.stringify(redactSensitiveInfo(request)));
  var config = cc.getConfig();
  config.setIsSteppedConfig(false);
  config.newTextInput()
    .setId('connector_token')
    .setName('Connector Token')
    .setHelpText('Generate a connector token from your hosted /auth/tiktok/start page and paste it here.');
  return config.build();
}

/**
 * Returns the schema for the connector.
 * @param {object} request The request parameters.
 * @return {object} The schema response.
 */
function getSchema(request) {
  Logger.log('getSchema called. Request: ' + JSON.stringify(redactSensitiveInfo(request)));
  try {
    var fields = getFields();
    return { schema: fields.build() };
  } catch (e) {
    logError('Error in getSchema', e);
    cc.newUserError()
      .setDebugText('Error building schema: ' + e.message)
      .setText('An error occurred while building the connector schema. Please try again later.')
      .throwException();
  }
}

// ------------------------ Data Model & Schema ------------------------

/**
 * Defines all fields (dimensions and metrics) provided by the connector.
 * @return {Fields} The Fields object containing all field definitions.
 */
function getFields() {
  var fields = cc.getFields();
  var types = cc.FieldType;
  var aggregations = cc.AggregationType;

  // User Info - Dimensions
  fields.newDimension()
    .setId('user_open_id')
    .setName('User Open ID')
    .setDescription('Unique identifier for the TikTok user')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('user_union_id')
    .setName('Union ID')
    .setDescription('Union ID for the user (if available)')
    .setType(types.TEXT);
    
  fields.newDimension()
    .setId('user_username')
    .setName('Username')
    .setDescription('TikTok username')
    .setType(types.TEXT);
    
  fields.newDimension()
    .setId('user_display_name')
    .setName('Display Name')
    .setDescription('User\'s display name')
    .setType(types.TEXT);
    
  fields.newDimension()
    .setId('user_bio_description')
    .setName('Bio Description')
    .setDescription('User profile bio')
    .setType(types.TEXT);
    
  fields.newDimension()
    .setId('user_profile_deep_link')
    .setName('Profile Deep Link')
    .setDescription('Direct link to user profile')
    .setType(types.URL);
    
  fields.newDimension()
    .setId('user_avatar_url')
    .setName('Avatar URL')
    .setDescription('URL of user profile picture')
    .setType(types.URL);
    
  fields.newDimension()
    .setId('user_avatar_url_100')
    .setName('Avatar URL (100px)')
    .setDescription('URL of 100px profile picture')
    .setType(types.URL);
    
  fields.newDimension()
    .setId('user_avatar_large_url')
    .setName('Avatar URL (Large)')
    .setDescription('URL of large profile picture')
    .setType(types.URL);
    
  fields.newDimension()
    .setId('user_is_verified')
    .setName('Is Verified')
    .setDescription('Whether the user is verified')
    .setType(types.BOOLEAN);

  // User Info - Metrics
  fields.newMetric()
    .setId('user_follower_count')
    .setName('Follower Count')
    .setDescription('Number of followers')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
    
  fields.newMetric()
    .setId('user_following_count')
    .setName('Following Count')
    .setDescription('Number of accounts followed')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
    
  fields.newMetric()
    .setId('user_likes_count')
    .setName('Total Likes Received')
    .setDescription('Total likes on user\'s videos')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
    
  fields.newMetric()
    .setId('user_video_count')
    .setName('Total Video Count')
    .setDescription('Number of videos posted')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);

  // Video Info - Dimensions
  fields.newDimension()
    .setId('video_id')
    .setName('Video ID')
    .setDescription('Unique identifier for the video')
    .setType(types.TEXT);
    
  fields.newDimension()
    .setId('video_create_time')
    .setName('Video Create Time')
    .setDescription('When the video was created (YYYYMMDDHH)')
    .setType(types.YEAR_MONTH_DAY_HOUR);
    
  fields.newDimension()
    .setId('video_cover_image_url')
    .setName('Video Cover Image URL')
    .setDescription('URL of video thumbnail')
    .setType(types.URL);
    
  fields.newDimension()
    .setId('video_share_url')
    .setName('Video Share URL')
    .setDescription('URL to share the video')
    .setType(types.URL);
    
  fields.newDimension()
    .setId('video_description')
    .setName('Video Description')
    .setDescription('Caption/text description of video')
    .setType(types.TEXT);
    
  fields.newDimension()
    .setId('video_title')
    .setName('Video Title')
    .setDescription('Title of the video')
    .setType(types.TEXT);
    
  fields.newDimension()
    .setId('video_embed_html')
    .setName('Video Embed HTML')
    .setDescription('HTML code to embed the video')
    .setType(types.TEXT);
    
  fields.newDimension()
    .setId('video_embed_link')
    .setName('Video Embed Link')
    .setDescription('URL to embed the video')
    .setType(types.URL);

  // Video Info - Metrics
  fields.newMetric()
    .setId('video_duration')
    .setName('Video Duration (seconds)')
    .setDescription('Length of video in seconds')
    .setType(types.NUMBER)
    .setAggregation(aggregations.AVG);
    
  fields.newMetric()
    .setId('video_height')
    .setName('Video Height')
    .setDescription('Height of video in pixels')
    .setType(types.NUMBER)
    .setAggregation(aggregations.MAX);
    
  fields.newMetric()
    .setId('video_width')
    .setName('Video Width')
    .setDescription('Width of video in pixels')
    .setType(types.NUMBER)
    .setAggregation(aggregations.MAX);
    
  fields.newMetric()
    .setId('video_like_count')
    .setName('Video Like Count')
    .setDescription('Number of likes on video')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
    
  fields.newMetric()
    .setId('video_comment_count')
    .setName('Video Comment Count')
    .setDescription('Number of comments on video')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
    
  fields.newMetric()
    .setId('video_share_count')
    .setName('Video Share Count')
    .setDescription('Number of shares of video')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
    
  fields.newMetric()
    .setId('video_view_count')
    .setName('Video View Count')
    .setDescription('Number of views on video')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);

  return fields;
}

/**
 * Retrieves the backend base URL for API calls.
 * @return {string} The backend base URL.
 */
function getBackendBaseUrl() {
  const scriptProps = PropertiesService.getScriptProperties();
  const configuredUrl = scriptProps.getProperty('BACKEND_API_BASE_URL') ||
    scriptProps.getProperty('DEPLOYED_DOMAIN');
  const baseUrl = configuredUrl || BACKEND_API_BASE_URL;

  if (!baseUrl || baseUrl === 'https://YOUR_DOMAIN_HERE') {
    throw new Error('Backend API base URL is not configured.');
  }

  return baseUrl.replace(/\\/$/, '');
}

/**
 * Extracts the connector token from the request config.
 * @param {object} request The request parameters.
 * @return {string|null} The connector token.
 */
function getConnectorToken(request) {
  return request && request.configParams ? request.configParams.connector_token : null;
}

// ------------------------ Data Fetching ------------------------

/**
 * Returns the data for the connector based on the user's request.
 * @param {object} request The request parameters.
 * @return {object} The data response.
 */
function getData(request) {
  if (!request || !request.fields || request.fields.length === 0) {
    cc.newUserError()
      .setDebugText('Invalid request: missing fields')
      .setText('The connector received an invalid request. Please try again.')
      .throwException();
  }

  Logger.log("getData request received for fields: " + 
    request.fields.map(f => f.name).join(', '));

  try {
    const connectorToken = getConnectorToken(request);
    if (!connectorToken) {
      cc.newUserError()
        .setDebugText('Missing connector token')
        .setText('A connector token is required. Please generate one and add it to the connector configuration.')
        .throwException();
    }

    const requestedFields = getFields().forIds(request.fields.map(f => f.name));
    const dataRows = [];

    // Fetch User Info
    const userData = fetchUserInfo(connectorToken);
    if (!userData || !userData.open_id) {
      throw new Error('Failed to fetch user data or obtain open_id');
    }

    // Fetch Video Data with pagination
    const videoApiFields = [
      'id', 'create_time', 'cover_image_url', 'share_url',
      'video_description', 'duration', 'height', 'width',
      'title', 'like_count', 'comment_count', 'share_count',
      'view_count', 'embed_html', 'embed_link'
    ];

    let videosData = [];
    try {
      videosData = fetchPaginatedVideos(userData.open_id, connectorToken, videoApiFields);
      Logger.log(`Retrieved ${videosData.length} videos`);
    } catch (e) {
      Logger.log("Error fetching video data: " + e.message);
      // Continue with user data only if video fetch fails
    }

    // Combine data into rows
    if (videosData.length === 0) {
      dataRows.push(createCombinedRow(userData, null, requestedFields));
    } else {
      videosData.forEach(video => {
        dataRows.push(createCombinedRow(userData, video, requestedFields));
      });
    }

    return {
      schema: requestedFields.build(),
      rows: dataRows
    };

  } catch (error) {
    logError('Error in getData', error);
    cc.newUserError()
      .setDebugText('Failed to fetch data: ' + error.message)
      .setText('An error occurred while fetching data from TikTok. Please try again.')
      .throwException();
  }
}

/**
 * Fetches user info from TikTok API.
 * @param {string} connectorToken The connector token from the backend.
 * @return {object|null} The user data object or null on failure.
 */
function fetchUserInfo(connectorToken) {
  const userApiFields = [
    'open_id', 'union_id', 'username', 'display_name',
    'bio_description', 'profile_deep_link', 'avatar_url',
    'avatar_url_100', 'avatar_large_url', 'is_verified',
    'follower_count', 'following_count', 'likes_count',
    'video_count'
  ];

  const backendBaseUrl = getBackendBaseUrl();
  const options = {
    method: 'GET',
    headers: { 
      'Authorization': 'Bearer ' + connectorToken,
      'Content-Type': 'application/json'
    },
    muteHttpExceptions: true,
    validateHttpsCertificates: true // Ensure SSL certificate validation
  };

  const url = `${backendBaseUrl}/api/tiktok/user?fields=${encodeURIComponent(userApiFields.join(','))}`;
  Logger.log('Fetching user info from backend: ' + url);
  const response = fetchWithRetry(url, options);

  if (response.getResponseCode() !== 200) {
    handleApiError(response.getResponseCode(), response.getContentText(), url, "fetchUserInfo");
    return null;
  }

  const responseText = response.getContentText();
  const data = safeJsonParse(responseText);
  
  if (data.error && data.error.code && data.error.code !== "ok") {
    handleTikTokApiError(data.error, "fetchUserInfo");
    return null;
  }

  return data.data && data.data.user ? data.data.user : null;
}

/**
 * Fetches videos with pagination handling.
 * @param {string} openId The user's open_id.
 * @param {string} connectorToken The connector token from the backend.
 * @param {string[]} fields The list of video fields to request.
 * @param {number} maxVideos Maximum number of videos to fetch.
 * @return {object[]} An array of video data objects.
 */
function fetchPaginatedVideos(openId, connectorToken, fields, maxVideos = MAX_VIDEOS_TO_FETCH) {
  const allVideos = [];
  let cursor = null;
  let hasMore = true;
  let requestCount = 0;
  const maxRequests = 10; // Prevent infinite loops

  const fieldQuery = encodeURIComponent(fields.join(','));
  const backendBaseUrl = getBackendBaseUrl();
  const videoListUrl = `${backendBaseUrl}/api/tiktok/videos?fields=${fieldQuery}`;

  Logger.log(`Fetching paginated videos. URL base: ${videoListUrl}, Max videos: ${maxVideos}`);

  while (hasMore && allVideos.length < maxVideos && requestCount < maxRequests) {
    requestCount++;
    Logger.log(`Fetching video page ${requestCount}. Current videos: ${allVideos.length}`);

    const options = {
      method: 'GET',
      headers: {
        'Authorization': 'Bearer ' + connectorToken,
        'Content-Type': 'application/json'
      },
      muteHttpExceptions: true,
      validateHttpsCertificates: true
    };

    const queryParams = [];
    queryParams.push(`max_count=20`);
    if (cursor) {
      queryParams.push(`cursor=${encodeURIComponent(cursor)}`);
    }
    const pagedUrl = `${videoListUrl}&${queryParams.join('&')}`;
    const response = fetchWithRetry(pagedUrl, options);

    if (response.getResponseCode() !== 200) {
      handleApiError(response.getResponseCode(), response.getContentText(), videoListUrl, `fetchPaginatedVideos (page ${requestCount})`);
      hasMore = false;
      break;
    }

    const responseText = response.getContentText();
    const data = safeJsonParse(responseText);

    if (data.error && data.error.code && data.error.code !== "ok") {
      handleTikTokApiError(data.error, `fetchPaginatedVideos (page ${requestCount})`);
      hasMore = false;
      break;
    }

    if (data.data && data.data.videos && data.data.videos.length > 0) {
      allVideos.push(...data.data.videos);
      hasMore = data.data.has_more === true;
      cursor = data.data.cursor || null;
    } else {
      hasMore = false;
    }

    // Respect API rate limits
    if (hasMore) {
      Utilities.sleep(API_RATE_LIMIT_DELAY_MS);
    }
  }

  Logger.log(`Finished fetching videos. Total videos retrieved: ${allVideos.length}`);
  return allVideos.slice(0, maxVideos);
}

/**
 * Creates a combined data row from user and video data.
 * @param {object} userData The user data object.
 * @param {object|null} videoData The video data object or null.
 * @param {Fields} requestedFields The Fields object for the request.
 * @return {object} A row object for Looker Studio.
 */
function createCombinedRow(userData, videoData, requestedFields) {
  return {
    values: requestedFields.asArray().map(field => {
      const fieldId = field.getId();
      const fieldType = field.getType();

      // User fields
      if (fieldId.startsWith('user_')) {
        const userApiField = fieldId.replace('user_', '');
        if (userData && typeof userData[userApiField] !== 'undefined') {
          return formatFieldValue(userData[userApiField], fieldType);
        }
        return null;
      }

      // Video fields
      if (fieldId.startsWith('video_')) {
        if (videoData) {
          const videoApiField = fieldId.replace('video_', '');
          if (typeof videoData[videoApiField] !== 'undefined') {
            return formatFieldValue(videoData[videoApiField], fieldType);
          }
        }
        return null;
      }

      Logger.log('Encountered unexpected fieldId: ' + fieldId);
      return null;
    })
  };
}

/**
 * Formats field values according to their type for Looker Studio.
 * @param {*} value The raw value from the API.
 * @param {string} fieldType The FieldType constant.
 * @return {*} The formatted value.
 */
function formatFieldValue(value, fieldType) {
  if (value === null || value === undefined) {
    return null;
  }

  const types = cc.FieldType;

  try {
    switch (fieldType) {
      case types.NUMBER:
        const num = Number(value);
        return isNaN(num) ? 0 : num;
      case types.YEAR_MONTH_DAY_HOUR:
        if (isNaN(Number(value))) return null;
        return Utilities.formatDate(new Date(Number(value) * 1000), 'UTC', 'yyyyMMddHH');
      case types.BOOLEAN:
        return Boolean(value);
      case types.TEXT:
      case types.URL:
        return String(value);
      default:
        return String(value);
    }
  } catch (e) {
    Logger.log('Error formatting field value: ' + e.toString());
    return null;
  }
}

// ------------------------ API Utilities ------------------------

/**
 * Fetches a URL with retry logic for transient errors.
 * @param {string} url The URL to fetch.
 * @param {object} options The options for UrlFetchApp.fetch().
 * @param {number} retries The maximum number of retries.
 * @return {HTTPResponse} The HTTPResponse object.
 */
function fetchWithRetry(url, options, retries = MAX_API_RETRIES) {
  for (let i = 0; i < retries; i++) {
    try {
      Logger.log(`fetchWithRetry: Attempt ${i + 1} for URL: ${url}`);
      const response = UrlFetchApp.fetch(url, options);
      const responseCode = response.getResponseCode();

      // Retry on 429 or 5xx errors
      if (responseCode === 429 || (responseCode >= 500 && responseCode < 600)) {
        if (i < retries - 1) {
          const sleepTime = Math.pow(2, i) * 1000 + Math.random() * 1000;
          Logger.log(`Retrying in ${sleepTime / 1000}s...`);
          Utilities.sleep(sleepTime);
          continue;
        }
      }
      return response;
    } catch (e) {
      Logger.log(`fetchWithRetry: Exception during fetch attempt ${i + 1}: ${e.toString()}`);
      if (i < retries - 1) {
        const sleepTime = Math.pow(2, i) * 1000 + Math.random() * 1000;
        Utilities.sleep(sleepTime);
      } else {
        throw e;
      }
    }
  }
}

/**
 * Handles HTTP API errors.
 * @param {number} responseCode The HTTP response code.
 * @param {string} responseBody The response body text.
 * @param {string} apiUrl The API URL that was called.
 * @param {string} context Additional context for the error.
 */
function handleApiError(responseCode, responseBody, apiUrl, context) {
  context = context || "API call";
  Logger.log(`handleApiError (${context}): HTTP Error ${responseCode}. URL: ${apiUrl}`);
  
  let errorMessage = `Error fetching data from TikTok API (${context}). Response code: ${responseCode}.`;
  try {
    const errorData = safeJsonParse(responseBody);
    if (errorData.error && errorData.error.message) {
      errorMessage += ` Message: ${errorData.error.message}`;
    } else if (errorData.message) {
      errorMessage += ` Message: ${errorData.message}`;
    }
  } catch (parseError) {
    errorMessage += ' Could not parse error response body.';
  }

  cc.newUserError()
    .setDebugText(`${errorMessage} | URL: ${apiUrl}`)
    .setText(`Failed to retrieve data from TikTok (Code: ${responseCode}).`)
    .throwException();
}

/**
 * Handles TikTok API-specific errors.
 * @param {object} error The error object from the API response.
 * @param {string} context Additional context for the error.
 */
function handleTikTokApiError(error, context) {
  context = context || "TikTok API";
  Logger.log(`handleTikTokApiError (${context}): Code: ${error.code}, Message: ${error.message}`);

  const errorMap = {
    "invalid_params": "Invalid parameters sent to TikTok API.",
    "invalid_token": "Authentication token is invalid or expired.",
    "access_token_invalid": "Authentication token is invalid.",
    "token_expired": "Authentication token has expired.",
    "rate_limit_exceeded": "API rate limit reached.",
    "permission_denied": "Permission denied. Check required scopes.",
    "insufficient_scope": "Missing required permissions.",
    "user_not_found": "User not found.",
    "video_not_found": "Video not found.",
    "account_not_authorized_for_open_api": "Account not authorized for API access."
  };

  const userMessage = errorMap[error.code] || `TikTok API error: ${error.message || error.code}`;

  cc.newUserError()
    .setDebugText(`TikTok API Error (${context}): ${error.code} - ${error.message}`)
    .setText(userMessage)
    .throwException();
}

// ------------------------ Admin Functions ------------------------

/**
 * Determines if the current user is an admin.
 * @return {boolean} True if admin, false otherwise.
 */
function isAdminUser() {
  // In production, implement proper admin checks
  return false;
}

// ------------------------ Utility Functions ------------------------

/**
 * Safely parses JSON with error handling.
 * @param {string} jsonString The JSON string to parse.
 * @return {object|null} The parsed object or null on error.
 */
function safeJsonParse(jsonString) {
  try {
    return JSON.parse(jsonString);
  } catch (e) {
    Logger.log('Error parsing JSON: ' + e.toString());
    return null;
  }
}

/**
 * Logs errors with stack traces.
 * @param {string} context The context of the error.
 * @param {Error} error The error object.
 */
function logError(context, error) {
  Logger.log(`${context}: ${error.message}\nStack: ${error.stack}`);
}

/**
 * Redacts sensitive information from logs.
 * @param {object} data The data to redact.
 * @return {object} The redacted data.
 */
function redactSensitiveInfo(data) {
  if (!data) return data;
  const redacted = {...data};
  if (redacted.code) redacted.code = 'REDACTED';
  if (redacted.access_token) redacted.access_token = 'REDACTED';
  if (redacted.refresh_token) redacted.refresh_token = 'REDACTED';
  return redacted;
}
