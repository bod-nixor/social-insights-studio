/**
 * Looker Studio Community Connector for TikTok User Info & Video Info API
 * Version: 8.7.1 (Combines User and Video Info, fixes, and formatting)
 *
 * Description: This connector retrieves user and video information from the TikTok API
 * and makes it available in Looker Studio. It handles OAuth2 authentication,
 * data fetching, and schema definition.
 */

// Global connector instance
var cc = DataStudioApp.createCommunityConnector();

// ------------------------ Connector Configuration ------------------------

/**
 * Returns the authentication method required by the connector.
 * This function tells Looker Studio that this connector uses OAuth2 for authentication.
 * @return {object} The AuthType response.
 */
function getAuthType() {
  Logger.log('getAuthType called');
  return cc
    .newAuthTypeResponse()
    .setAuthType(cc.AuthType.OAUTH2) // Use OAuth2 authentication
    .build();
}

/**
 * Returns the configuration for the connector.
 * This function now has no user-configurable options, as it always fetches both user and video data.
 * @param {object} request The request parameters (not used in this version).
 * @return {object} The configuration object.
 */
function getConfig(request) {
  Logger.log('getConfig called. Request: ' + JSON.stringify(request));
  var config = cc.getConfig();

  // No report type selection needed, as we will always return both user and video info.
  config.setIsSteppedConfig(false); // No longer a stepped config as there are no conditional inputs
  return config.build();
}

/**
 * Returns the schema for the connector.
 * The schema now defines all fields from both user and video data.
 * @param {object} request The request parameters.
 * @return {object} The schema response.
 */
function getSchema(request) {
  Logger.log('getSchema called. Request: ' + JSON.stringify(request));
  try {
    var fields = getFields(); // Get all combined field definitions
    return { schema: fields.build() }; // Build the schema object
  } catch (e) {
    Logger.log("Error in getSchema: " + e.toString() + " Stack: " + e.stack);
    cc.newUserError()
      .setDebugText('Error in getSchema: ' + e.toString() + ' Stack: ' + e.stack)
      .setText('An unexpected error occurred while building the connector schema. Please try again later.')
      .throwException();
  }
}

/**
 * Defines all fields (dimensions and metrics) provided by the connector, combining user and video data.
 * @param {object} request The request parameters (not used, kept for signature consistency).
 * @return {object} The Fields object.
 */
function getFields(request) {
  var fields = cc.getFields();
  var types = cc.FieldType;
  var aggregations = cc.AggregationType;

  // ──────── USER INFO ────────

  // User Info - Dimensions
  fields.newDimension().setId('user_open_id').setName('User Open ID').setType(types.TEXT);
  fields.newDimension().setId('user_union_id').setName('Union ID').setType(types.TEXT); // Optional in API
  fields.newDimension().setId('user_username').setName('Username').setType(types.TEXT);
  fields.newDimension().setId('user_display_name').setName('Display Name').setType(types.TEXT);
  fields.newDimension().setId('user_bio_description').setName('Bio Description').setType(types.TEXT);
  fields.newDimension().setId('user_profile_deep_link').setName('Profile Deep Link').setType(types.URL);
  fields.newDimension().setId('user_avatar_url').setName('Avatar URL').setType(types.URL);
  fields.newDimension().setId('user_avatar_url_100').setName('Avatar URL (100px)').setType(types.URL);
  fields.newDimension().setId('user_avatar_large_url').setName('Avatar URL (Large)').setType(types.URL);
  fields.newDimension().setId('user_is_verified').setName('Is Verified').setType(types.BOOLEAN);

  // User Info - Metrics
  fields.newMetric().setId('user_follower_count').setName('Follower Count').setType(types.NUMBER).setAggregation(aggregations.SUM);
  fields.newMetric().setId('user_following_count').setName('Following Count').setType(types.NUMBER).setAggregation(aggregations.SUM);
  fields.newMetric().setId('user_likes_count').setName('Total Likes Received').setType(types.NUMBER).setAggregation(aggregations.SUM);
  fields.newMetric().setId('user_video_count').setName('Total Video Count').setType(types.NUMBER).setAggregation(aggregations.SUM);

  // ──────── VIDEO INFO ────────

  // Video Info - Dimensions
  fields.newDimension().setId('video_id').setName('Video ID').setType(types.TEXT);
  fields.newDimension().setId('video_create_time').setName('Video Create Time').setType(types.YEAR_MONTH_DAY_HOUR);
  fields.newDimension().setId('video_cover_image_url').setName('Video Cover Image URL').setType(types.URL);
  fields.newDimension().setId('video_share_url').setName('Video Share URL').setType(types.URL);
  fields.newDimension().setId('video_description').setName('Video Description').setType(types.TEXT);
  fields.newDimension().setId('video_title').setName('Video Title').setType(types.TEXT);
  fields.newDimension().setId('video_embed_html').setName('Video Embed HTML').setType(types.TEXT);
  fields.newDimension().setId('video_embed_link').setName('Video Embed Link').setType(types.URL);

  // Video Info - Metrics
  fields.newMetric().setId('video_duration').setName('Video Duration (seconds)').setType(types.NUMBER).setAggregation(aggregations.AVG);
  fields.newMetric().setId('video_height').setName('Video Height').setType(types.NUMBER).setAggregation(aggregations.MAX);
  fields.newMetric().setId('video_width').setName('Video Width').setType(types.NUMBER).setAggregation(aggregations.MAX);
  fields.newMetric().setId('video_like_count').setName('Video Like Count').setType(types.NUMBER).setAggregation(aggregations.SUM);
  fields.newMetric().setId('video_comment_count').setName('Video Comment Count').setType(types.NUMBER).setAggregation(aggregations.SUM);
  fields.newMetric().setId('video_share_count').setName('Video Share Count').setType(types.NUMBER).setAggregation(aggregations.SUM);
  fields.newMetric().setId('video_view_count').setName('Video View Count').setType(types.NUMBER).setAggregation(aggregations.SUM);

  return fields;
}

// ------------------------ Data Fetching ------------------------

// Field mapping configuration (defined but not currently used in createCombinedRow, kept for reference)
const USER_FIELD_MAP = {
  'user_open_id': 'open_id',
  'user_union_id': 'union_id',
  'user_username': 'username',
  'user_display_name': 'display_name',
  'user_bio_description': 'bio_description',
  'user_profile_deep_link': 'profile_deep_link',
  'user_avatar_url': 'avatar_url',
  'user_avatar_url_100': 'avatar_url_100',
  'user_avatar_large_url': 'avatar_large_url',
  'user_is_verified': 'is_verified',
  'user_follower_count': 'follower_count',
  'user_following_count': 'following_count',
  'user_likes_count': 'likes_count',
  'user_video_count': 'video_count'
};

const VIDEO_FIELD_MAP = {
  'video_id': 'id',
  'video_create_time': 'create_time',
  'video_cover_image_url': 'cover_image_url',
  'video_share_url': 'share_url',
  'video_description': 'video_description',
  'video_duration': 'duration',
  'video_height': 'height',
  'video_width': 'width',
  'video_title': 'title',
  'video_embed_html': 'embed_html',
  'video_embed_link': 'embed_link',
  'video_like_count': 'like_count',
  'video_comment_count': 'comment_count',
  'video_share_count': 'share_count',
  'video_view_count': 'view_count'
};


/**
 * Returns the data for the connector based on the user's request.
 */
function getData(request) {
  // Validate request
  if (!request || !request.fields || request.fields.length === 0) {
    cc.newUserError()
      .setDebugText('Invalid request: missing fields')
      .setText('The connector received an invalid request. Please try again.')
      .throwException();
  }

  Logger.log("getData request received for fields: " +
    request.fields.map(f => f.name).join(', '));

  try {
    // Get access token and validate
    const accessToken = getAccessToken();
    if (!accessToken) {
      // isAuthValid() or getAccessToken() should have triggered re-auth if possible.
      // If still no token, it means auth failed or is needed.
      cc.newUserError()
        .setDebugText('No valid access token available. Please re-authenticate.')
        .setText('Authentication is required or has failed. Please re-authenticate the connector.')
        .throwException();
    }

    // Get requested fields
    const requestedFields = getFields().forIds(request.fields.map(f => f.name));
    const dataRows = [];

    // 1. Fetch User Info
    const userData = fetchUserInfo(accessToken);
    if (!userData || !userData.open_id) { // Check if userData itself is null/undefined
      throw new Error('Failed to fetch user data or obtain open_id');
    }

    // 2. Fetch Video Data with proper pagination
    // Define the video fields expected from the API for the video.list endpoint
    const videoApiFieldsToRequest = [
      'id', 'create_time', 'cover_image_url', 'share_url',
      'video_description', 'duration', 'height', 'width',
      'title', 'like_count', 'comment_count', 'share_count',
      'view_count', 'embed_html', 'embed_link' // Added embed fields
    ];

    let videosData = [];
    try {
      videosData = fetchPaginatedVideos(userData.open_id, accessToken, videoApiFieldsToRequest);
      Logger.log(`Retrieved ${videosData.length} videos`);
    } catch (e) {
      // Log the error but proceed, as user data might still be valuable.
      // Or, decide if this should be a fatal error for the getData request.
      Logger.log("Error fetching video data: " + e.message + ". Proceeding with user data only or partial video data if some pages were fetched.");
      // Optionally, rethrow or throw a user error if video data is critical
      // cc.newUserError().setDebugText('Failed to fetch video data: ' + e.message).setText('Could not retrieve video information.').throwException();
    }

    // 3. Combine data into rows
    if (videosData.length === 0) {
      // If no videos, still return a row with user data and nulls for video fields
      Logger.log("No videos found or fetched. Returning user data only in a single row.");
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
    Logger.log("getData error: " + error.message + "\nStack: " + error.stack);
    cc.newUserError()
      .setDebugText('Failed to fetch data: ' + error.message)
      .setText('An error occurred while fetching data from TikTok. Please try again or check connector configuration.')
      .throwException();
  }
}

/**
 * Fetches user info from TikTok API
 * @param {string} accessToken The OAuth2 access token.
 * @return {object|null} The user data object or null on failure.
 */
function fetchUserInfo(accessToken) {
  // Define the user fields expected from the API for the user/info endpoint
  const userApiFieldsToRequest = [
    'open_id', 'union_id', 'username', 'display_name',
    'bio_description', 'profile_deep_link', 'avatar_url',
    'avatar_url_100', 'avatar_large_url', 'is_verified',
    'follower_count', 'following_count', 'likes_count', // User's total likes given
    'video_count' // User's total videos posted
  ];

  const options = {
    method: 'GET',
    headers: { 'Authorization': 'Bearer ' + accessToken },
    muteHttpExceptions: true
  };

  const url = `https://open.tiktokapis.com/v2/user/info/?fields=${userApiFieldsToRequest.join(',')}`;
  Logger.log('Fetching user info from: ' + url);
  const response = fetchWithRetry(url, options);

  if (response.getResponseCode() !== 200) {
    handleApiError(response.getResponseCode(), response.getContentText(), url, "fetchUserInfo");
    return null; // Or throw error
  }

  const responseText = response.getContentText();
  Logger.log('User info API response: ' + responseText);
  const data = JSON.parse(responseText);

  if (data.error && data.error.code && data.error.code !== "ok") {
    handleTikTokApiError(data.error, "fetchUserInfo");
    return null; // Or throw error
  }

  return data.data && data.data.user ? data.data.user : null;
}

/**
 * Fetches videos with proper pagination handling.
 * @param {string} openId The user's open_id (not directly used by video/list, but good for context).
 * @param {string} accessToken The OAuth2 access token.
 * @param {string[]} fields The list of video fields to request from the API.
 * @param {number} maxVideos Maximum number of videos to fetch in total.
 * @return {object[]} An array of video data objects.
 */
function fetchPaginatedVideos(openId, accessToken, fields, maxVideos = 200) { // openId is not used in API call itself
  const allVideos = [];
  let cursor = null;
  let hasMore = true;
  let requestCount = 0;
  const maxRequests = 10; // Limit API calls to prevent infinite loops or excessive requests

  const fieldQuery = fields.join(',');
  const videoListUrl = `https://open.tiktokapis.com/v2/video/list/?fields=${encodeURIComponent(fieldQuery)}`;

  Logger.log(`Fetching paginated videos. URL base: ${videoListUrl}, Max videos: ${maxVideos}`);

  while (hasMore && allVideos.length < maxVideos && requestCount < maxRequests) {
    requestCount++;
    Logger.log(`Fetching video page ${requestCount}. Current videos: ${allVideos.length}. Cursor: ${cursor}`);

    const payload = {
      max_count: 20 // Max allowed by TikTok API is 20
    };
    if (cursor) {
      payload.cursor = cursor;
    }

    const options = {
      method: 'POST',
      headers: {
        'Authorization': 'Bearer ' + accessToken,
        'Content-Type': 'application/json'
      },
      payload: JSON.stringify(payload),
      muteHttpExceptions: true
    };

    const response = fetchWithRetry(videoListUrl, options); // URL already includes fields

    if (response.getResponseCode() !== 200) {
      // Log error and potentially stop pagination or throw a more specific error
      handleApiError(response.getResponseCode(), response.getContentText(), videoListUrl, `fetchPaginatedVideos (page ${requestCount})`);
      hasMore = false; // Stop pagination on HTTP error
      break;
    }

    const responseText = response.getContentText();
    Logger.log(`Video list API response (page ${requestCount}): ${responseText}`);
    const data = JSON.parse(responseText);

    if (data.error && data.error.code && data.error.code !== "ok") {
      handleTikTokApiError(data.error, `fetchPaginatedVideos (page ${requestCount})`);
      hasMore = false; // Stop pagination on TikTok API error
      break;
    }

    if (data.data && data.data.videos && data.data.videos.length > 0) {
      allVideos.push(...data.data.videos);
      hasMore = data.data.has_more === true;
      cursor = data.data.cursor || null;
      if (!hasMore) Logger.log('No more videos to fetch from API.');
    } else {
      Logger.log('No videos in current page response or data.videos is missing/empty.');
      hasMore = false;
    }

    // Respect API rate limits - a small delay between paginated requests
    if (hasMore) {
      Utilities.sleep(500); // Increased sleep time slightly
    }
  }

  if (requestCount >= maxRequests && hasMore) {
    Logger.log(`Reached max request limit (${maxRequests}) for fetching videos, but API indicates more might be available.`);
  }
  Logger.log(`Finished fetching videos. Total videos retrieved: ${allVideos.length}`);
  return allVideos.slice(0, maxVideos); // Ensure we don't exceed maxVideos if last page pushes over
}

/**
 * Creates a combined data row from user and (optional) video data.
 * @param {object} userData The user data object.
 * @param {object|null} videoData The video data object, or null if no video for this row.
 * @param {object} requestedFields The Fields object for the current request.
 * @return {object} A row object for Looker Studio.
 */
function createCombinedRow(userData, videoData, requestedFields) {
  return {
    values: requestedFields.asArray().map(field => {
      const fieldId = field.getId(); // e.g., 'user_open_id', 'video_id'
      const fieldType = field.getType();

      // User fields
      if (fieldId.startsWith('user_')) {
        const userApiField = fieldId.replace('user_', ''); // e.g., 'open_id'
        if (userData && typeof userData[userApiField] !== 'undefined') {
          return formatFieldValue(userData[userApiField], fieldType);
        }
        return null; // User data or specific field not available
      }

      // Video fields
      if (fieldId.startsWith('video_')) {
        if (videoData) { // videoData might be null if it's a user-only row
          const videoApiField = fieldId.replace('video_', ''); // e.g., 'id'
          if (typeof videoData[videoApiField] !== 'undefined') {
            return formatFieldValue(videoData[videoApiField], fieldType);
          }
        }
        return null; // Video data or specific field not available, or no videoData for this row
      }

      // This case should ideally not be reached if schema and requested fields are consistent
      Logger.log('createCombinedRow: Encountered fieldId without expected prefix: ' + fieldId + '. Returning null.');
      return null;
    })
  };
}

/**
 * Formats field values according to their type for Looker Studio.
 * @param {*} value The raw value from the API.
 * @param {string} fieldType The FieldType constant from cc.FieldType.
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
        return isNaN(num) ? 0 : num; // Default to 0 if not a valid number (original behavior)
      case types.YEAR_MONTH_DAY: // Not currently used by any field in this connector's schema
        if (isNaN(Number(value))) return null;
        return Utilities.formatDate(new Date(Number(value) * 1000), 'UTC', 'yyyyMMdd');
      case types.YEAR_MONTH_DAY_HOUR: // Used by 'video_create_time'
        if (isNaN(Number(value))) return null; // Ensure value is a number (timestamp)
        // API's create_time is a Unix timestamp (seconds)
        return Utilities.formatDate(new Date(Number(value) * 1000), 'UTC', 'yyyyMMddHH');
      case types.BOOLEAN:
        return Boolean(value);
      case types.TEXT:
      case types.URL: // URL is also essentially text
        return String(value);
      default:
        Logger.log('formatFieldValue: Unknown field type: ' + fieldType + '. Returning value as string.');
        return String(value); // Fallback, convert to string
    }
  } catch (e) {
    Logger.log('Error formatting field value (' + value + ') for type (' + fieldType + '): ' + e.toString());
    return null; // Return null if formatting fails
  }
}

/**
 * Fetches a URL with retry logic for transient errors (e.g., rate limits, server errors).
 * @param {string} url The URL to fetch.
 * @param {object} options The options for UrlFetchApp.fetch().
 * @param {number} retries The maximum number of retries.
 * @return {HTTPResponse} The HTTPResponse object.
 */
function fetchWithRetry(url, options, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      Logger.log(`fetchWithRetry: Attempt ${i + 1} for URL: ${url}`);
      const response = UrlFetchApp.fetch(url, options);
      const responseCode = response.getResponseCode();

      // Retry on 429 (Too Many Requests) or 5xx server errors
      if (responseCode === 429 || (responseCode >= 500 && responseCode < 600)) {
        if (i < retries - 1) {
          const sleepTime = Math.pow(2, i) * 1000 + Math.random() * 1000; // Exponential backoff with jitter
          Logger.log(`fetchWithRetry: Response code ${responseCode}. Retrying in ${sleepTime / 1000}s...`);
          Utilities.sleep(sleepTime);
          continue; // Retry
        }
      }
      return response; // Success or non-retryable error
    } catch (e) {
      Logger.log(`fetchWithRetry: Exception during fetch attempt ${i + 1} for URL ${url}: ${e.toString()}`);
      if (i < retries - 1) {
        const sleepTime = Math.pow(2, i) * 1000 + Math.random() * 1000;
        Logger.log(`fetchWithRetry: Retrying in ${sleepTime / 1000}s...`);
        Utilities.sleep(sleepTime);
      } else {
        Logger.log(`fetchWithRetry: Max retries reached for URL ${url}. Rethrowing error.`);
        throw e; // Max retries reached, rethrow the last error
      }
    }
  }
}

/**
 * Helper function to handle common HTTP API errors.
 * @param {number} responseCode The HTTP response code.
 * @param {string} responseBody The response body text.
 * @param {string} apiUrl The API URL that was called.
 * @param {string} context Additional context for the error (e.g., function name).
 */
function handleApiError(responseCode, responseBody, apiUrl, context) {
  context = context || "API call";
  Logger.log(`handleApiError (${context}): TikTok API HTTP Error ${responseCode}. URL: ${apiUrl}. Response: ${responseBody}`);
  var errorMessage = `Error fetching data from TikTok API (${context}). Response code: ${responseCode}.`;
  try {
    var errorData = JSON.parse(responseBody);
    if (errorData.error && errorData.error.message) {
      errorMessage += ` Message: ${errorData.error.message}`;
    } else if (errorData.message) { // Some APIs might use 'message' directly
      errorMessage += ` Message: ${errorData.message}`;
    } else {
      errorMessage += ' Could not parse specific error message from response.';
    }
  } catch (parseError) {
    errorMessage += ' Could not parse error response body. Raw response: ' + responseBody.substring(0, 200);
    Logger.log(`handleApiError (${context}): Error parsing error response: ${parseError}`);
  }

  cc.newUserError()
    .setDebugText(`${errorMessage} | URL: ${apiUrl} | Response Body: ${responseBody.substring(0, 500)}`)
    .setText(`Failed to retrieve data from TikTok (Code: ${responseCode}). Check connector logs or API status.`)
    .throwException();
}

/**
 * Helper function to handle TikTok API-specific errors within a 200 OK HTTP response.
 * @param {object} error The error object from the TikTok API response (e.g., data.error).
 * @param {string} context Additional context for the error (e.g., function name).
 */
function handleTikTokApiError(error, context) {
  context = context || "TikTok API logic";
  Logger.log(`handleTikTokApiError (${context}): Code: ${error.code}, Message: ${error.message}, Log ID: ${error.log_id}`);

  const errorMap = {
    "ok": "Success (but an error object was still processed, check logic).", // Should not be an error if "ok"
    "invalid_params": "Invalid parameters sent to TikTok API. Please check connector configuration or report this issue.",
    "invalid_token": "Authentication token is invalid or expired. Please re-authenticate the connector.",
    "access_token_invalid": "Authentication token is invalid or expired. Please re-authenticate the connector.", // Common variation
    "token_expired": "Authentication token has expired. Please re-authenticate the connector.", // Common variation
    "rate_limit_exceeded": "API rate limit reached. Please try again later.",
    "permission_denied": "Permission denied. Ensure the connector has the required scopes and the user has access.",
    "insufficient_scope": "Missing required permissions (scopes). Please re-authenticate and grant all requested permissions.",
    "user_not_found": "User not found.",
    "video_not_found": "Video not found.",
    "account_not_authorized_for_open_api": "This TikTok account is not authorized for Open API access. This might be due to account type (e.g., private, under 18) or other restrictions.",
    // Add more specific TikTok error codes as they are encountered
  };

  const userMessage = errorMap[error.code] || `TikTok API error (${context}): ${error.message || error.code}. Log ID: ${error.log_id || 'N/A'}`;

  cc.newUserError()
    .setDebugText(`TikTok API Error (${context}): Code: ${error.code}, Message: ${error.message}, Details: ${JSON.stringify(error)}`)
    .setText(userMessage)
    .throwException();
}


// ------------------------ OAuth2 Handling ------------------------

/**
 * Handles GET requests, primarily for the OAuth callback.
 * @param {object} e The event parameter for a GET request.
 * @return {HtmlOutput} The HTML response for the callback.
 */
function doGet(e) {
  Logger.log("doGet called");
  const params = e.parameter;

  // Redact sensitive info from logs
  const loggableParams = { ...params };
  if (loggableParams.code) loggableParams.code = 'REDACTED_AUTH_CODE';
  if (loggableParams.access_token) loggableParams.access_token = 'REDACTED_ACCESS_TOKEN';
  Logger.log("doGet: Request params: " + JSON.stringify(loggableParams));

  // Standard OAuth2 callback handling
  if (params.code || params.error) {
    Logger.log("doGet: Detected OAuth2 callback.");
    const service = getOAuthService(); // Initialize service for callback

    if (params.error) {
      const error = params.error;
      const errorDescription = params.error_description || 'No description provided.';
      const errorUri = params.error_uri || 'No URI provided.';
      Logger.log(`doGet: OAuth2 error callback: ${error} - ${errorDescription} - ${errorUri}`);
      var friendlyMessage = `Authorization failed: ${error}. ${errorDescription}`;
      if (error === 'access_denied') {
        friendlyMessage = 'Access was denied. Please grant the requested permissions to use this connector.';
      }
      return HtmlService.createHtmlOutput(
        `<h1>Authorization Error</h1><p>${friendlyMessage}</p><p>Please try authorizing again from Looker Studio.</p>`
      );
    }

    try {
      const authorized = service.handleCallback(e); // Process the callback

      if (authorized) {
        // Token storage is handled by the OAuth2 library and our custom token property saving
        // in getAccessToken/refreshAccessToken and potentially here if needed.
        // The library itself stores tokens in PropertiesService.getUserProperties() under 'oauth2.TikTok'
        // We also explicitly store TIKTOK_ACCESS_TOKEN, TIKTOK_REFRESH_TOKEN, TIKTOK_TOKEN_EXPIRY

        // Explicitly save tokens after successful authorization callback
        const userProps = PropertiesService.getUserProperties();
        const accessToken = service.getAccessToken(); // Get freshly obtained token
        userProps.setProperty('TIKTOK_ACCESS_TOKEN', accessToken);

        const tokenData = service.getToken(); // Contains more details like refresh_token, expires_in
        if (tokenData) {
          if (tokenData.refresh_token) {
            userProps.setProperty('TIKTOK_REFRESH_TOKEN', tokenData.refresh_token);
          }
          if (tokenData.expires_in) { // expires_in is in seconds
            const expiryTime = Date.now() + (parseInt(tokenData.expires_in, 10) * 1000);
            userProps.setProperty('TIKTOK_TOKEN_EXPIRY', expiryTime.toString());
          }
        }
        Logger.log("doGet: OAuth2 authorization successful. Tokens stored.");
        return HtmlService.createHtmlOutput(
          '<h1>Success!</h1><p>TikTok authentication complete. You can now close this tab and return to Looker Studio.</p><script>setTimeout(function(){ window.close(); }, 3000);</script>'
        );
      } else {
        Logger.log("doGet: OAuth2 authorization denied by service.handleCallback.");
        return HtmlService.createHtmlOutput(
          '<h1>Authorization Denied</h1><p>The authorization was not successful. Please try again from Looker Studio.</p>'
        );
      }
    } catch (err) {
      Logger.log("doGet: Exception in OAuth2 callback flow: " + err.toString() + " Stack: " + err.stack);
      return HtmlService.createHtmlOutput(
        `<h1>Authentication Error</h1><p>An unexpected error occurred during authentication: ${err.message}</p><p>Please try again.</p>`
      );
    }
  }

  // Default view if not a callback (e.g., direct access to script URL)
  Logger.log("doGet: No OAuth2 callback parameters detected. Displaying default page.");
  return HtmlService.createHtmlOutput(
    '<h1>TikTok Looker Studio Connector</h1><p>This is the OAuth2 callback endpoint. Please initiate authentication from Looker Studio.</p>'
  );
}


/**
 * Configures the OAuth2 service for TikTok.
 * @return {Service} The configured OAuth2 service.
 */
function getOAuthService() {
  Logger.log('getOAuthService called: Initializing OAuth2 service.');
  var scriptProps = PropertiesService.getScriptProperties();
  var clientId = scriptProps.getProperty('TIKTOK_CLIENT_ID'); // This is the Client Key from TikTok
  var clientSecret = scriptProps.getProperty('TIKTOK_CLIENT_SECRET');

  if (!clientId || !clientSecret) {
    var errorMessage = 'OAuth Error: Client ID (Key) or Secret not set in Script Properties. Please ensure TIKTOK_CLIENT_ID and TIKTOK_CLIENT_SECRET are set by the admin.';
    Logger.log(errorMessage);
    // This error should ideally be caught and shown to the user in Looker Studio
    // For now, throwing an error will halt execution.
    throw new Error(errorMessage);
  }
  Logger.log('getOAuthService: Client ID and Secret are set.');

  const currentClientId = clientId; // For closure in setTokenPayloadHandler
  const currentClientSecret = clientSecret; // For closure

  // The redirect URI must be exactly what is registered with TikTok and whitelisted.
  // It's the URL of the deployed Apps Script.
  // You can get this from File > Project properties > Info > Head deployment ID, then construct URL.
  // Or by deploying as Web App and using that URL.
  // Example: 'https://script.google.com/macros/s/YOUR_DEPLOYMENT_ID/exec'
  const redirectUri = ScriptApp.getService().getUrl(); // Gets the /dev or /exec URL based on context
  Logger.log('getOAuthService: Redirect URI determined as: ' + redirectUri);


  return OAuth2.createService('TikTok')
    .setAuthorizationBaseUrl('https://www.tiktok.com/v2/auth/authorize/') // Ensure trailing slash if API expects
    .setTokenUrl('https://open.tiktokapis.com/v2/oauth/token/')     // Ensure trailing slash if API expects
    .setClientId(clientId) // Standard OAuth2 library field, though TikTok uses client_key in payload
    .setClientSecret(clientSecret) // Standard OAuth2 library field
    .setCallbackFunction('doGet') // Function in this script to handle the callback
    .setPropertyStore(PropertiesService.getUserProperties()) // Where to store OAuth tokens
    .setScope('user.info.stats,user.info.profile,video.list,video.list.beta') // Added video.list.beta if needed, check TikTok docs
    // .setRedirectUri(redirectUri) // The library usually infers this, but can be set explicitly.
                                  // Make sure this matches exactly what's in TikTok dev portal.
                                  // Using ScriptApp.getService().getUrl() is often more reliable for /exec.
                                  // The provided code had a hardcoded one, which is also common.
                                  // Let's use the hardcoded one from the original script for consistency with potential existing setups.
    .setRedirectUri('https://script.google.com/macros/s/AKfycbyCcIeaqb2K8X_WeEJexpmgSOOg0bCEqm8ZsssnrmDIKkbcNINlvqagHjjLm63AugH_/exec')


    // TikTok requires 'client_key' instead of 'client_id' in the token exchange payload.
    // It also requires client_secret in the payload.
    .setTokenPayloadHandler(function(payload) {
      Logger.log('setTokenPayloadHandler: Original token payload from library: ' + JSON.stringify(payload));
      // The library's default payload for authorization_code grant usually includes:
      // code, client_id, client_secret, redirect_uri, grant_type.
      // For refresh_token grant:
      // refresh_token, client_id, client_secret, grant_type.

      const tiktokPayload = {
        client_key: currentClientId,    // TikTok specific
        client_secret: currentClientSecret, // TikTok specific
        grant_type: payload.grant_type,
        redirect_uri: payload.redirect_uri // Required for auth code grant
      };

      if (payload.grant_type === 'authorization_code') {
        tiktokPayload.code = payload.code;
      } else if (payload.grant_type === 'refresh_token') {
        tiktokPayload.refresh_token = payload.refresh_token || PropertiesService.getUserProperties().getProperty('TIKTOK_REFRESH_TOKEN');
        // For refresh, redirect_uri is typically not needed in the payload by most OAuth servers.
        // Check TikTok docs if it's required for refresh token grant. If not, remove it.
        // For now, keeping it as the library might include it.
        // delete tiktokPayload.redirect_uri; // If not needed for refresh
      }
      Logger.log('setTokenPayloadHandler: Modified token payload for TikTok: ' + JSON.stringify(tiktokPayload));
      return tiktokPayload;
    })
    .setLock(LockService.getUserLock()); // Use lock to prevent concurrent execution issues with token refresh
}

/**
 * Checks if the user has valid authentication.
 * @return {boolean} True if the user has access, false otherwise.
 */
function isAuthValid() {
  Logger.log('isAuthValid called.');
  try {
    var service = getOAuthService();
    // service.hasAccess() checks for a stored access token and might try to refresh it if it's expired
    // and a refresh token is available and the library is configured to do so.
    var hasAccess = service.hasAccess();
    Logger.log('isAuthValid: OAuth2 service.hasAccess() result: ' + hasAccess);

    // Additional check for our explicitly stored token and its expiry,
    // as service.hasAccess() might not align perfectly with our custom expiry logic.
    if (hasAccess) {
        const explicitToken = PropertiesService.getUserProperties().getProperty('TIKTOK_ACCESS_TOKEN');
        if (!explicitToken || isAccessTokenExpired()) {
            Logger.log('isAuthValid: Service has access, but explicit token is missing or expired. Attempting to use getAccessToken to force refresh if needed.');
            // Calling getAccessToken will attempt a refresh if our custom logic deems it necessary.
            return !!getAccessToken(); // Check if getAccessToken successfully returns a token
        }
    }
    return hasAccess;

  } catch (e) {
    Logger.log('isAuthValid: Error checking auth status: ' + e.toString());
    return false; // Assume not valid if an error occurs
  }
}

/**
 * Resets the OAuth2 authorization, removing stored tokens.
 */
function resetAuth() {
  Logger.log('resetAuth called');
  try {
    var service = getOAuthService();
    service.reset(); // Clears tokens stored by the OAuth2 library (prefixed with 'oauth2.')

    // Also clear our custom-stored token properties
    var userProps = PropertiesService.getUserProperties();
    const customTokenKeys = ['TIKTOK_ACCESS_TOKEN', 'TIKTOK_REFRESH_TOKEN', 'TIKTOK_TOKEN_EXPIRY'];
    customTokenKeys.forEach(key => {
      userProps.deleteProperty(key);
      Logger.log(`Deleted custom property: ${key}`);
    });

    Logger.log('resetAuth: OAuth service reset and custom token properties deleted.');
  } catch (e) {
    Logger.log('resetAuth: Error during resetAuth: ' + e.toString());
    // Optionally, inform the user if this fails, though it's an internal cleanup.
  }
}

/**
 * Generates the authorization URL for the user to click.
 * This is called by Looker Studio when it needs the user to authorize.
 * @return {string} The authorization URL.
 */
function get3PAuthorizationUrls() {
  Logger.log('get3PAuthorizationUrls called.');
  try {
    var service = getOAuthService(); // Ensure service is configured
    // The OAuth2 library's getAuthorizationUrl() method constructs the URL
    // using the configured base URL, client ID, scope, redirect URI, etc.
    // It also typically includes a 'state' parameter for security.
    return service.getAuthorizationUrl();
  } catch (e) {
    Logger.log('Error in get3PAuthorizationUrls: ' + e.toString());
    // This error will be shown to the user in Looker Studio if thrown.
    cc.newUserError()
      .setDebugText('Error generating authorization URL: ' + e.toString())
      .setText('Could not initiate authentication with TikTok. Please check connector script properties (Client ID/Secret) or try again.')
      .throwException();
    return ''; // Should not be reached due to throwException
  }
}


// ------------------------ Token Management Helpers ------------------------

/**
 * Retrieves a valid access token, refreshing if necessary using our custom logic.
 * @return {string|null} The valid access token, or null if unavailable/refresh failed.
 */
function getAccessToken() {
  Logger.log('getAccessToken: Attempting to retrieve or refresh token.');
  var userProps = PropertiesService.getUserProperties();
  var accessToken = userProps.getProperty('TIKTOK_ACCESS_TOKEN');

  if (accessToken && !isAccessTokenExpired()) {
    Logger.log('getAccessToken: Found valid, non-expired explicit access token.');
    return accessToken;
  }

  // Token is either missing or expired, try to use the OAuth2 service, which might refresh.
  Logger.log('getAccessToken: Explicit token missing or expired. Consulting OAuth2 service.');
  try {
    var service = getOAuthService();
    if (service.hasAccess()) { // This can trigger a refresh if the library handles it.
      accessToken = service.getAccessToken();
      Logger.log('getAccessToken: Token retrieved/refreshed via service.hasAccess().');
      // Update our custom properties after the library might have refreshed.
      userProps.setProperty('TIKTOK_ACCESS_TOKEN', accessToken);
      const tokenData = service.getToken(); // Get full token data from service
      if (tokenData) {
        if (tokenData.refresh_token) {
          userProps.setProperty('TIKTOK_REFRESH_TOKEN', tokenData.refresh_token);
        }
        if (tokenData.expires_in) {
          const expiryTime = Date.now() + (parseInt(tokenData.expires_in, 10) * 1000);
          userProps.setProperty('TIKTOK_TOKEN_EXPIRY', expiryTime.toString());
          Logger.log('getAccessToken: Updated custom token properties from service. New expiry: ' + new Date(expiryTime));
        }
      }
      return accessToken;
    } else {
      Logger.log('getAccessToken: service.hasAccess() returned false. No token available via service.');
      // If service.hasAccess() is false, it means either no token or refresh failed.
      // We might need to explicitly call our refresh logic if the library didn't handle it.
      // However, the original code relied on service.hasAccess() to do the heavy lifting.
      // If that fails, it implies a more fundamental auth issue.
      resetAuth(); // Clear potentially bad state
      return null;
    }
  } catch (e) {
    Logger.log('getAccessToken: Error interacting with OAuth2 service: ' + e.toString() + ' Stack: ' + e.stack);
    resetAuth(); // Clear potentially bad state on error
    return null;
  }
}


/**
 * Checks if the stored access token is expired based on our custom stored expiry time.
 * @return {boolean} True if the token is expired or expiry info is missing, false otherwise.
 */
function isAccessTokenExpired() {
  const expiryTimeStr = PropertiesService.getUserProperties().getProperty('TIKTOK_TOKEN_EXPIRY');
  if (!expiryTimeStr) {
    Logger.log('isAccessTokenExpired: No TIKTOK_TOKEN_EXPIRY property found. Assuming expired.');
    return true; // Assume expired if we don't know when it expires
  }
  const expiryTime = parseInt(expiryTimeStr, 10);
  if (isNaN(expiryTime)) {
    Logger.log('isAccessTokenExpired: TIKTOK_TOKEN_EXPIRY is not a valid number. Assuming expired.');
    return true;
  }
  const currentTime = Date.now();
  // Buffer to consider token expired a bit before it actually does, to allow time for refresh.
  const buffer = 5 * 60 * 1000; // 5 minutes buffer
  const isExpired = currentTime >= (expiryTime - buffer);

  Logger.log('isAccessTokenExpired: CurrentTime=' + new Date(currentTime) +
             ', ExpiryTime=' + new Date(expiryTime) +
             ', Buffer=' + (buffer / 1000) + 's' +
             ', IsExpired=' + isExpired);
  return isExpired;
}

/**
 * Refreshes the access token using the library's refresh capability.
 * Note: This function was part of the original script but `getAccessToken` now primarily relies on
 * `service.hasAccess()` to trigger refreshes. This function can be kept for explicit refresh calls if needed,
 * or integrated more directly if `service.hasAccess()` isn't sufficient for all refresh scenarios.
 * For now, it's less central to the token retrieval logic.
 * @return {boolean} True if refresh was successful, false otherwise.
 */
function refreshAccessTokenUsingLibrary() { // Renamed to clarify it uses the library
  Logger.log('refreshAccessTokenUsingLibrary: Attempting library refresh.');
  var userProps = PropertiesService.getUserProperties();
  // The refresh token for the library is typically stored as 'oauth2.TikTok.refresh_token'
  // or our custom 'TIKTOK_REFRESH_TOKEN' might be used by setTokenPayloadHandler.
  // The library's service.refresh() should handle this.

  try {
    var service = getOAuthService();
    Logger.log("refreshAccessTokenUsingLibrary: Calling service.refresh().");
    var success = service.refresh(); // Ask the library to refresh

    if (success) {
      Logger.log("refreshAccessTokenUsingLibrary: Library refresh reported successful.");
      var newAccessToken = service.getAccessToken();
      userProps.setProperty('TIKTOK_ACCESS_TOKEN', newAccessToken);
      Logger.log("refreshAccessTokenUsingLibrary: Saved new access token from library. Length: " + (newAccessToken ? newAccessToken.length : '0'));

      const tokenData = service.getToken(); // Get full token data
      if (tokenData) {
        if (tokenData.refresh_token) { // TikTok might issue a new refresh token
          userProps.setProperty('TIKTOK_REFRESH_TOKEN', tokenData.refresh_token);
          Logger.log("refreshAccessTokenUsingLibrary: Saved updated refresh token from library.");
        }
        if (tokenData.expires_in) {
          const expiryTime = Date.now() + (parseInt(tokenData.expires_in, 10) * 1000);
          userProps.setProperty('TIKTOK_TOKEN_EXPIRY', expiryTime.toString());
          Logger.log("refreshAccessTokenUsingLibrary: Updated token expiry time: " + new Date(expiryTime));
        } else {
           // If expires_in is not returned, we might need a default or clear old one.
           // For now, if not present, the old expiry might persist or be absent.
           Logger.log("refreshAccessTokenUsingLibrary: No expires_in in token data after refresh.");
        }
      }
      return true;
    } else {
      Logger.log("refreshAccessTokenUsingLibrary: Library refresh method returned false. This might mean refresh token is invalid.");
      resetAuth(); // If refresh fails, tokens are likely invalid.
      return false;
    }
  } catch (e) {
    Logger.log("refreshAccessTokenUsingLibrary: Error during library refresh: " + e.toString() + ' Stack: ' + e.stack);
    if (e.message && (e.message.toLowerCase().includes('invalid_grant') ||
                      e.message.toLowerCase().includes('token has been revoked') ||
                      e.message.toLowerCase().includes('invalid_refresh_token'))) {
      Logger.log("refreshAccessTokenUsingLibrary: Refresh token likely invalid, expired, or revoked. Resetting auth.");
      resetAuth();
    }
    return false;
  }
}


// ------------------------ Admin Functions ------------------------
/**
 * Required function, determines if the current user is an admin.
 * This function is used by Looker Studio to restrict access to
 * certain connector features (e.g., configuration if it were admin-configurable).
 * @return {boolean} True if the user is an admin, false otherwise.
 */
function isAdminUser() {
  // In a real-world scenario, you would check if the user's email
  // (ScriptApp.getEffectiveUser().getEmail() or Session.getEffectiveUser().getEmail())
  // is in a list of admin users, or use a more robust admin check.
  // For this community connector, typically this is false unless specific admin config is needed.
  Logger.log('isAdminUser called. Current effective user: ' + Session.getEffectiveUser().getEmail());
  return false; // Defaulting to false, meaning no special admin privileges in the connector UI.
}
