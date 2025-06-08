/**
 * Looker Studio Community Connector for TikTok User Info & Video Info API
 * Version: 9.0.0 (Production-ready with enhanced security)
 * 
 * Description: This connector retrieves user and video information from the TikTok API
 * and makes it available in Looker Studio. It handles OAuth2 authentication,
 * data fetching, and schema definition with improved security and reliability.
 */

// Global connector instance
var cc = DataStudioApp.createCommunityConnector();

// ------------------------ Constants & Configuration ------------------------

// API Configuration
const TIKTOK_API_BASE_URL = 'https://open.tiktokapis.com/v2/';
const TIKTOK_AUTH_URL = 'https://www.tiktok.com/v2/auth/authorize/';
const TIKTOK_TOKEN_URL = 'https://open.tiktokapis.com/v2/oauth/token/';
const MAX_VIDEOS_TO_FETCH = 200; // Safe default limit to prevent excessive API usage
const MAX_API_RETRIES = 3;
const API_RATE_LIMIT_DELAY_MS = 500;

// Token management constants
const TOKEN_EXPIRY_BUFFER_MS = 5 * 60 * 1000; // 5 minutes buffer for token expiry
const TOKEN_PROPERTIES = {
  ACCESS_TOKEN: 'TIKTOK_ACCESS_TOKEN',
  REFRESH_TOKEN: 'TIKTOK_REFRESH_TOKEN',
  EXPIRY_TIME: 'TIKTOK_TOKEN_EXPIRY'
};

// OAuth Scopes required by the connector
const REQUIRED_SCOPES = [
  'user.info.stats',
  'user.info.profile',
  'video.list'
].join(',');

// ------------------------ Connector Configuration ------------------------

/**
 * Returns the authentication method required by the connector.
 * @return {object} The AuthType response.
 */
function getAuthType() {
  Logger.log('getAuthType called');
  return cc
    .newAuthTypeResponse()
    .setAuthType(cc.AuthType.OAUTH2)
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
    const accessToken = getAccessToken();
    if (!accessToken) {
      cc.newUserError()
        .setDebugText('No valid access token available')
        .setText('Authentication is required. Please re-authenticate the connector.')
        .throwException();
    }

    const requestedFields = getFields().forIds(request.fields.map(f => f.name));
    const dataRows = [];

    // Fetch User Info
    const userData = fetchUserInfo(accessToken);
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
      videosData = fetchPaginatedVideos(userData.open_id, accessToken, videoApiFields);
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
 * @param {string} accessToken The OAuth2 access token.
 * @return {object|null} The user data object or null on failure.
 */
function fetchUserInfo(accessToken) {
  const userApiFields = [
    'open_id', 'union_id', 'username', 'display_name',
    'bio_description', 'profile_deep_link', 'avatar_url',
    'avatar_url_100', 'avatar_large_url', 'is_verified',
    'follower_count', 'following_count', 'likes_count',
    'video_count'
  ];

  const options = {
    method: 'GET',
    headers: { 
      'Authorization': 'Bearer ' + accessToken,
      'Content-Type': 'application/json'
    },
    muteHttpExceptions: true,
    validateHttpsCertificates: true // Ensure SSL certificate validation
  };

  const url = `${TIKTOK_API_BASE_URL}user/info/?fields=${encodeURIComponent(userApiFields.join(','))}`;
  Logger.log('Fetching user info from: ' + url);
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
 * @param {string} accessToken The OAuth2 access token.
 * @param {string[]} fields The list of video fields to request.
 * @param {number} maxVideos Maximum number of videos to fetch.
 * @return {object[]} An array of video data objects.
 */
function fetchPaginatedVideos(openId, accessToken, fields, maxVideos = MAX_VIDEOS_TO_FETCH) {
  const allVideos = [];
  let cursor = null;
  let hasMore = true;
  let requestCount = 0;
  const maxRequests = 10; // Prevent infinite loops

  const fieldQuery = encodeURIComponent(fields.join(','));
  const videoListUrl = `${TIKTOK_API_BASE_URL}video/list/?fields=${fieldQuery}`;

  Logger.log(`Fetching paginated videos. URL base: ${videoListUrl}, Max videos: ${maxVideos}`);

  while (hasMore && allVideos.length < maxVideos && requestCount < maxRequests) {
    requestCount++;
    Logger.log(`Fetching video page ${requestCount}. Current videos: ${allVideos.length}`);

    const payload = {
      max_count: 20 // Max allowed by TikTok API
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
      muteHttpExceptions: true,
      validateHttpsCertificates: true
    };

    const response = fetchWithRetry(videoListUrl, options);

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

// ------------------------ OAuth2 Handling ------------------------

/**
 * Handles GET requests for OAuth callback.
 * @param {object} e The event parameter for a GET request.
 * @return {HtmlOutput} The HTML response.
 */
function doGet(e) {
  Logger.log("doGet called with params: " + JSON.stringify(redactSensitiveInfo(e.parameter)));
  
  const params = e.parameter;
  if (params.code || params.error) {
    Logger.log("Processing OAuth2 callback.");
    const service = getOAuthService();

    if (params.error) {
      const errorDescription = params.error_description || 'No description provided.';
      let friendlyMessage = `Authorization failed: ${params.error}. ${errorDescription}`;
      if (params.error === 'access_denied') {
        friendlyMessage = 'Access was denied. Please grant the requested permissions.';
      }
      return HtmlService.createHtmlOutput(
        `<h1>Authorization Error</h1><p>${friendlyMessage}</p><p>Please try again from Looker Studio.</p>`
      );
    }

    try {
      const authorized = service.handleCallback(e);
      if (authorized) {
        // Store tokens explicitly
        const userProps = PropertiesService.getUserProperties();
        const accessToken = service.getAccessToken();
        userProps.setProperty(TOKEN_PROPERTIES.ACCESS_TOKEN, accessToken);

        const tokenData = service.getToken();
        if (tokenData) {
          if (tokenData.refresh_token) {
            userProps.setProperty(TOKEN_PROPERTIES.REFRESH_TOKEN, tokenData.refresh_token);
          }
          if (tokenData.expires_in) {
            const expiryTime = Date.now() + (parseInt(tokenData.expires_in, 10) * 1000);
            userProps.setProperty(TOKEN_PROPERTIES.EXPIRY_TIME, expiryTime.toString());
          }
        }
        
        return HtmlService.createHtmlOutput(
          '<h1>Success!</h1><p>TikTok authentication complete. Close this tab and return to Looker Studio.</p>' +
          '<script>setTimeout(function(){ window.close(); }, 2000);</script>'
        );
      } else {
        return HtmlService.createHtmlOutput(
          '<h1>Authorization Denied</h1><p>The authorization was not successful. Please try again.</p>'
        );
      }
    } catch (err) {
      logError('Exception in OAuth2 callback', err);
      return HtmlService.createHtmlOutput(
        `<h1>Authentication Error</h1><p>An error occurred: ${err.message}</p><p>Please try again.</p>`
      );
    }
  }

  // Default view if not a callback
  return HtmlService.createHtmlOutput(
    '<h1>TikTok Looker Studio Connector</h1><p>Please initiate authentication from Looker Studio.</p>'
  );
}

/**
 * Configures the OAuth2 service for TikTok.
 * @return {Service} The configured OAuth2 service.
 */
function getOAuthService() {
  Logger.log('Initializing OAuth2 service.');
  const scriptProps = PropertiesService.getScriptProperties();
  const clientId = scriptProps.getProperty('TIKTOK_CLIENT_ID');
  const clientSecret = scriptProps.getProperty('TIKTOK_CLIENT_SECRET');

  if (!clientId || !clientSecret) {
    const errorMessage = 'Client ID or Secret not set in Script Properties.';
    Logger.log(errorMessage);
    throw new Error(errorMessage);
  }

  // Use the service URL as redirect URI
  const redirectUri = "https://script.google.com/macros/s/AKfycbyCcIeaqb2K8X_WeEJexpmgSOOg0bCEqm8ZsssnrmDIKkbcNINlvqagHjjLm63AugH_/exec"

  return OAuth2.createService('TikTok')
    .setAuthorizationBaseUrl(TIKTOK_AUTH_URL)
    .setTokenUrl(TIKTOK_TOKEN_URL)
    .setClientId(clientId)
    .setParam('client_key', clientId) // TikTok expects this
    .setClientSecret(clientSecret)
    .setCallbackFunction('doGet')
    .setPropertyStore(PropertiesService.getUserProperties())
    .setScope(REQUIRED_SCOPES)
    .setRedirectUri(redirectUri)
    .setTokenPayloadHandler(function(payload) {
      // Transform the payload to TikTok's requirements
      const tiktokPayload = {
        client_key: clientId,  // Use client_key instead of client_id
        client_secret: clientSecret,
        grant_type: payload.grant_type
      };

      if (payload.grant_type === 'authorization_code') {
        tiktokPayload.code = payload.code;
        tiktokPayload.redirect_uri = payload.redirect_uri;
      } else if (payload.grant_type === 'refresh_token') {
        tiktokPayload.refresh_token = payload.refresh_token || 
          PropertiesService.getUserProperties().getProperty(TOKEN_PROPERTIES.REFRESH_TOKEN);
      }
      return tiktokPayload;
    })
    .setLock(LockService.getUserLock());
}

/**
 * Checks if the user has valid authentication.
 * @return {boolean} True if authenticated, false otherwise.
 */
function isAuthValid() {
  Logger.log('Checking authentication status.');
  try {
    const service = getOAuthService();
    const hasAccess = service.hasAccess();
    Logger.log('OAuth2 service.hasAccess() result: ' + hasAccess);

    if (hasAccess) {
      const explicitToken = PropertiesService.getUserProperties().getProperty(TOKEN_PROPERTIES.ACCESS_TOKEN);
      if (!explicitToken || isAccessTokenExpired()) {
        Logger.log('Checking token via getAccessToken.');
        return !!getAccessToken();
      }
    }
    return hasAccess;
  } catch (e) {
    logError('Error checking auth status', e);
    return false;
  }
}

/**
 * Resets the OAuth2 authorization.
 */
function resetAuth() {
  Logger.log('Resetting authentication.');
  try {
    const service = getOAuthService();
    service.reset();

    const userProps = PropertiesService.getUserProperties();
    Object.values(TOKEN_PROPERTIES).forEach(key => {
      userProps.deleteProperty(key);
    });
  } catch (e) {
    logError('Error during resetAuth', e);
  }
}

/**
 * Generates the authorization URL.
 * @return {string} The authorization URL.
 */
function get3PAuthorizationUrls() {
  Logger.log('Generating authorization URL.');
  try {
    const service = getOAuthService();
    return service.getAuthorizationUrl();
  } catch (e) {
    logError('Error generating authorization URL', e);
    cc.newUserError()
      .setDebugText('Error generating authorization URL: ' + e.message)
      .setText('Could not initiate authentication with TikTok.')
      .throwException();
  }
}

// ------------------------ Token Management ------------------------

/**
 * Retrieves a valid access token, refreshing if necessary.
 * @return {string|null} The access token or null if unavailable.
 */
function getAccessToken() {
  Logger.log('Retrieving access token.');
  const userProps = PropertiesService.getUserProperties();
  let accessToken = userProps.getProperty(TOKEN_PROPERTIES.ACCESS_TOKEN);

  if (accessToken && !isAccessTokenExpired()) {
    Logger.log('Using valid existing token.');
    return accessToken;
  }

  Logger.log('Token needs refresh.');
  try {
    const service = getOAuthService();
    if (service.hasAccess()) {
      accessToken = service.getAccessToken();
      userProps.setProperty(TOKEN_PROPERTIES.ACCESS_TOKEN, accessToken);

      const tokenData = service.getToken();
      if (tokenData) {
        if (tokenData.refresh_token) {
          userProps.setProperty(TOKEN_PROPERTIES.REFRESH_TOKEN, tokenData.refresh_token);
        }
        if (tokenData.expires_in) {
          const expiryTime = Date.now() + (parseInt(tokenData.expires_in, 10) * 1000);
          userProps.setProperty(TOKEN_PROPERTIES.EXPIRY_TIME, expiryTime.toString());
        }
      }
      return accessToken;
    } else {
      Logger.log('service.hasAccess() returned false.');
      resetAuth();
      return null;
    }
  } catch (e) {
    logError('Error getting access token', e);
    resetAuth();
    return null;
  }
}

/**
 * Checks if the access token is expired.
 * @return {boolean} True if expired, false otherwise.
 */
function isAccessTokenExpired() {
  const expiryTimeStr = PropertiesService.getUserProperties().getProperty(TOKEN_PROPERTIES.EXPIRY_TIME);
  if (!expiryTimeStr) {
    Logger.log('No expiry time found. Assuming expired.');
    return true;
  }

  const expiryTime = parseInt(expiryTimeStr, 10);
  if (isNaN(expiryTime)) {
    Logger.log('Invalid expiry time. Assuming expired.');
    return true;
  }

  const currentTime = Date.now();
  const isExpired = currentTime >= (expiryTime - TOKEN_EXPIRY_BUFFER_MS);

  Logger.log(`Token expiry check: Current=${new Date(currentTime)}, Expiry=${new Date(expiryTime)}, IsExpired=${isExpired}`);
  return isExpired;
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
