# Social Insights Studio

![Version](https://img.shields.io/badge/Version-9.0.0-blue.svg)

This is a Google Apps Script-based Community Connector designed to pull user profile information and a list of their videos, complete with metrics, from the TikTok Open Platform API. It makes this data available in Google Looker Studio (formerly Google Data Studio) and uses a backend OAuth provider to handle TikTok authentication securely.

The connector is built to be production-ready, featuring enhanced security and reliability. It leverages a secure OAuth2 flow for authentication and handles the complexities of API interaction, including pagination, error handling, and token management, allowing you to create insightful dashboards and reports from your TikTok account data.

## Features

* **Secure Authentication**: Uses a backend OAuth provider to connect to your TikTok account, ensuring credentials are never stored in Apps Script.
* **Encrypted Token Storage**: Persists TikTok tokens to disk using AES-256-GCM encryption with atomic writes and pruning.
* **Automatic Token Refresh**: Automatically handles the refreshing of access tokens to maintain continuous data access.
* **Comprehensive User Data**: Fetches a wide range of user profile details, including username, bio, avatar URLs, verification status, and key metrics like follower, following, and total like counts.
* **Detailed Video Data**: Retrieves in-depth information and performance metrics for an account's videos, including descriptions, URLs, duration, and counts for likes, comments, shares, and views.
* **Video Pagination**: Automatically fetches multiple pages of video data, up to a safe limit of 200 videos, to provide a comprehensive dataset.
* **Robust Error Handling**: Features built-in logic to handle API errors gracefully, with an automatic retry mechanism for transient issues to ensure data reliability.
* **Rate Limit Management**: Respects API rate limits by introducing delays between paginated requests to prevent API throttling.

## Data Schema

The connector provides the following fields, which are defined in the script and fetched from the TikTok API.

### User Dimensions

| Field ID | Field Name | Description |
| :--- | :--- | :--- |
| `user_open_id` | User Open ID | Unique identifier for the TikTok user. |
| `user_union_id` | Union ID | Union ID for the user (if available). |
| `user_username` | Username | TikTok username. |
| `user_display_name` | Display Name | User's display name. |
| `user_bio_description` | Bio Description | User profile bio. |
| `user_profile_deep_link` | Profile Deep Link | Direct link to user profile. |
| `user_avatar_url` | Avatar URL | URL of user profile picture. |
| `user_avatar_url_100` | Avatar URL (100px) | URL of 100px profile picture. |
| `user_avatar_large_url` | Avatar URL (Large) | URL of large profile picture. |
| `user_is_verified` | Is Verified | Whether the user is verified. |

### User Metrics

| Field ID | Field Name | Description | Default Aggregation |
| :--- | :--- | :--- | :--- |
| `user_follower_count` | Follower Count | Number of followers. | SUM |
| `user_following_count`| Following Count | Number of accounts followed. | SUM |
| `user_likes_count` | Total Likes Received | Total likes on user's videos. | SUM |
| `user_video_count` | Total Video Count | Number of videos posted. | SUM |

### Video Dimensions

| Field ID | Field Name | Description |
| :--- | :--- | :--- |
| `video_id` | Video ID | Unique identifier for the video. |
| `video_create_time` | Video Create Time | When the video was created (YYYYMMDDHH). |
| `video_cover_image_url` | Video Cover Image URL | URL of video thumbnail. |
| `video_share_url` | Video Share URL | URL to share the video. |
| `video_description` | Video Description | Caption/text description of video. |
| `video_title` | Video Title | Title of the video. |
| `video_embed_html` | Video Embed HTML | HTML code to embed the video. |
| `video_embed_link` | Video Embed Link | URL to embed the video. |

### Video Metrics

| Field ID | Field Name | Description | Default Aggregation |
| :--- | :--- | :--- | :--- |
| `video_duration` | Video Duration (seconds) | Length of video in seconds. | AVG |
| `video_height` | Video Height | Height of video in pixels. | MAX |
| `video_width` | Video Width | Width of video in pixels. | MAX |
| `video_like_count` | Video Like Count | Number of likes on video. | SUM |
| `video_comment_count` | Video Comment Count | Number of comments on video. | SUM |
| `video_share_count` | Video Share Count | Number of shares of video. | SUM |
| `video_view_count` | Video View Count | Number of views on video. | SUM |

## Setup and Installation

Follow these steps carefully to set up and deploy your connector.

### Step 1: Create a TikTok Developer Application

1. Navigate to the [TikTok Developer Console](https://developers.tiktok.com/).
2. Log in and create a new application.
3. Fill in the required application details.
4. Under your app settings, ensure you have requested and been granted access to the following scopes:
   * `user.info.stats`
   * `user.info.profile`
   * `video.list`
5. Go to the **App credentials** section and note down your **Client key** and **Client secret**.
6. You must configure the **Redirect URI**. This will be your backend callback URL (configured in Step 2).

### Step 2: Configure the Backend Service

1. Copy `.env.example` to `.env` and fill in values (or set environment variables directly in your hosting panel).
2. Required backend environment variables:
   * `BASE_URL`: The HTTPS base URL of your backend (e.g., `https://lstc.nixorcorporate.com`).
   * `TIKTOK_CLIENT_KEY`: TikTok Client Key from Step 1.
   * `TIKTOK_CLIENT_SECRET`: TikTok Client Secret from Step 1.
   * `ENCRYPTION_KEY`: 32-byte key (base64 or hex) for AES-256-GCM encryption.
   * `BACKEND_JWT_SECRET`: Secret used to sign backend JWTs for Looker Studio.
3. Recommended backend environment variables:
   * `TOKEN_STORE_PATH`: Absolute path outside the public web root (e.g., `/var/lib/social-insights-studio/tokens.json`).
   * `TOKEN_LOCK_PATH`: Lock file path in the same private directory.
   * `TRUST_PROXY`: Set to `1` when behind Passenger or a reverse proxy.
   * `ALLOWED_ORIGINS`: Comma-separated list of allowed CORS origins (leave blank for server-to-server only).
4. Start the backend from `server/` using `npm start`.
5. Update the TikTok **Redirect URI** to: `https://<your-domain>/auth/tiktok/callback`.
6. Ensure the token directory is private (`chmod 700`) and token files are restricted (`chmod 600`).

### Step 3: Create a Google Apps Script Project

1. Go to [Google Apps Script](https://script.google.com/home).
2. Click `New project` and give it a name (e.g., `Social Insights Studio Connector`).
3. Replace the default `Code.gs` content with the entire code provided.
4. **Add the OAuth2 for Apps Script Library**:
   * Click on **Libraries** (`+` icon) in the left sidebar.
   * In the "Script ID" field, paste: `1B7FSrg4E5WqmNcpecVPODk6Oztd_PlrxXh8HTMrdhr6Cw6yazg4m8PTK`
   * Click **Look up**. Select the latest version, ensure the "Identifier" is `OAuth2`, and click **Add**.

### Step 4: Configure Script Properties

1. In your Apps Script project, go to **Project settings** (gear icon).
2. Scroll down to **Script properties** and click `Add script property`.
3. Add one property:
   * `BACKEND_API_BASE_URL`: Your backend base URL (e.g., `https://lstc.nixorcorporate.com`).
4. Click `Save script properties`.

### Step 5: Deploy the Apps Script

1. In the Apps Script editor, click `Deploy > New deployment`.
2. From the "Select type" dropdown, choose `Web app`.
3. Configure the deployment:
   * **Execute as:** `Me` (your Google account).
   * **Who has access:** `Anyone`.
4. Click `Deploy`. Authorize the script if prompted.
5. After deployment, **copy the Web app URL** (used by Looker Studio).

### Step 6: Use in Looker Studio

1. Go to [Google Looker Studio](https://lookerstudio.google.com/) and open a data source or report.
2. In the connector gallery, search for and select the **"Build Your Own"** connector (sometimes called "Deploy from ID").
3. Go back to your Apps Script project, click `Deploy > Manage deployments`, and copy the **Deployment ID**.
4. Paste the Deployment ID into Looker Studio and click `Validate`.
5. Your connector should appear. Select it.
6. Click `Authorize` and follow the prompts to sign in via your backend and grant TikTok permissions.
7. Once authorized, click `Connect` to add the data source to your report.

## Configuration

### Apps Script Properties

The following properties must be set in your Apps Script project's `Project settings > Script properties`.

| Property Name | Description |
| :--- | :--- |
| `BACKEND_API_BASE_URL` | The base URL of your backend service. |

### Backend Environment Variables

| Property Name | Description |
| :--- | :--- |
| `BASE_URL` | Backend base URL (e.g., `https://lstc.nixorcorporate.com`). |
| `TIKTOK_CLIENT_KEY` | TikTok Developer App's **Client Key**. |
| `TIKTOK_CLIENT_SECRET`| TikTok Developer App's **Client Secret**. |
| `ENCRYPTION_KEY` | 32-byte key (base64 or hex) for encrypting TikTok tokens. |
| `BACKEND_JWT_SECRET` | Secret used to sign backend JWTs for Looker Studio. |
| `TOKEN_STORE_PATH` | Absolute path for encrypted token storage (outside public web root). |
| `TOKEN_LOCK_PATH` | Lock file path in the same private directory. |
| `TOKEN_PRUNE_DAYS` | Number of days before pruning expired refresh tokens (default: 30). |
| `LOOKER_CLIENT_ID` | Expected OAuth client ID for Looker Studio (default: `looker-studio-connector`). |
| `LOOKER_CLIENT_SECRET` | Expected OAuth client secret (default: `unused`). |
| `ALLOWED_ORIGINS` | Comma-separated list of allowed browser origins for API calls. |
| `TRUST_PROXY` | Set to `1` when running behind Passenger/reverse proxy. |
| `RATE_LIMIT_WINDOW_MINUTES` | Auth rate limit window in minutes (default: 15). |
| `RATE_LIMIT_MAX` | Max auth requests per window (default: 60). |
| `API_RATE_LIMIT_WINDOW_MINUTES` | API rate limit window in minutes (default: 5). |
| `API_RATE_LIMIT_MAX` | Max API requests per window (default: 120). |

## Production Deployment (cPanel/Passenger)

1. Upload the repo and set the Node.js application root to `/server`.
2. Set all required environment variables in cPanel's Node.js app settings (do not commit `.env` to git).
3. Create a private storage directory outside `public/`, for example: `/home/<user>/secure/social-insights/`.
4. Set permissions:
   * `chmod 700 /home/<user>/secure/social-insights/`
   * `chmod 600 /home/<user>/secure/social-insights/tokens.json` (after first run)
5. Set `TOKEN_STORE_PATH` and `TOKEN_LOCK_PATH` to files in that private directory.
6. Set `TRUST_PROXY=1` so Express honors `X-Forwarded-*` headers behind Passenger.
7. Restart the Passenger app to pick up configuration changes.

## API Scopes Used

This connector requests the following scopes from the TikTok API. Ensure they are enabled for your TikTok application in the TikTok Developer Console.

* `user.info.stats`
* `user.info.profile`
* `video.list`

## Troubleshooting

If you encounter issues, especially during authentication or data fetching:

* **Redirect URI Mismatch**: This is the most common setup problem. The error often appears after authorizing in TikTok. Ensure the backend callback URL (`/auth/tiktok/callback`) is copied exactly into the **Redirect URI** field in your TikTok Developer App settings.
* **Authorization Errors (`access_denied`)**: This error in the callback URL means you did not grant all the requested permissions in the TikTok pop-up window. You must approve all requested scopes.
* **API Errors (`invalid_token`, `permission_denied`, etc.)**: The connector will display specific error messages from TikTok.
  * `permission_denied` or `insufficient_scope`: Your TikTok App does not have the correct scopes enabled and approved.
  * `token_expired` or `access_token_invalid`: May require you to re-authenticate. In Looker Studio, edit the data source connection, revoke access, and authorize again.
  * `rate_limit_exceeded`: You have made too many API requests in a short period. The connector has built-in delays, but heavy usage can still trigger this.
* **No Data or "Failed to Fetch"**: Check the Apps Script execution logs (`View > Executions` in the Apps Script editor) for detailed error messages. This can provide clues about failed API calls or other script issues.

## Contributing

Feel free to open issues or submit pull requests if you find bugs or want to add new features.

## License

This project is open-source and available under the MIT License.
