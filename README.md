# TikTok User & Video Data Connector for Looker Studio

![Version](https://img.shields.io/badge/Version-9.0.0-blue.svg)

[cite_start]This is a Google Apps Script-based Community Connector designed to pull user profile information and a list of their videos, complete with metrics, from the TikTok Open Platform API.  It makes this data available in Google Looker Studio (formerly Google Data Studio).

[cite_start]The connector is built to be production-ready, featuring enhanced security and reliability.  [cite_start]It leverages a secure OAuth2 flow for authentication and handles the complexities of API interaction, including pagination, error handling, and token management, allowing you to create insightful dashboards and reports from your TikTok account data. 

## Features

* [cite_start]**Secure Authentication**: Implements the standard OAuth2 protocol to securely connect to your TikTok account, ensuring your credentials are not stored directly in the script. 
* [cite_start]**Automatic Token Refresh**: Automatically handles the refreshing of access tokens to maintain continuous data access. 
* [cite_start]**Comprehensive User Data**: Fetches a wide range of user profile details, including username, bio, avatar URLs, verification status, and key metrics like follower, following, and total like counts. 
* [cite_start]**Detailed Video Data**: Retrieves in-depth information and performance metrics for an account's videos, including descriptions, URLs, duration, and counts for likes, comments, shares, and views. 
* [cite_start]**Video Pagination**: Automatically fetches multiple pages of video data, up to a safe limit of 200 videos, to provide a comprehensive dataset. 
* [cite_start]**Robust Error Handling**: Features built-in logic to handle API errors gracefully, with an automatic retry mechanism for transient issues to ensure data reliability. 
* [cite_start]**Rate Limit Management**: Respects API rate limits by introducing delays between paginated requests to prevent API throttling. 

## Data Schema

[cite_start]The connector provides the following fields, which are defined in the script and fetched from the TikTok API. 

### User Dimensions

| Field ID | Field Name | Description |
| :--- | :--- | :--- |
| `user_open_id` | User Open ID | [cite_start]Unique identifier for the TikTok user.  |
| `user_union_id` | Union ID | [cite_start]Union ID for the user (if available).  |
| `user_username` | Username | [cite_start]TikTok username.  |
| `user_display_name` | Display Name | [cite_start]User's display name.  |
| `user_bio_description` | Bio Description | [cite_start]User profile bio.  |
| `user_profile_deep_link` | Profile Deep Link | [cite_start]Direct link to user profile.  |
| `user_avatar_url` | Avatar URL | [cite_start]URL of user profile picture.  |
| `user_avatar_url_100` | Avatar URL (100px) | [cite_start]URL of 100px profile picture.  |
| `user_avatar_large_url` | Avatar URL (Large) | [cite_start]URL of large profile picture.  |
| `user_is_verified` | Is Verified | [cite_start]Whether the user is verified.  |

### User Metrics

| Field ID | Field Name | Description | Default Aggregation |
| :--- | :--- | :--- | :--- |
| `user_follower_count` | Follower Count | [cite_start]Number of followers.  | SUM |
| `user_following_count`| Following Count | [cite_start]Number of accounts followed.  | SUM |
| `user_likes_count` | Total Likes Received | [cite_start]Total likes on user's videos.  | SUM |
| `user_video_count` | Total Video Count | [cite_start]Number of videos posted.  | SUM |

### Video Dimensions

| Field ID | Field Name | Description |
| :--- | :--- | :--- |
| `video_id` | Video ID | [cite_start]Unique identifier for the video.  |
| `video_create_time` | Video Create Time | [cite_start]When the video was created (YYYYMMDDHH).  |
| `video_cover_image_url` | Video Cover Image URL | [cite_start]URL of video thumbnail.  |
| `video_share_url` | Video Share URL | [cite_start]URL to share the video.  |
| `video_description` | Video Description | [cite_start]Caption/text description of video.  |
| `video_title` | Video Title | [cite_start]Title of the video.  |
| `video_embed_html` | Video Embed HTML | [cite_start]HTML code to embed the video.  |
| `video_embed_link` | Video Embed Link | [cite_start]URL to embed the video.  |

### Video Metrics

| Field ID | Field Name | Description | Default Aggregation |
| :--- | :--- | :--- | :--- |
| `video_duration` | Video Duration (seconds) | [cite_start]Length of video in seconds.  | AVG |
| `video_height` | Video Height | [cite_start]Height of video in pixels.  | MAX |
| `video_width` | Video Width | [cite_start]Width of video in pixels.  | MAX |
| `video_like_count` | Video Like Count | [cite_start]Number of likes on video.  | SUM |
| `video_comment_count` | Video Comment Count | [cite_start]Number of comments on video.  | SUM |
| `video_share_count` | Video Share Count | [cite_start]Number of shares of video.  | SUM |
| `video_view_count` | Video View Count | [cite_start]Number of views on video.  | SUM |

## Setup and Installation

Follow these steps carefully to set up and deploy your connector.

### Step 1: Create a TikTok Developer Application

1.  Navigate to the [TikTok Developer Console](https://developers.tiktok.com/).
2.  Log in and create a new application.
3.  Fill in the required application details.
4.  Under your app settings, ensure you have requested and been granted access to the following scopes:
    * [cite_start]`user.info.stats` 
    * [cite_start]`user.info.profile` 
    * [cite_start]`video.list` 
5.  [cite_start]Go to the **App credentials** section and note down your **Client key** and **Client secret**. 
6.  You must configure the **Redirect URI**. You will get the exact URL for this in Step 4. For now, you can use a placeholder.

### Step 2: Create a Google Apps Script Project

1.  Go to [Google Apps Script](https://script.google.com/home).
2.  Click `New project` and give it a name (e.g., `TikTok Looker Studio Connector`).
3.  Replace the default `Code.gs` content with the entire code provided.
4.  **Add the OAuth2 for Apps Script Library**:
    * Click on **Libraries** (`+` icon) in the left sidebar.
    * In the "Script ID" field, paste: `1B7FSrg4E5WqmNcpecVPODk6Oztd_PlrxXh8HTMrdhr6Cw6yazg4m8PTK`
    * Click **Look up**. Select the latest version, ensure the "Identifier" is `OAuth2`, and click **Add**.

### Step 3: Configure Script Properties

1.  In your Apps Script project, go to **Project settings** (gear icon).
2.  Scroll down to **Script properties** and click `Add script property`.
3.  [cite_start]Add two properties:
    * `TIKTOK_CLIENT_ID`: Paste your **Client Key** from Step 1.
    * `TIKTOK_CLIENT_SECRET`: Paste your **Client Secret** from Step 1.
4.  Click `Save script properties`.

### Step 4: Deploy the Apps Script

1.  In the Apps Script editor, click `Deploy > New deployment`.
2.  From the "Select type" dropdown, choose `Web app`.
3.  Configure the deployment:
    * **Execute as:** `Me` (your Google account).
    * **Who has access:** `Anyone`.
4.  Click `Deploy`. Authorize the script if prompted.
5.  After deployment, **copy the Web app URL**.
6.  **Update TikTok Redirect URI**: Go back to your TikTok Developer Console and paste the copied Web app URL into the **Redirect URI** field. [cite_start]The URL must be an exact match. 

### Step 5: Use in Looker Studio

1.  Go to [Google Looker Studio](https://lookerstudio.google.com/) and open a data source or report.
2.  In the connector gallery, search for and select the **"Build Your Own"** connector (sometimes called "Deploy from ID").
3.  Go back to your Apps Script project, click `Deploy > Manage deployments`, and copy the **Deployment ID**.
4.  Paste the Deployment ID into Looker Studio and click `Validate`.
5.  Your TikTok connector should appear. Select it.
6.  [cite_start]Click `Authorize` and follow the pop-up prompts to sign in to your TikTok account and grant the necessary permissions. 
7.  Once authorized, click `Connect` to add the data source to your report.

## Configuration

[cite_start]The following properties must be set in your Apps Script project's `Project settings > Script properties`. 

| Property Name | Description |
| :--- | :--- |
| `TIKTOK_CLIENT_ID` | [cite_start]Your TikTok Developer App's **Client Key**.  |
| `TIKTOK_CLIENT_SECRET`| [cite_start]Your TikTok Developer App's **Client Secret**.  |

## API Scopes Used

[cite_start]This connector requests the following scopes from the TikTok API.  Ensure they are enabled for your TikTok application in the TikTok Developer Console.

* [cite_start]`user.info.stats` 
* [cite_start]`user.info.profile` 
* [cite_start]`video.list` 

## Troubleshooting

If you encounter issues, especially during authentication or data fetching:

* **Redirect URI Mismatch**: This is the most common setup problem. The error often appears after authorizing in TikTok. Ensure the **Web app URL** from your Apps Script deployment is copied *exactly* into the **Redirect URI** field in your TikTok Developer App settings.
* [cite_start]**Authorization Errors (`access_denied`)**: This error in the callback URL means you did not grant all the requested permissions in the TikTok pop-up window.  You must approve all requested scopes.
* **API Errors (`invalid_token`, `permission_denied`, etc.)**: The connector will display specific error messages from TikTok. 
    * `permission_denied` or `insufficient_scope`: Your TikTok App does not have the correct scopes enabled and approved.
    * `token_expired` or `access_token_invalid`: May require you to re-authenticate. In Looker Studio, edit the data source connection, revoke access, and authorize again.
    * `rate_limit_exceeded`: You have made too many API requests in a short period. [cite_start]The connector has built-in delays, but heavy usage can still trigger this. 
* [cite_start]**No Data or "Failed to Fetch"**: Check the Apps Script execution logs (`View > Executions` in the Apps Script editor) for detailed error messages.  This can provide clues about failed API calls or other script issues.

## Contributing

Feel free to open issues or submit pull requests if you find bugs or want to add new features.

## License

This project is open-source and available under the MIT License.
