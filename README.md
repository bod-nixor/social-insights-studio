# TikTok User & Video Data Connector for Looker Studio

This is a Google Apps Script-based Community Connector designed to pull user profile information and a list of their videos (with metrics) from the TikTok Open Platform API and make it available in Google Looker Studio (formerly Google Data Studio).

It leverages OAuth2 for secure authentication with TikTok, allowing you to create dashboards and reports based on your TikTok account's data.

## Features

* **Comprehensive User Data:** Fetches user profile details including Open ID, Union ID, username, display name, bio, profile links, avatar URLs, verification status, follower/following counts, total likes, and total video count.
* **Detailed Video Data:** Retrieves information for videos published by the authenticated user, including video ID, creation time, cover image, share URL, description, title, embed details, duration, dimensions, and metrics like likes, comments, shares, and views.
* **OAuth2 Authentication:** Securely authenticates with TikTok using the standard OAuth2 flow, ensuring your credentials are not stored directly in the connector.
* **Automatic Token Refresh:** Handles the refreshing of access tokens to maintain continuous data access without manual re-authentication (as long as the refresh token remains valid).
* **Single Data Source:** Combines both user and video data into a unified schema for easier reporting in Looker Studio.

## How it Works

This connector operates as a Google Apps Script web application.
1.  **Deployment:** You deploy this script as a web app in your Google Cloud Project.
2.  **Authentication (OAuth2):** When you add the connector to Looker Studio, it initiates an OAuth2 flow.
    * Your Apps Script provides an authorization URL to TikTok.
    * You are redirected to TikTok to grant permissions.
    * TikTok redirects back to your Apps Script's `doGet` function with an authorization code.
    * Your Apps Script exchanges this code for an Access Token and a Refresh Token using the OAuth2 for Apps Script library.
    * These tokens are securely stored in your User Properties, not directly in the script.
3.  **Data Fetching:** When Looker Studio requests data, the connector uses the stored Access Token to make requests to the TikTok Open Platform API (User Info API and Video List API).
4.  **Data Transformation:** The fetched data is processed and formatted according to the defined schema (`getFields`) and returned to Looker Studio.

## Setup & Installation

Follow these steps carefully to set up and deploy your connector. **Exact matches for URLs and IDs are critical!**

### Step 1: Create a TikTok Developer Application

1.  Go to the [TikTok Developer Console](https://developers.tiktok.com/).
2.  Log in and create a new application.
3.  Fill in the required app details (App Name, Logo, Category, etc.).
4.  In your app settings, navigate to the **"Product Management"** or **"Integrations"** section.
5.  Request access to the following products/permissions (scopes):
    * `Login Kit` (This usually grants `user.info.basic`, `user.info.profile`)
    * `Video Kit` (This usually grants `video.list`)
    * Ensure you have access to `user.info.stats`, `user.info.profile`, and `video.list`. You may need to apply for these permissions.
6.  Go to the **"Credentials"** or **"App Key & Secret"** section. Note down your:
    * **Client Key** (This will be your `TIKTOK_CLIENT_ID`)
    * **Client Secret** (This will be your `TIKTOK_CLIENT_SECRET`)
7.  **Crucially, configure the "Redirect URI" / "Callback URL".** For now, you can use a placeholder like `https://script.google.com/macros/d/YOUR_SCRIPT_ID/exec` or `https://example.com/oauth2callback`. You will update this with the *exact* URL of your deployed Apps Script in Step 4.

### Step 2: Create a Google Apps Script Project

1.  Go to [Google Apps Script](https://script.google.com/home).
2.  Click `New project`.
3.  Name your project (e.g., `TikTok Looker Studio Connector`).
4.  **Replace the default `Code.gs` content** with the entire code provided in `Code.txt` from this repository.
5.  **Add the OAuth2 for Apps Script Library:**
    * In the Apps Script editor, click on `Libraries` (the `+` icon next to "Libraries" in the left sidebar).
    * In the "Add a library" field, paste the **Script ID**: `1B7FSrg4E5WqmNcpecVPODk6Oztd_PlrxXh8HTMrdhr6Cw6yazg4m8PTK`
    * Click "Look up".
    * Select the latest version.
    * Ensure the "Identifier" field is `OAuth2`.
    * Click "Add".

### Step 3: Configure Script Properties

1.  In your Apps Script project, go to `Project settings` (the gear icon on the left sidebar).
2.  Scroll down to "Script properties".
3.  Add two new properties:
    * `TIKTOK_CLIENT_ID`: Paste your **Client Key** from Step 1.
    * `TIKTOK_CLIENT_SECRET`: Paste your **Client Secret** from Step 1.
4.  Click `Save script properties`.

### Step 4: Deploy the Apps Script as a Web App

1.  In your Apps Script project, click `Deploy > New deployment`.
2.  Click the "Select type" dropdown and choose `Web app`.
3.  Configure the deployment:
    * **Execute as:** `Me` (your email address)
    * **Who has access:** `Anyone, even anonymous`
4.  Click `Deploy`.
5.  Google may ask you to authorize the script for the first time. Follow the prompts, click "Review permissions," select your Google account, and grant access.
6.  Once deployed, you will get a "Web app URL". **Copy this entire URL.**

7.  **Update TikTok Redirect URI:**
    * Go back to your TikTok Developer Console (from Step 1).
    * Edit your application's "Redirect URI" / "Callback URL".
    * **Paste the exact "Web app URL" you copied from the Apps Script deployment.** It must match precisely, including `https://` and `/exec`. Save the changes in TikTok.

### Step 5: Use the Connector in Looker Studio

1.  Go to [Google Looker Studio](https://lookerstudio.google.com/).
2.  Start a new report or data source.
3.  Choose `Explore connectors`.
4.  In the search bar, search for "Deploy by ID".
5.  Go back to your Apps Script project, click `Deploy > Manage deployments`.
6.  Copy the **Deployment ID** (it's a long string of characters).
7.  In Looker Studio, paste the Deployment ID and click `Validate`.
8.  You should see your "TikTok User & Video Data Connector". Select it.
9.  Click `Authorize`.
10. A pop-up will appear prompting you to authorize with your TikTok account. Follow the instructions to grant access.
11. If authentication is successful, you will see a success message, and then be prompted to connect to your data source in Looker Studio.
12. Click `Connect` and then `Create Report` or `Explore`.

## Configuration (Script Properties)

The following properties must be set in your Apps Script project's `Project settings > Script properties`:

| Property Name         | Description                                                                                             |
| :-------------------- | :------------------------------------------------------------------------------------------------------ |
| `TIKTOK_CLIENT_ID`    | Your TikTok Developer App's **Client Key**.                                                            |
| `TIKTOK_CLIENT_SECRET`| Your TikTok Developer App's **Client Secret**.                                                         |

## TikTok API Scopes Used

This connector requests the following scopes from the TikTok API:

* `user.info.stats`
* `user.info.profile`
* `video.list`

Ensure these scopes are enabled for your TikTok application in the TikTok Developer Console.

## Troubleshooting

If you encounter issues, especially during authentication:

* **"Sorry, unable to open the file at this time." / `doGet` not called:**
    * **MOST COMMON CAUSE:** The "Redirect URI" configured in your TikTok Developer Console **does not exactly match** the "Web app URL" from your Apps Script deployment. Copy the URL directly from `Deploy > Manage deployments` and paste it into TikTok.
    * **Deployment Permissions:** Ensure "Execute as: `Me`" and "Who has access: `Anyone, even anonymous`" are set for your web app deployment. Redeploy if you change these.
    * **Client ID/Secret:** Double-check `TIKTOK_CLIENT_ID` (Client Key) and `TIKTOK_CLIENT_SECRET` in your Apps Script properties for typos.
    * **TikTok App Scopes:** Verify that the required scopes (`user.info.stats`, `user.info.profile`, `video.list`) are active and approved for your TikTok application.
    * **OAuth2 Library:** Confirm the OAuth2 library is correctly added to your Apps Script project (Script ID: `1B7FSrg4E5WqmNcpecVPODk6Oztd_PlrxXh8HTMrdhr6Cw6yazg4m8PTK`, Identifier: `OAuth2`).
    * **Try Incognito:** Sometimes browser extensions can interfere. Try the authentication flow in an incognito/private browser window.

* **"Error retrieving token from TikTok:" / "invalid_grant" / "token has been revoked":**
    * This usually means the `client_key`, `client_secret`, or `refresh_token` is invalid or expired.
    * **Action:** Go to your Looker Studio data source, click `Edit connection`, and `Revoke` access, then try to re-authorize. Also, re-verify your `TIKTOK_CLIENT_ID` and `TIKTOK_CLIENT_SECRET` in Apps Script properties.

* **"Failed to get your TikTok user profile." / "TikTok reported an error retrieving data":**
    * This indicates that authentication succeeded, but the API call to TikTok for data failed.
    * **Action:** Check your Apps Script execution logs (`View > Executions`) for the `getData` function. Look for specific error messages from the TikTok API response, which will usually indicate missing permissions for the requested data fields.

## Contributing

Feel free to open issues or submit pull requests if you find bugs or want to add new features.

## License

This project is open-source and available under the [MIT License](LICENSE.md).
