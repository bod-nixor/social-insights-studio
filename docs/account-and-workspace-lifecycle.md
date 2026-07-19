# Account, Session, Member, And Deletion Lifecycle

Date: 2026-07-18

Migration: `008_account_invitation_lifecycle.sql`

This document records the locally implemented lifecycle behavior. It does not define the operator's final legal retention or deletion policy; those facts remain in `docs/compliance-blockers.md`.

## Sessions

- Session and CSRF secrets remain hashed at rest and are never returned by account APIs.
- Each new session stores only a coarse browser/device label, such as `Chrome on macOS`, alongside the pre-existing one-way user-agent hash. Raw user-agent strings and approximate locations are not stored.
- `GET /api/account` returns only the signed-in user's active, unexpired sessions and identifies the current session.
- A user can revoke one owned session, every other owned session, or all owned sessions. Session identifiers cannot be used across users.
- Revoking the current session or all sessions clears both authentication cookies.

## Invitations And Memberships

- Invitation email addresses are normalized and must not already identify an active workspace member or active invitation.
- Secrets are random, stored only as hashes, expire after seven days, and are returned only when non-production development authentication is enabled.
- Production mail uses the configured mail adapter. No real messages are sent by repository tests.
- Resending rotates the secret, extends expiry, has a 60-second cooldown, and stops after five total sends.
- Acceptance requires an authenticated user whose email exactly matches the invited email. It is transactional, replay-safe, and never silently joins a different account.
- Revoked or accepted invitations cannot be reused. Owners and admins can resend or revoke pending invitations according to RBAC.
- Invite, resend, revoke, accept, role-change, and removal actions are written to the workspace audit log. Last-owner protection is unchanged.

## Profile And Deletion Requests

- Users can update a display name of at most 100 characters. Sign-in email changes remain a support workflow.
- Account deletion intake requires the signed-in user to re-enter the full account email.
- Workspace deletion intake requires the `deleteWorkspace` capability and an exact workspace-name confirmation.
- Duplicate open requests are idempotent. Authenticated confirmation records a `verified` request; it does not immediately delete data.
- Operators must define and document identity review, approval, retention, backup treatment, and completion communication before enabling claims about deletion turnaround.

## API Summary

- `GET /api/account`
- `PATCH /api/account/profile`
- `DELETE /api/account/sessions/:sessionId`
- `POST /api/account/sessions/revoke-others`
- `POST /api/account/sessions/revoke-all`
- `POST /api/account/deletion-requests`
- `POST /api/invitations/accept`
- `POST /api/workspaces/:workspaceId/invitations/:invitationId/resend`
- `DELETE /api/workspaces/:workspaceId/invitations/:invitationId`
- `POST /api/workspaces/:workspaceId/deletion-requests`

Every mutating route requires an authenticated session and the existing double-submit CSRF check.
