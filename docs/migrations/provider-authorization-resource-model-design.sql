-- Social Insights Studio provider authorization/resource model design notes.
-- The executable phase-3 migration lives under server/migrations when implemented.
-- Keep this document aligned with ADR 0001.

-- Core planned entities:
-- provider_authorizations
-- provider_authorization_credentials
-- provider_authorization_scopes
-- provider_resources
-- workspace_provider_connections
-- provider_capabilities
-- provider_sync_states
-- provider_revocation_events

-- Existing TikTok compatibility rows must be copied from data_sources,
-- provider_accounts, oauth_credentials, provider_scopes, and sync_jobs without
-- decrypting or replacing ciphertext.
