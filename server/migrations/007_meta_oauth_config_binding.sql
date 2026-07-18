ALTER TABLE oauth_transactions
  ADD COLUMN provider_config_id VARCHAR(191) NULL AFTER redirect_uri,
  ADD INDEX oauth_transactions_provider_config_idx (provider, provider_config_id, status);
