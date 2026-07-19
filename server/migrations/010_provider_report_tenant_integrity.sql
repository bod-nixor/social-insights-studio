ALTER TABLE provider_authorizations
  ADD CONSTRAINT provider_authorizations_id_workspace_provider_unique
    UNIQUE (id, workspace_id, provider);

ALTER TABLE provider_resources
  ADD CONSTRAINT provider_resources_id_workspace_provider_unique
    UNIQUE (id, workspace_id, provider),
  ADD CONSTRAINT provider_resources_auth_workspace_provider_fk
    FOREIGN KEY (provider_authorization_id, workspace_id, provider)
    REFERENCES provider_authorizations(id, workspace_id, provider) ON DELETE CASCADE;

ALTER TABLE workspace_provider_connections
  ADD CONSTRAINT workspace_connections_id_workspace_provider_unique
    UNIQUE (id, workspace_id, provider),
  ADD CONSTRAINT workspace_connections_resource_workspace_provider_fk
    FOREIGN KEY (provider_resource_id, workspace_id, provider)
    REFERENCES provider_resources(id, workspace_id, provider) ON DELETE RESTRICT;

ALTER TABLE sync_runs
  ADD CONSTRAINT sync_runs_id_workspace_unique UNIQUE (id, workspace_id);

ALTER TABLE provider_resource_observations
  ADD CONSTRAINT resource_observations_connection_tenant_fk
    FOREIGN KEY (workspace_provider_connection_id, workspace_id, provider)
    REFERENCES workspace_provider_connections(id, workspace_id, provider) ON DELETE CASCADE,
  ADD CONSTRAINT resource_observations_run_tenant_fk
    FOREIGN KEY (sync_run_id, workspace_id) REFERENCES sync_runs(id, workspace_id) ON DELETE CASCADE;

ALTER TABLE provider_metric_observations
  ADD CONSTRAINT metric_observations_connection_tenant_fk
    FOREIGN KEY (workspace_provider_connection_id, workspace_id, provider)
    REFERENCES workspace_provider_connections(id, workspace_id, provider) ON DELETE CASCADE,
  ADD CONSTRAINT metric_observations_run_tenant_fk
    FOREIGN KEY (sync_run_id, workspace_id) REFERENCES sync_runs(id, workspace_id) ON DELETE CASCADE;

ALTER TABLE provider_dimension_observations
  ADD CONSTRAINT dimension_observations_connection_tenant_fk
    FOREIGN KEY (workspace_provider_connection_id, workspace_id, provider)
    REFERENCES workspace_provider_connections(id, workspace_id, provider) ON DELETE CASCADE,
  ADD CONSTRAINT dimension_observations_run_tenant_fk
    FOREIGN KEY (sync_run_id, workspace_id) REFERENCES sync_runs(id, workspace_id) ON DELETE CASCADE;

ALTER TABLE report_definitions
  ADD CONSTRAINT report_definitions_id_workspace_unique UNIQUE (id, workspace_id);

ALTER TABLE report_definition_resources
  ADD COLUMN workspace_id CHAR(36) NOT NULL AFTER report_definition_id,
  ADD CONSTRAINT report_definition_resources_definition_tenant_fk
    FOREIGN KEY (report_definition_id, workspace_id)
    REFERENCES report_definitions(id, workspace_id) ON DELETE CASCADE,
  ADD CONSTRAINT report_definition_resources_connection_tenant_fk
    FOREIGN KEY (workspace_provider_connection_id, workspace_id, provider)
    REFERENCES workspace_provider_connections(id, workspace_id, provider) ON DELETE CASCADE;

ALTER TABLE report_runs
  ADD CONSTRAINT report_runs_id_workspace_unique UNIQUE (id, workspace_id),
  ADD CONSTRAINT report_runs_definition_tenant_fk
    FOREIGN KEY (report_definition_id, workspace_id)
    REFERENCES report_definitions(id, workspace_id) ON DELETE RESTRICT;

ALTER TABLE report_run_resources
  ADD COLUMN workspace_id CHAR(36) NOT NULL AFTER report_run_id,
  ADD CONSTRAINT report_run_resources_run_tenant_fk
    FOREIGN KEY (report_run_id, workspace_id) REFERENCES report_runs(id, workspace_id) ON DELETE CASCADE;

ALTER TABLE report_artifacts
  ADD CONSTRAINT report_artifacts_run_workspace_unique UNIQUE (report_run_id, workspace_id),
  ADD CONSTRAINT report_artifacts_run_tenant_fk
    FOREIGN KEY (report_run_id, workspace_id) REFERENCES report_runs(id, workspace_id) ON DELETE CASCADE;
