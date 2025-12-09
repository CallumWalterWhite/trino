/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.jdbc;

import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.jdbc.credential.CredentialPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

/**
 * ConnectionFactory that rewrites the JDBC URL (and optionally the user) using a tenant id
 * passed via connector identity extra credentials. This allows routing to per-tenant databases
 * or proxies without exposing multiple catalogs.
 *
 * Provide connection-url-template with a {tenant} placeholder in the connector config and
 * set the tenant extra credential key to read from the session.
 */
public final class TenantAwareConnectionFactory
        implements ConnectionFactory
{
    private final Driver driver;
    private final String connectionUrlTemplate;
    private final String fallbackConnectionUrl;
    private final Properties baseConnectionProperties;
    private final CredentialPropertiesProvider credentialPropertiesProvider;
    private final String tenantCredentialKey;
    private final Optional<String> userTemplate;
    private final OpenTelemetry openTelemetry;

    public TenantAwareConnectionFactory(
            Driver driver,
            String connectionUrlTemplate,
            String fallbackConnectionUrl,
            Properties baseConnectionProperties,
            CredentialPropertiesProvider credentialPropertiesProvider,
            String tenantCredentialKey,
            Optional<String> userTemplate,
            OpenTelemetry openTelemetry)
    {
        this.driver = requireNonNull(driver, "driver is null");
        this.connectionUrlTemplate = requireNonNull(connectionUrlTemplate, "connectionUrlTemplate is null");
        this.fallbackConnectionUrl = requireNonNull(fallbackConnectionUrl, "fallbackConnectionUrl is null");
        this.baseConnectionProperties = new Properties();
        this.baseConnectionProperties.putAll(requireNonNull(baseConnectionProperties, "baseConnectionProperties is null"));
        this.credentialPropertiesProvider = requireNonNull(credentialPropertiesProvider, "credentialPropertiesProvider is null");
        this.tenantCredentialKey = requireNonNull(tenantCredentialKey, "tenantCredentialKey is null");
        this.userTemplate = requireNonNull(userTemplate, "userTemplate is null");
        this.openTelemetry = requireNonNull(openTelemetry, "openTelemetry is null");
    }

    @Override
    public Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        ConnectorIdentity identity = session.getIdentity();
        String tenant = identity.getExtraCredentials().get(tenantCredentialKey);
        String connectionUrl = (tenant == null) ? fallbackConnectionUrl : applyTenant(connectionUrlTemplate, tenant);

        Properties properties = new Properties();
        properties.putAll(baseConnectionProperties);
        properties.putAll(credentialPropertiesProvider.getCredentialProperties(identity));
        userTemplate.ifPresent(template -> {
            if (tenant != null && !properties.containsKey("user")) {
                properties.put("user", applyTenant(template, tenant));
            }
        });

        Connection connection = new TracingDataSource(openTelemetry, driver, connectionUrl).getConnection(properties);
        checkState(connection != null, "Driver returned null connection, make sure the connection URL '%s' is valid for the driver %s", connectionUrl, driver);
        return connection;
    }

    @Override
    public void close()
            throws SQLException
    {
        // no-op
    }

    private static String applyTenant(String template, String tenant)
    {
        return template.replace("{tenant}", tenant);
    }

    public static Builder builder(Driver driver)
    {
        return new Builder(driver);
    }

    public static final class Builder
    {
        private final Driver driver;
        private String connectionUrlTemplate;
        private String fallbackConnectionUrl;
        private Properties connectionProperties = new Properties();
        private CredentialPropertiesProvider credentialPropertiesProvider;
        private String tenantCredentialKey = "tenant";
        private Optional<String> userTemplate = Optional.empty();
        private OpenTelemetry openTelemetry = OpenTelemetry.noop();

        public Builder(Driver driver)
        {
            this.driver = requireNonNull(driver, "driver is null");
        }

        public Builder setConnectionUrlTemplate(String connectionUrlTemplate)
        {
            this.connectionUrlTemplate = connectionUrlTemplate;
            return this;
        }

        /**
         * Used when no tenant is supplied. Typically the shared or default database/schema.
         */
        public Builder setFallbackConnectionUrl(String fallbackConnectionUrl)
        {
            this.fallbackConnectionUrl = fallbackConnectionUrl;
            return this;
        }

        public Builder setConnectionProperties(Properties connectionProperties)
        {
            this.connectionProperties = connectionProperties;
            return this;
        }

        public Builder setCredentialPropertiesProvider(CredentialPropertiesProvider credentialPropertiesProvider)
        {
            this.credentialPropertiesProvider = credentialPropertiesProvider;
            return this;
        }

        public Builder setTenantCredentialKey(String tenantCredentialKey)
        {
            this.tenantCredentialKey = tenantCredentialKey;
            return this;
        }

        /**
         * Optional template for the JDBC user, e.g. "{tenant}_user".
         */
        public Builder setUserTemplate(Optional<String> userTemplate)
        {
            this.userTemplate = requireNonNullElse(userTemplate, Optional.empty());
            return this;
        }

        public Builder setOpenTelemetry(OpenTelemetry openTelemetry)
        {
            this.openTelemetry = openTelemetry;
            return this;
        }

        public TenantAwareConnectionFactory build()
        {
            requireNonNull(connectionUrlTemplate, "connectionUrlTemplate is null");
            requireNonNull(fallbackConnectionUrl, "fallbackConnectionUrl is null");
            requireNonNull(credentialPropertiesProvider, "credentialPropertiesProvider is null");
            return new TenantAwareConnectionFactory(
                    driver,
                    connectionUrlTemplate,
                    fallbackConnectionUrl,
                    connectionProperties,
                    credentialPropertiesProvider,
                    tenantCredentialKey,
                    userTemplate,
                    openTelemetry);
        }
    }
}
