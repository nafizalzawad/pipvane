terraform {
  required_version = ">= 1.5.0"
  required_providers {
    auth0 = {
      source  = "auth0/auth0"
      version = ">= 1.4.0"
    }
  }
}

provider "auth0" {
  domain        = var.auth0_domain
  client_id     = var.auth0_client_id
  client_secret = var.auth0_client_secret
}

# -----------------------------------------------------------------------------
# Auth0 Tenant Configuration
# -----------------------------------------------------------------------------
resource "auth0_tenant" "tenant_config" {
  friendly_name = "Pipvane Cloud Control Plane (${var.environment})"
  support_email = "support@pipvane.com"

  flags {
    enable_client_connections = true
  }
}

# -----------------------------------------------------------------------------
# Pipvane Cloud Control Plane API (Resource Server)
# RS256 Cryptographic Signing, 30-Day Token Lifetime (FR-8.1 & FR-8.4)
# -----------------------------------------------------------------------------
resource "auth0_resource_server" "pipvane_api" {
  name        = "Pipvane Cloud Control Plane API"
  identifier  = var.api_identifier
  signing_alg = "RS256"

  # 30-Day offline grace period lifetime (30 * 86400 seconds = 2,592,000s)
  token_lifetime          = 2592000
  token_lifetime_for_web  = 7200
  enforce_policies        = true
  token_dialect           = "access_token"
  skip_consent_for_verifiable_first_party_clients = true

  scopes {
    value       = "read:license"
    description = "Read active license status, tier, and account limits"
  }

  scopes {
    value       = "issue:license"
    description = "Issue fresh 30-day RS256 cryptographically signed license tokens"
  }

  scopes {
    value       = "refresh:license"
    description = "Perform 7-day silent refresh and automatic token rotation"
  }

  scopes {
    value       = "revoke:license"
    description = "Revoke compromised license or device JTI tokens"
  }

  scopes {
    value       = "telemetry:write"
    description = "Ingest anonymized execution latency and heartbeat telemetry"
  }

  scopes {
    value       = "lease:coordinate"
    description = "Acquire and heartbeat active-passive failover execution locks"
  }
}

# -----------------------------------------------------------------------------
# Pipvane Local Engine M2M Client (Non-interactive Machine-to-Machine)
# Used by the local agent to communicate with the Cloud Control Plane
# -----------------------------------------------------------------------------
resource "auth0_client" "pipvane_engine_m2m" {
  name        = "Pipvane Local Engine M2M Client"
  description = "M2M Client for Pipvane local agent to interact with the Cloud Control Plane"
  app_type    = "non_interactive"

  grant_types = [
    "client_credentials"
  ]

  token_endpoint_auth_method = "client_secret_post"
  jwt_configuration {
    alg = "RS256"
  }
}

# Grant M2M Client access to the Pipvane API
resource "auth0_client_grant" "pipvane_engine_m2m_grant" {
  client_id = auth0_client.pipvane_engine_m2m.id
  audience  = auth0_resource_server.pipvane_api.identifier

  scopes = [
    "read:license",
    "issue:license",
    "refresh:license",
    "telemetry:write",
    "lease:coordinate"
  ]
}

# -----------------------------------------------------------------------------
# Pipvane Desktop Interactive Application (PKCE Native Client)
# -----------------------------------------------------------------------------
resource "auth0_client" "pipvane_desktop_app" {
  name        = "Pipvane Desktop Application"
  description = "Desktop UI interactive application client using PKCE flow"
  app_type    = "native"

  grant_types = [
    "authorization_code",
    "refresh_token"
  ]

  callbacks = [
    "http://127.0.0.1:45823/callback",
    "http://localhost:45823/callback",
    "https://pipvane.com/callback"
  ]

  allowed_logout_urls = [
    "http://127.0.0.1:45823/logout",
    "http://localhost:45823/logout",
    "https://pipvane.com/logout"
  ]

  token_endpoint_auth_method = "none" # Public client requiring PKCE
  oidc_conformant            = true

  jwt_configuration {
    alg = "RS256"
  }

  refresh_token {
    rotation_type                = "rotating"
    expiration_type              = "expiring"
    token_lifetime               = 2592000 # 30 days
    idle_token_lifetime          = 604800  # 7 days
    infinite_idle_token_lifetime = false
    infinite_token_lifetime      = false
  }
}

# -----------------------------------------------------------------------------
# License Roles: Standard (4 terminals), Pro (7 terminals), and Founder Edition
# -----------------------------------------------------------------------------
resource "auth0_role" "standard_tier" {
  name        = "Pipvane Standard License"
  description = "Standard tier user - entitlement for up to 4 concurrent MT4/MT5 terminals"
}

resource "auth0_role" "pro_tier" {
  name        = "Pipvane Pro License"
  description = "Pro tier user - entitlement for up to 7 concurrent MT4/MT5 terminals"
}

resource "auth0_role" "founder_tier" {
  name        = "Pipvane Founder Edition"
  description = "Founder cohort license - Edition: Founder with permanent 4/7 terminal entitlement and prioritized support"
}

# -----------------------------------------------------------------------------
# Auth0 Action: Custom Claims Injection for JWTs
# Injects `edition`, `tier`, `account_limit`, `priority_support`, and `license_id` into tokens
# -----------------------------------------------------------------------------
resource "auth0_action" "add_pipvane_license_claims" {
  name    = "Add Pipvane Custom License Claims"
  runtime = "node18"
  deploy  = true

  supported_triggers {
    id      = "post-login"
    version = "v3"
  }

  code = <<-EOT
    /**
     * Handler executed post-login to inject Pipvane custom license claims
     * into access and ID tokens, including Sprint 26 Founder cohort provisioning.
     *
     * @param {Event} event - Auth0 post-login event
     * @param {PostLoginAPI} api - Auth0 post-login API
     */
    exports.onExecutePostLogin = async (event, api) => {
      const namespace = 'https://pipvane.com/claims/';
      const roles = event.authorization && event.authorization.roles ? event.authorization.roles : [];
      const appMetadata = event.user.app_metadata || {};

      // Check for Founder cohort (Sprint 26)
      const isFounder = roles.includes('Pipvane Founder Edition') ||
        roles.includes('Founder') ||
        appMetadata.edition === 'Founder' ||
        appMetadata.is_founder === true;

      const edition = isFounder ? 'Founder' : (appMetadata.edition || 'Standard');
      const isProPack = roles.includes('Pipvane Pro License') || appMetadata.tier === 'Pro' || appMetadata.pack === 'pro_7';

      // Determine terminal limit:
      // Founder: permanently set to 4 (or 7 if bought pro pack)
      let tier = isProPack ? 'Pro' : 'Standard';
      let accountLimit = isProPack ? 7 : 4;

      const licenseId = appMetadata.license_id ? appMetadata.license_id : event.user.user_id;

      // Attach custom claims
      api.accessToken.setCustomClaim(namespace + 'edition', edition);
      api.accessToken.setCustomClaim(namespace + 'tier', tier);
      api.accessToken.setCustomClaim(namespace + 'account_limit', accountLimit);
      api.accessToken.setCustomClaim(namespace + 'license_id', licenseId);
      api.accessToken.setCustomClaim(namespace + 'priority_support', isFounder);

      api.idToken.setCustomClaim(namespace + 'edition', edition);
      api.idToken.setCustomClaim(namespace + 'tier', tier);
      api.idToken.setCustomClaim(namespace + 'account_limit', accountLimit);
      api.idToken.setCustomClaim(namespace + 'license_id', licenseId);
      api.idToken.setCustomClaim(namespace + 'priority_support', isFounder);
    };
  EOT
}

# Attach Action to Post-Login Trigger
resource "auth0_trigger_actions" "post_login_trigger" {
  trigger = "post-login"

  actions {
    id           = auth0_action.add_pipvane_license_claims.id
    display_name = auth0_action.add_pipvane_license_claims.name
  }
}
