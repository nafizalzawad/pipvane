output "auth0_api_identifier" {
  value       = auth0_resource_server.pipvane_api.identifier
  description = "The audience identifier for the Pipvane API"
}

output "auth0_m2m_client_id" {
  value       = auth0_client.pipvane_engine_m2m.client_id
  description = "The Client ID for the Pipvane Local Engine M2M client"
}

output "auth0_desktop_client_id" {
  value       = auth0_client.pipvane_desktop_app.client_id
  description = "The Client ID for the Pipvane Desktop PKCE application"
}

output "auth0_action_id" {
  value       = auth0_action.add_pipvane_license_claims.id
  description = "The ID of the custom claims post-login action"
}

output "auth0_founder_role_id" {
  value       = auth0_role.founder_tier.id
  description = "The ID of the Pipvane Founder Edition role"
}
