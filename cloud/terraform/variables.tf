variable "auth0_domain" {
  type        = string
  description = "The Auth0 tenant domain (e.g., pipvane.us.auth0.com)"
}

variable "auth0_client_id" {
  type        = string
  description = "The Auth0 Management API Client ID"
}

variable "auth0_client_secret" {
  type        = string
  description = "The Auth0 Management API Client Secret"
  sensitive   = true
}

variable "api_identifier" {
  type        = string
  description = "The unique audience identifier for the Pipvane Cloud Control Plane API"
  default     = "https://api.pipvane.com/v3"
}

variable "environment" {
  type        = string
  description = "Deployment environment (production, staging, development)"
  default     = "production"
}
