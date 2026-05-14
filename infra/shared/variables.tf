variable "zone_name" {
  description = "Apex Cloudflare zone shared between prod and staging clusters."
  type        = string
  default     = "pln.sh"
}

variable "staging_subdomain" {
  description = "Subdomain under zone_name that fronts the staging cluster (e.g. \"staging\"). Used in the origin-port ruleset expressions."
  type        = string
  default     = "staging"
}
