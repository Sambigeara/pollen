output "zone_id" {
  description = "Cloudflare zone ID for the shared apex zone."
  value       = data.cloudflare_zone.pln.id
}

output "zone_name" {
  description = "Apex Cloudflare zone name (e.g. \"pln.sh\")."
  value       = var.zone_name
}

output "staging_subdomain" {
  description = "Subdomain under zone_name that fronts the staging cluster."
  value       = var.staging_subdomain
}
