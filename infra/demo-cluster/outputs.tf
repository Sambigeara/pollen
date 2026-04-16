output "node_ips" {
  value = { for name, mod in local.modules : name => mod }
}

output "all_ips" {
  value = [for _, ip in local.modules : ip]
}

output "ssh_commands" {
  value = { for name, ip in local.modules : name => "ssh -o StrictHostKeyChecking=no ubuntu@${ip}" }
}

output "exerciser_ip" {
  description = "Elastic IP of the dedicated exerciser host."
  value       = module.exerciser.public_ip
}
