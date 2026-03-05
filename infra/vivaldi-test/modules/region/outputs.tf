output "public_ips" {
  value = aws_instance.node[*].public_ip
}

output "instance_ids" {
  value = aws_instance.node[*].id
}
