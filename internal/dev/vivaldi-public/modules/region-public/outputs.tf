output "public_ips" {
  value = aws_instance.public[*].public_ip
}

output "private_ips" {
  value = aws_instance.public[*].private_ip
}

output "instance_ids" {
  value = aws_instance.public[*].id
}
