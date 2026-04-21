output "public_ip" {
  value = aws_instance.public.public_ip
}

output "public_node_private_ip" {
  value = aws_instance.public.private_ip
}

output "private_ips" {
  value = aws_instance.private[*].private_ip
}

output "instance_ids" {
  value = concat([aws_instance.public.id], aws_instance.private[*].id)
}
