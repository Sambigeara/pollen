output "public_ip" {
  description = "Elastic IP of the exerciser host. Stable across instance recreation."
  value       = aws_eip.exerciser.public_ip
}

output "eip_cidr" {
  description = "CIDR form of the public IP, ready to paste into pollen-node ingress rules."
  value       = "${aws_eip.exerciser.public_ip}/32"
}
