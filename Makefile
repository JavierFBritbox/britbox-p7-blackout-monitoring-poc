include .env
export AWS_PROFILE
export AWS_REGION

TF_DIR := terraform

# Bucket naming: stage → britbox-stage-terraform, prod → britbox-production-terraform
ifeq ($(ENV),prod)
  STATE_BUCKET := britbox-production-terraform
else
  STATE_BUCKET := britbox-$(ENV)-terraform
endif
STATE_KEY := p7-blackout-monitoring-poc/terraform.tfstate

.PHONY: init plan apply destroy fmt validate

init:
	cd $(TF_DIR) && terraform init -reconfigure \
		-backend-config="bucket=$(STATE_BUCKET)" \
		-backend-config="key=$(STATE_KEY)" \
		-backend-config="region=$(AWS_REGION)"

plan:
	cd $(TF_DIR) && terraform plan \
		-var "env=$(ENV)" \
		-var "aws_region=$(AWS_REGION)"

apply:
	cd $(TF_DIR) && terraform apply \
		-var "env=$(ENV)" \
		-var "aws_region=$(AWS_REGION)"

destroy:
	cd $(TF_DIR) && terraform destroy \
		-var "env=$(ENV)" \
		-var "aws_region=$(AWS_REGION)"

fmt:
	cd $(TF_DIR) && terraform fmt

validate:
	cd $(TF_DIR) && terraform validate
