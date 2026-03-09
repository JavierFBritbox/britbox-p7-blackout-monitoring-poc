# P7 Blackout Monitoring PoC

Lambda-based API that reads P7 EPG schedule data from S3 and reports upcoming blackout windows.

## Prerequisites

- [Terraform](https://developer.hashicorp.com/terraform/install) >= 1.5
- AWS CLI configured with `britbox-nonprod` and/or `britbox-prod` profiles
- GNU Make

## Setup

1. Copy the environment template:

   ```bash
   cp .env.example .env
   ```

2. Edit `.env` to select your target environment:

   | Variable      | Description                          | Values                              |
   |---------------|--------------------------------------|-------------------------------------|
   | `ENV`         | Target environment                   | `stage` (nonprod) or `prod`         |
   | `AWS_PROFILE` | AWS CLI profile                      | `britbox-nonprod` or `britbox-prod` |
   | `AWS_REGION`  | AWS region                           | `eu-west-1`                         |

3. Initialize Terraform (downloads providers and configures S3 backend):

   ```bash
   make init
   ```

4. Preview the infrastructure changes:

   ```bash
   make plan
   ```

5. Deploy:

   ```bash
   make apply
   ```

## Switching Environments

To switch between stage and prod:

1. Update `.env` with the new `ENV` and `AWS_PROFILE` values
2. Re-run `make init` (required — the S3 state bucket changes between accounts)
3. Run `make plan` / `make apply` as usual

## Infrastructure

| Resource               | Description                                              |
|------------------------|----------------------------------------------------------|
| Lambda Function        | `p7-blackout-monitoring-{env}` — Node.js 20.x           |
| Lambda Function URL    | Public GET endpoint (no auth)                            |
| IAM Role               | Lambda execution role with S3 read + CloudWatch access   |
| CloudWatch Log Group   | 14-day retention                                         |

## Tear Down

```bash
make destroy
```
