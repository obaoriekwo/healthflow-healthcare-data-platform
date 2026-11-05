variable "project" {
  description = "Short name used in every resource name."
  type        = string
  default     = "healthflow"
}

variable "location" {
  description = "Azure region."
  type        = string
  default     = "northeurope"
}

variable "allowed_ip" {
  description = "Your public IP address (e.g. from https://ifconfig.me). Only this address can reach the database."
  type        = string

  validation {
    condition     = can(cidrhost("${var.allowed_ip}/32", 0))
    error_message = "allowed_ip must be a single IPv4 address, such as 203.0.113.10."
  }
}

variable "budget_eur" {
  description = "Monthly budget for the resource group. Alerts at 50%, 80% and 100%; Azure budgets alert, they do not stop spending."
  type        = number
  default     = 10
}

variable "budget_contact_email" {
  description = "Where budget alerts are sent."
  type        = string
}

variable "budget_start_date" {
  description = "First day of the current month, as YYYY-MM-01 (Azure budgets must start on the first of a month)."
  type        = string

  validation {
    condition     = can(regex("^\\d{4}-\\d{2}-01$", var.budget_start_date))
    error_message = "budget_start_date must look like 2026-11-01."
  }
}

variable "tags" {
  description = "Tags applied to every resource."
  type        = map(string)
  default = {
    project     = "healthflow"
    environment = "demo"
    data        = "synthetic-only"
  }
}
