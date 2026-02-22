# LIBER Invoice Generator

This folder contains generated PDF invoices from the LIBER Invoice Generator application.

## File Naming Convention

Invoices are automatically named using the following format:
- `invoice_LIBER-YYYYMMDD-XXX.pdf`

Where:
- `YYYYMMDD` = Year, Month, Day (e.g., 20241201 for December 1, 2024)
- `XXX` = Sequential invoice number (e.g., 001, 002, 003...)

## Example Files

- `invoice_LIBER-20241201-001.pdf` - First invoice of December 1, 2024
- `invoice_LIBER-20241201-002.pdf` - Second invoice of December 1, 2024
- `invoice_LIBER-20241202-001.pdf` - First invoice of December 2, 2024

## Invoice Content

Each PDF invoice contains:
- LIBER logo and branding
- Client name
- Invoice ID and date
- Detailed service breakdown with costs and hours
- Subtotal, tax (0%), and total amounts
- Company footer with copyright

## Storage

All invoices are stored locally and can be accessed from this folder. The application automatically increments invoice numbers and maintains a consistent naming convention for easy organization.
