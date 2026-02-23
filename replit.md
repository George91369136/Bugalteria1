# Офисный дом — Бухгалтерия (ИП Селецкий)

## Overview

A full-featured web application for small business accounting called "Офисный дом" (Office House) for ИП Селецкий. The application provides comprehensive bookkeeping functionality including bank statement processing, counterparty management, document generation (acts, payment orders, invoices), employee payroll tracking, marketplace revenue tracking (Wildberries, Ozon), and financial reporting. Built as a monolithic Python web application with PostgreSQL database backend.

## Recent Changes
- **2026-01-26**: Added Marketplace section for tracking Wildberries/Ozon revenue with file parsing support
- **2026-01-26**: Updated company name to "ИП Селецкий"
- **2026-01-26**: Configured email integration for automatic bank statement downloads

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Backend Architecture
- **Framework**: Pure Python WSGI application using `wsgiref.simple_server`
- **Pattern**: Monolithic single-file architecture with all routes and logic in `main.py`
- **Server**: Runs on `0.0.0.0:5000`

**Rationale**: Lightweight approach without heavy web frameworks, suitable for a self-contained business application with moderate traffic requirements.

### Database Layer
- **Database**: PostgreSQL with `psycopg2` driver
- **Cursor**: Uses `RealDictCursor` for dictionary-style row access
- **Tables**:
  - `settings` - Application configuration
  - `bank_rows` - Bank transactions/statements
  - `counterparties` - Business partner directory
  - `acts` - Work completion acts (stores party data as JSON in executor_json, customer_json)
  - `payment_orders` - Payment orders (stores party data as JSON in payer_json, receiver_json)
  - `employees` - Employee records
  - `salary_payments` - Salary payment records
  - `upd_rows` - Universal transfer documents (UPD)
  - `real_rows` - Realization transfer records
  - `cp_category_map` - Counterparty category mapping
  - `user_category_map` - User-defined categories
  - `basis_history` - Document basis history
  - `marketplace_rows` - Marketplace revenue entries (Wildberries, Ozon)

**Design Decision**: JSON fields for storing related party snapshots ensures historical accuracy even if counterparty details change over time.

### Frontend
- **Technology**: Server-rendered HTML/CSS without JavaScript frameworks
- **Styling**: Inline and embedded CSS
- **Templating**: Python string formatting with HTML escaping via `html.escape`

### Email Integration
- **Protocol**: IMAP for fetching bank statements from email
- **Library**: Python `imaplib` and `email` modules
- **Purpose**: Automatic import of bank statements sent via email

### Document Generation
- **Excel Export**: `openpyxl` for creating styled XLSX spreadsheets
- **PDF Generation**: `reportlab` for generating PDF documents (acts, payment orders)
- **CSV**: Standard library for data import/export

### Data Processing
- **Bank Statements**: Parses 1CClientBankExchange format (Russian banking standard)
- **HTML Parsing**: `BeautifulSoup` for processing HTML content
- **Decimal Handling**: Python `Decimal` for precise financial calculations

## External Dependencies

### Database
- **PostgreSQL**: Primary data store (Neon-backed on Replit)
- **Connection**: Via `DATABASE_URL` environment variable

### Python Packages
- `psycopg2-binary` - PostgreSQL adapter
- `openpyxl` - Excel file generation and parsing
- `reportlab` - PDF document generation
- `beautifulsoup4` - HTML parsing
- `pdfplumber` - PDF text extraction (for Wildberries reports)

### Email Services
- **IMAP Server**: Gmail IMAP (`imap.gmail.com:993`) for fetching bank statements
- **Authentication**: App-specific passwords for Gmail access

### File Formats
- **1CClientBankExchange**: Russian banking standard for statement import
- **CSV/XLSX**: Category mapping and data export
- **PDF**: Document output for acts and payment orders