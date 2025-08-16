# LIBER Invoice Generator

A modern, adaptive web application for generating professional invoices for LIBER Creative LLC. Built with HTML5, CSS3, and vanilla JavaScript, featuring a sleek black and white design with the iconic LIBER branding.

## Features

### 🎯 Core Functionality
- **Invoice Generation**: Create professional PDF invoices with automatic numbering
- **Client Management**: Save and manage client information for quick access
- **Service Management**: Define and save services with default costs and hourly rates
- **Real-time Calculations**: Automatic total calculation with support for hours and rates
- **PDF Export**: Download invoices as PDF files with proper formatting

### 🎨 Design & UX
- **Modern UI**: Black background with white elements for a professional look
- **Responsive Design**: Works seamlessly on desktop, tablet, and mobile devices
- **LIBER Branding**: Incorporates the official LIBER logo and color scheme
- **Intuitive Navigation**: Tab-based interface for easy organization

### 💾 Data Management
- **Local Storage**: All data is saved locally in the browser
- **Persistent Data**: Clients and services are remembered between sessions
- **Auto-incrementing IDs**: Invoice numbers automatically increment
- **Data Validation**: Comprehensive input validation and error handling

## Project Structure

```
INVOICES-APP/
├── invoices.html          # Main application file
├── css/
│   └── styles.css         # All styling and responsive design
├── js/
│   └── app.js            # Main application logic
├── invoices/             # Generated PDF invoices folder
│   └── README.md         # Invoice folder documentation
└── README.md             # This file
```

## Getting Started

### Prerequisites
- Modern web browser (Chrome, Firefox, Safari, Edge)
- No additional software installation required

### Installation
1. Clone or download this repository
2. Open `invoices.html` in your web browser
3. Start creating invoices immediately!

### Usage

#### 1. Generate Invoice Tab
- **Client Name**: Enter manually or select from saved clients
- **Invoice Date**: Automatically set to current date (editable)
- **Invoice ID**: Auto-generated with format `LIBER-YYYYMMDD-XXX`
- **Services**: Add multiple services with names, costs, and optional hours
- **Total Calculation**: Real-time calculation of subtotal and total

#### 2. Manage Services Tab
- **Add New Services**: Define service names, default costs, and hourly rates
- **Saved Services**: View and manage all saved services
- **Quick Selection**: Use saved services in invoice generation

#### 3. Manage Clients Tab
- **Add New Clients**: Save client names for quick access
- **Client List**: View and manage all saved clients
- **Quick Selection**: Select saved clients when generating invoices

## Invoice Features

### PDF Output
- Professional formatting with LIBER branding
- Complete invoice details including client, date, and ID
- Detailed service breakdown with costs and hours
- Automatic total calculations
- Company footer with copyright information

### Invoice Content
- **Header**: LIBER logo and "INVOICE" title
- **Client Information**: Name, invoice ID, and date
- **Services Table**: Service name, cost, hours, and total
- **Totals Section**: Subtotal, tax (0%), and final total
- **Footer**: "LIBER CREATIVE LLC © 2025"

## Technical Details

### Dependencies
- **jsPDF**: PDF generation library
- **jsPDF-AutoTable**: Table formatting for PDFs
- **Font Awesome**: Icons for the user interface

### Browser Compatibility
- Chrome 60+
- Firefox 55+
- Safari 12+
- Edge 79+

### Data Storage
- Uses browser's localStorage for data persistence
- No server required - completely client-side
- Data is saved locally and persists between sessions

## Customization

### Adding New Features
The modular JavaScript architecture makes it easy to extend:
- Add new invoice fields in the `getInvoiceData()` method
- Modify PDF generation in the `generatePDF()` method
- Update styling in `css/styles.css`

### Branding Changes
- Update logo styling in CSS (`.logo-text` and related classes)
- Modify company information in JavaScript
- Adjust color scheme in CSS variables

## Deployment

### GitHub Pages
1. Push code to GitHub repository
2. Enable GitHub Pages in repository settings
3. Access via `https://username.github.io/repository-name/invoices.html`

### Web Server
1. Upload all files to web server
2. Ensure proper MIME types for HTML, CSS, and JS files
3. Access via `https://yourdomain.com/invoices.html`

### Local Development
1. Use any local web server (Live Server, Python SimpleHTTPServer, etc.)
2. Open `http://localhost:port/invoices.html`

## File Naming Convention

Generated invoices follow the pattern:
- `invoice_LIBER-YYYYMMDD-XXX.pdf`
- Example: `invoice_LIBER-20241201-001.pdf`

## Support

For technical support or feature requests, please contact the development team.

## License

This project is proprietary software for LIBER Creative LLC.

---

**LIBER Creative LLC © 2025**
