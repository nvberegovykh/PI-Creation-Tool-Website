// Invoice Generator Application
class InvoiceGenerator {
    constructor() {
        this.services = JSON.parse(localStorage.getItem('services')) || [];
        this.clients = JSON.parse(localStorage.getItem('clients')) || [];
        this.invoiceCounter = parseInt(localStorage.getItem('invoiceCounter')) || 1;
        this.currentServices = [];
        
        this.initializeApp();
        this.bindEvents();
        this.loadSavedData();
    }

    initializeApp() {
        // Set current date
        const today = new Date().toISOString().split('T')[0];
        document.getElementById('invoiceDate').value = today;
        
        // Generate invoice ID
        this.generateInvoiceId();
        
        // Add first service row
        this.addServiceRow();
    }

    bindEvents() {
        // Tab navigation
        document.querySelectorAll('.tab-btn').forEach(btn => {
            btn.addEventListener('click', (e) => this.switchTab(e.target.dataset.tab));
        });

        // Client dropdown
        document.getElementById('clientDropdownBtn').addEventListener('click', () => {
            this.toggleClientDropdown();
        });

        // Add service button
        document.getElementById('addServiceBtn').addEventListener('click', () => {
            this.addServiceRow();
        });

        // Save service button
        document.getElementById('saveServiceBtn').addEventListener('click', () => {
            this.saveService();
        });

        // Save client button
        document.getElementById('saveClientBtn').addEventListener('click', () => {
            this.saveClient();
        });

        // Generate PDF button
        document.getElementById('generatePdfBtn').addEventListener('click', () => {
            this.generatePDF();
        });

        // Preview button
        document.getElementById('previewBtn').addEventListener('click', () => {
            this.showPreview();
        });

        // Close preview modal
        document.getElementById('closePreviewBtn').addEventListener('click', () => {
            this.closePreview();
        });

        // Close modal when clicking outside
        document.getElementById('previewModal').addEventListener('click', (e) => {
            if (e.target.id === 'previewModal') {
                this.closePreview();
            }
        });

        // Close dropdown when clicking outside
        document.addEventListener('click', (e) => {
            if (!e.target.closest('.input-with-dropdown')) {
                this.closeClientDropdown();
            }
        });
    }

    switchTab(tabName) {
        // Update active tab button
        document.querySelectorAll('.tab-btn').forEach(btn => {
            btn.classList.remove('active');
        });
        document.querySelector(`[data-tab="${tabName}"]`).classList.add('active');

        // Update active tab content
        document.querySelectorAll('.tab-content').forEach(content => {
            content.classList.remove('active');
        });
        document.getElementById(`${tabName}-tab`).classList.add('active');
    }

    generateInvoiceId() {
        const date = new Date();
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        const invoiceId = `LIBER-${year}${month}${day}-${String(this.invoiceCounter).padStart(3, '0')}`;
        document.getElementById('invoiceId').value = invoiceId;
    }

    addServiceRow() {
        const servicesList = document.getElementById('servicesList');
        const serviceId = Date.now() + Math.random();
        
        const serviceRow = document.createElement('div');
        serviceRow.className = 'service-item';
        serviceRow.dataset.serviceId = serviceId;
        
        serviceRow.innerHTML = `
            <div class="service-item-header">
                <div class="service-item-title">Service ${this.currentServices.length + 1}</div>
                <button type="button" class="remove-service-btn" onclick="app.removeServiceRow('${serviceId}')">
                    <i class="fas fa-trash"></i> Remove
                </button>
            </div>
            <div class="service-fields">
                <div class="form-group">
                    <label>Service Name</label>
                    <div class="input-with-dropdown">
                        <input type="text" class="service-name" placeholder="Enter service name or select from saved" required>
                        <button type="button" class="dropdown-btn service-dropdown-btn">
                            <i class="fas fa-chevron-down"></i>
                        </button>
                        <div class="dropdown-menu service-dropdown">
                            ${this.services.map(service => `
                                <div class="dropdown-item" data-service='${JSON.stringify(service)}'>
                                    <div>${service.name}</div>
                                    <small>$${service.cost}${service.rate ? ` / $${service.rate}/hr` : ''}</small>
                                </div>
                            `).join('')}
                        </div>
                    </div>
                </div>
                <div class="form-group">
                    <label>Cost</label>
                    <input type="number" class="service-cost" placeholder="0.00" step="0.01" min="0" required>
                </div>
                                 <div class="form-group">
                     <label>Unit (optional)</label>
                     <input type="text" class="service-unit" placeholder="e.g., hours, sf, count">
                 </div>
            </div>
        `;

        servicesList.appendChild(serviceRow);
        this.currentServices.push(serviceId);

        // Bind service dropdown events
        const dropdownBtn = serviceRow.querySelector('.service-dropdown-btn');
        const dropdown = serviceRow.querySelector('.service-dropdown');
                 const nameInput = serviceRow.querySelector('.service-name');
         const costInput = serviceRow.querySelector('.service-cost');
         const unitInput = serviceRow.querySelector('.service-unit');

        dropdownBtn.addEventListener('click', () => {
            this.toggleServiceDropdown(dropdown);
        });

        dropdown.addEventListener('click', (e) => {
            if (e.target.closest('.dropdown-item')) {
                const serviceData = JSON.parse(e.target.closest('.dropdown-item').dataset.service);
                                 nameInput.value = serviceData.name;
                 costInput.value = serviceData.cost;
                 if (serviceData.rate) {
                     unitInput.value = '1 hour';
                 }
                this.toggleServiceDropdown(dropdown);
                this.calculateTotal();
            }
        });

                 // Bind input events for total calculation
         [costInput, unitInput].forEach(input => {
             input.addEventListener('input', () => this.calculateTotal());
         });

        // Close dropdown when clicking outside
        document.addEventListener('click', (e) => {
            if (!e.target.closest('.input-with-dropdown')) {
                this.closeServiceDropdown(dropdown);
            }
        });
    }

    removeServiceRow(serviceId) {
        const serviceRow = document.querySelector(`[data-service-id="${serviceId}"]`);
        if (serviceRow) {
            serviceRow.remove();
            this.currentServices = this.currentServices.filter(id => id !== serviceId);
            this.calculateTotal();
        }
    }

    toggleServiceDropdown(dropdown) {
        dropdown.classList.toggle('show');
    }

    closeServiceDropdown(dropdown) {
        dropdown.classList.remove('show');
    }

    toggleClientDropdown() {
        const dropdown = document.getElementById('clientDropdown');
        dropdown.classList.toggle('show');
        
        if (dropdown.classList.contains('show')) {
            this.populateClientDropdown();
        }
    }

    closeClientDropdown() {
        document.getElementById('clientDropdown').classList.remove('show');
    }

    populateClientDropdown() {
        const dropdown = document.getElementById('clientDropdown');
        dropdown.innerHTML = this.clients.map(client => `
            <div class="dropdown-item" onclick="app.selectClient('${client.name}')">
                ${client.name}
            </div>
        `).join('');
    }

    selectClient(clientName) {
        document.getElementById('clientName').value = clientName;
        this.closeClientDropdown();
    }

              calculateTotal() {
         let total = 0;
         
         document.querySelectorAll('.service-item').forEach(serviceRow => {
             const cost = parseFloat(serviceRow.querySelector('.service-cost').value) || 0;
             const unitValue = serviceRow.querySelector('.service-unit').value.trim();
             
             // If unit is empty or N/A, use cost as is (1 unit)
             // If unit contains a number, extract it and multiply
             let multiplier = 1;
             if (unitValue && unitValue.toLowerCase() !== 'n/a') {
                 const numberMatch = unitValue.match(/(\d+(?:\.\d+)?)/);
                 if (numberMatch) {
                     multiplier = parseFloat(numberMatch[1]);
                 }
             }
             
             total += cost * multiplier;
         });

         document.getElementById('total').textContent = `$${total.toFixed(2)}`;
     }

    saveService() {
        const name = document.getElementById('newServiceName').value.trim();
        const cost = parseFloat(document.getElementById('newServiceCost').value) || 0;
        const rate = parseFloat(document.getElementById('newServiceRate').value) || null;

        if (!name) {
            alert('Please enter a service name');
            return;
        }

        const service = { name, cost, rate };
        this.services.push(service);
        localStorage.setItem('services', JSON.stringify(this.services));

        // Clear form
        document.getElementById('newServiceName').value = '';
        document.getElementById('newServiceCost').value = '';
        document.getElementById('newServiceRate').value = '';

        this.loadSavedData();
        alert('Service saved successfully!');
    }

    saveClient() {
        const name = document.getElementById('newClientName').value.trim();

        if (!name) {
            alert('Please enter a client name');
            return;
        }

        if (this.clients.some(client => client.name.toLowerCase() === name.toLowerCase())) {
            alert('Client already exists');
            return;
        }

        this.clients.push({ name });
        localStorage.setItem('clients', JSON.stringify(this.clients));

        // Clear form
        document.getElementById('newClientName').value = '';

        this.loadSavedData();
        alert('Client saved successfully!');
    }

    loadSavedData() {
        // Load saved services
        const savedServicesList = document.getElementById('savedServicesList');
        savedServicesList.innerHTML = this.services.map((service, index) => `
            <div class="saved-item">
                <div class="saved-item-info">
                    <div class="saved-item-name">${service.name}</div>
                    <div class="saved-item-details">
                        Default Cost: $${service.cost}
                        ${service.rate ? ` | Rate: $${service.rate}/hr` : ''}
                    </div>
                </div>
                <button type="button" class="delete-btn" onclick="app.deleteService(${index})">
                    <i class="fas fa-trash"></i>
                </button>
            </div>
        `).join('');

        // Load saved clients
        const savedClientsList = document.getElementById('savedClientsList');
        savedClientsList.innerHTML = this.clients.map((client, index) => `
            <div class="saved-item">
                <div class="saved-item-info">
                    <div class="saved-item-name">${client.name}</div>
                </div>
                <button type="button" class="delete-btn" onclick="app.deleteClient(${index})">
                    <i class="fas fa-trash"></i>
                </button>
            </div>
        `).join('');
    }

    deleteService(index) {
        if (confirm('Are you sure you want to delete this service?')) {
            this.services.splice(index, 1);
            localStorage.setItem('services', JSON.stringify(this.services));
            this.loadSavedData();
        }
    }

    deleteClient(index) {
        if (confirm('Are you sure you want to delete this client?')) {
            this.clients.splice(index, 1);
            localStorage.setItem('clients', JSON.stringify(this.clients));
            this.loadSavedData();
        }
    }

    validateInvoice() {
        const clientName = document.getElementById('clientName').value.trim();
        if (!clientName) {
            alert('Please enter a client name');
            return false;
        }

        const services = document.querySelectorAll('.service-item');
        if (services.length === 0) {
            alert('Please add at least one service');
            return false;
        }

        for (let service of services) {
            const name = service.querySelector('.service-name').value.trim();
            const cost = service.querySelector('.service-cost').value;
            
            if (!name || !cost) {
                alert('Please fill in all service details');
                return false;
            }
        }

        return true;
    }

    getInvoiceData() {
        const clientName = document.getElementById('clientName').value.trim();
        const invoiceDate = document.getElementById('invoiceDate').value;
        const invoiceId = document.getElementById('invoiceId').value;
        
                 const services = [];
         document.querySelectorAll('.service-item').forEach(serviceRow => {
             const name = serviceRow.querySelector('.service-name').value.trim();
             const cost = parseFloat(serviceRow.querySelector('.service-cost').value) || 0;
             const unitValue = serviceRow.querySelector('.service-unit').value.trim();
             
             // If unit is empty, set as N/A
             const unit = unitValue || 'N/A';
             
             // Calculate total based on unit
             let multiplier = 1;
             if (unitValue && unitValue.toLowerCase() !== 'n/a') {
                 const numberMatch = unitValue.match(/(\d+(?:\.\d+)?)/);
                 if (numberMatch) {
                     multiplier = parseFloat(numberMatch[1]);
                 }
             }
             const total = cost * multiplier;
             
             services.push({ name, cost, unit, total });
         });

                          const total = services.reduce((sum, service) => sum + service.total, 0);

         return {
             clientName,
             invoiceDate,
             invoiceId,
             services,
             total
         };
    }

    showPreview() {
        if (!this.validateInvoice()) return;

        const data = this.getInvoiceData();
        const previewContent = document.getElementById('previewContent');
        
        previewContent.innerHTML = this.generateInvoiceHTML(data);
        
        const modal = document.getElementById('previewModal');
        modal.classList.add('show');
    }

    closePreview() {
        document.getElementById('previewModal').classList.remove('show');
    }

         generateInvoiceHTML(data) {
         // Fix timezone issue by creating date in local timezone
         const [year, month, day] = data.invoiceDate.split('-');
         const localDate = new Date(parseInt(year), parseInt(month) - 1, parseInt(day));
         const formattedDate = localDate.toLocaleDateString('en-US', {
             year: 'numeric',
             month: 'long',
             day: 'numeric'
         });

        return `
            <div class="invoice-preview">
                <div class="invoice-header">
                    <div class="invoice-logo">
                        <img src="images/logo.png" alt="LIBER" class="invoice-logo-image">
                    </div>
                    <div class="invoice-details">
                        <h2>INVOICE</h2>
                    </div>
                </div>
                
                <div class="invoice-info">
                    <table>
                        <tr>
                            <td>Client:</td>
                            <td>${data.clientName}</td>
                        </tr>
                        <tr>
                            <td>Invoice ID:</td>
                            <td>${data.invoiceId}</td>
                        </tr>
                        <tr>
                            <td>Date:</td>
                            <td>${formattedDate}</td>
                        </tr>
                    </table>
                </div>
                
                <table class="services-table">
                                         <thead>
                         <tr>
                             <th>Service</th>
                             <th>Cost</th>
                             <th>Unit</th>
                             <th class="amount">Total</th>
                         </tr>
                     </thead>
                    <tbody>
                                                 ${data.services.map(service => `
                             <tr>
                                 <td>${service.name}</td>
                                 <td>$${service.cost.toFixed(2)}</td>
                                 <td>${service.unit}</td>
                                 <td class="amount">$${service.total.toFixed(2)}</td>
                             </tr>
                         `).join('')}
                    </tbody>
                </table>
                
                                 <div style="text-align: right; margin-bottom: 30px;">
                     <div style="font-size: 1.2em; font-weight: bold;">
                         <strong>Total: $${data.total.toFixed(2)}</strong>
                     </div>
                 </div>
                
                <div class="invoice-footer">
                    <p>LIBER CREATIVE LLC © 2025</p>
                </div>
            </div>
        `;
    }

    async generatePDF() {
        if (!this.validateInvoice()) return;

        const data = this.getInvoiceData();
        
        // Create PDF using jsPDF
        const { jsPDF } = window.jspdf;
        const doc = new jsPDF();
        
        // Set font
        doc.setFont('helvetica');
        
        // Add logo (simulated with text)
        doc.setFontSize(24);
        doc.setFont('helvetica', 'bold');
        doc.text('LIBER', 20, 30);
        
        // Invoice title
        doc.setFontSize(20);
        doc.text('INVOICE', 150, 30);
        
        // Invoice details
        doc.setFontSize(12);
        doc.setFont('helvetica', 'normal');
        doc.text('Client:', 20, 60);
        doc.text(data.clientName, 50, 60);
        
        doc.text('Invoice ID:', 20, 70);
        doc.text(data.invoiceId, 50, 70);
        
                 // Fix timezone issue by creating date in local timezone
         const [year, month, day] = data.invoiceDate.split('-');
         const localDate = new Date(parseInt(year), parseInt(month) - 1, parseInt(day));
         const formattedDate = localDate.toLocaleDateString('en-US', {
             year: 'numeric',
             month: 'long',
             day: 'numeric'
         });
         doc.text('Date:', 20, 80);
         doc.text(formattedDate, 50, 80);
        
                 // Services table
         const tableData = data.services.map(service => [
             service.name,
             `$${service.cost.toFixed(2)}`,
             service.unit,
             `$${service.total.toFixed(2)}`
         ]);
         
         doc.autoTable({
             startY: 100,
             head: [['Service', 'Cost', 'Unit', 'Total']],
            body: tableData,
            theme: 'grid',
            headStyles: { fillColor: [0, 0, 0] },
            styles: { fontSize: 10 }
        });
        
                 // Total
         const finalY = doc.lastAutoTable.finalY + 10;
         doc.setFont('helvetica', 'bold');
         doc.text('Total:', 150, finalY);
         doc.text(`$${data.total.toFixed(2)}`, 180, finalY);
        
                 // Footer
         doc.setFont('helvetica', 'normal');
         doc.setFontSize(10);
         doc.text('LIBER CREATIVE LLC © 2025', 105, finalY + 20, { align: 'center' });
        
        // Save PDF
        const filename = `invoice_${data.invoiceId}.pdf`;
        doc.save(filename);
        
        // Increment invoice counter
        this.invoiceCounter++;
        localStorage.setItem('invoiceCounter', this.invoiceCounter.toString());
        this.generateInvoiceId();
        
        alert('PDF generated successfully!');
    }
}

// Initialize the application when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.app = new InvoiceGenerator();
});
