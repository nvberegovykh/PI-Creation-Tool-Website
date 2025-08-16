// Crypto Manager for Invoice App - User-Specific Encryption
class InvoiceCryptoManager {
    constructor() {
        this.saltLength = 16;
        this.ivLength = 12;
        this.keyLength = 256;
        this.iterations = 100000;
        this.userKey = null; // Will be set when user logs in
    }

    // Set user-specific key when user logs in
    setUserKey(userPassword) {
        this.userKey = userPassword;
    }

    // Clear user key on logout
    clearUserKey() {
        this.userKey = null;
    }

    // Check if user is authenticated
    isAuthenticated() {
        return this.userKey !== null;
    }

    generateSalt(length = this.saltLength) {
        const array = new Uint8Array(length);
        crypto.getRandomValues(array);
        return Array.from(array, byte => byte.toString(16).padStart(2, '0')).join('');
    }

    generateIV(length = this.ivLength) {
        const array = new Uint8Array(length);
        crypto.getRandomValues(array);
        return Array.from(array, byte => byte.toString(16).padStart(2, '0')).join('');
    }

    async deriveKey(password, salt) {
        const encoder = new TextEncoder();
        const keyMaterial = await crypto.subtle.importKey(
            'raw',
            encoder.encode(password),
            { name: 'PBKDF2' },
            false,
            ['deriveBits', 'deriveKey']
        );

        return crypto.subtle.deriveKey(
            {
                name: 'PBKDF2',
                salt: encoder.encode(salt),
                iterations: this.iterations,
                hash: 'SHA-256'
            },
            keyMaterial,
            { name: 'AES-GCM', length: this.keyLength },
            true,
            ['encrypt', 'decrypt']
        );
    }

    async encrypt(data, password) {
        try {
            const salt = this.generateSalt();
            const iv = this.generateIV();
            const key = await this.deriveKey(password, salt);
            
            const encoder = new TextEncoder();
            const encodedData = encoder.encode(JSON.stringify(data));
            
            const encryptedData = await crypto.subtle.encrypt(
                { name: 'AES-GCM', iv: new Uint8Array(iv.match(/.{1,2}/g).map(byte => parseInt(byte, 16))) },
                key,
                encodedData
            );
            
            const encryptedArray = new Uint8Array(encryptedData);
            const encryptedHex = Array.from(encryptedArray, byte => byte.toString(16).padStart(2, '0')).join('');
            
            return {
                salt,
                iv,
                data: encryptedHex
            };
        } catch (error) {
            console.error('Encryption error:', error);
            throw new Error('Failed to encrypt data');
        }
    }

    async decrypt(encryptedData, password) {
        try {
            const { salt, iv, data } = encryptedData;
            const key = await this.deriveKey(password, salt);
            
            const encryptedArray = new Uint8Array(data.match(/.{1,2}/g).map(byte => parseInt(byte, 16)));
            
            const decryptedData = await crypto.subtle.decrypt(
                { name: 'AES-GCM', iv: new Uint8Array(iv.match(/.{1,2}/g).map(byte => parseInt(byte, 16))) },
                key,
                encryptedArray
            );
            
            const decoder = new TextDecoder();
            const decryptedString = decoder.decode(decryptedData);
            
            return JSON.parse(decryptedString);
        } catch (error) {
            console.error('Decryption error:', error);
            throw new Error('Failed to decrypt data');
        }
    }

    async hashPassword(password) {
        const encoder = new TextEncoder();
        const data = encoder.encode(password);
        const hashBuffer = await crypto.subtle.digest('SHA-256', data);
        const hashArray = Array.from(new Uint8Array(hashBuffer));
        return hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
    }

    async secureStore(key, data) {
        try {
            if (!this.isAuthenticated()) {
                throw new Error('User not authenticated');
            }
            const encryptedData = await this.encrypt(data, this.userKey);
            localStorage.setItem(key, JSON.stringify(encryptedData));
            return true;
        } catch (error) {
            console.error('Secure store error:', error);
            return false;
        }
    }

    async secureRetrieve(key) {
        try {
            if (!this.isAuthenticated()) {
                throw new Error('User not authenticated');
            }
            const encryptedData = localStorage.getItem(key);
            if (!encryptedData) return null;
            
            const parsedData = JSON.parse(encryptedData);
            return await this.decrypt(parsedData, this.userKey);
        } catch (error) {
            console.error('Secure retrieve error:', error);
            return null;
        }
    }

    async secureDelete(key) {
        try {
            localStorage.removeItem(key);
            return true;
        } catch (error) {
            console.error('Secure delete error:', error);
            return false;
        }
    }

    // Specific methods for invoice app data
    async saveInvoices(invoices) {
        return await this.secureStore('liber_invoices', invoices);
    }

    async loadInvoices() {
        const invoices = await this.secureRetrieve('liber_invoices');
        return invoices || [];
    }

    async saveServices(services) {
        return await this.secureStore('liber_services', services);
    }

    async loadServices() {
        const services = await this.secureRetrieve('liber_services');
        return services || [];
    }

    async saveClients(clients) {
        return await this.secureStore('liber_clients', clients);
    }

    async loadClients() {
        const clients = await this.secureRetrieve('liber_clients');
        return clients || [];
    }

    async saveInvoiceCounter(counter) {
        return await this.secureStore('liber_invoice_counter', counter);
    }

    async loadInvoiceCounter() {
        const counter = await this.secureRetrieve('liber_invoice_counter');
        return counter || 1;
    }

    async deleteInvoice(invoiceId) {
        const invoices = await this.loadInvoices();
        const filteredInvoices = invoices.filter(invoice => invoice.invoiceId !== invoiceId);
        return await this.saveInvoices(filteredInvoices);
    }
}

// Initialize crypto manager
window.invoiceCryptoManager = new InvoiceCryptoManager();
