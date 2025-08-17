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
            
            // Add timestamp for debugging
            const storageData = {
                encrypted: encryptedData,
                timestamp: new Date().toISOString(),
                version: '1.0'
            };
            
            localStorage.setItem(key, JSON.stringify(storageData));
            
            // Create backup
            await this.backupData(key, data);
            
            console.log(`✅ Data stored successfully for key: ${key}`, { timestamp: storageData.timestamp });
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
            const storedData = localStorage.getItem(key);
            if (!storedData) {
                console.log(`📭 No data found for key: ${key}`);
                return null;
            }
            
            const parsedData = JSON.parse(storedData);
            
            // Handle both old format (direct encrypted data) and new format (with metadata)
            let encryptedData;
            if (parsedData.encrypted) {
                // New format with metadata
                encryptedData = parsedData.encrypted;
                console.log(`📖 Data retrieved for key: ${key}`, { 
                    timestamp: parsedData.timestamp,
                    version: parsedData.version 
                });
            } else {
                // Old format - direct encrypted data
                encryptedData = parsedData;
                console.log(`📖 Legacy data retrieved for key: ${key}`);
            }
            
            const decryptedData = await this.decrypt(encryptedData, this.userKey);
            console.log(`✅ Data decrypted successfully for key: ${key}`);
            return decryptedData;
        } catch (error) {
            console.error('Secure retrieve error:', error);
            console.error('Error details:', {
                key,
                error: error.message,
                stack: error.stack
            });
            
            // Try to restore from backup
            console.log('🔄 Attempting to restore from backup...');
            const backupData = await this.restoreFromBackup(key);
            if (backupData) {
                console.log(`✅ Successfully restored ${key} from backup`);
                return backupData;
            }
            
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

    async saveProviders(providers) {
        return await this.secureStore('liber_providers', providers);
    }

    async loadProviders() {
        const providers = await this.secureRetrieve('liber_providers');
        return providers || [];
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

    // Debug method to check all stored data
    async debugStorage() {
        console.log('🔍 Debugging localStorage...');
        const keys = Object.keys(localStorage);
        const liberKeys = keys.filter(key => key.startsWith('liber_'));
        
        console.log('Found Liber keys:', liberKeys);
        
        for (const key of liberKeys) {
            try {
                const data = localStorage.getItem(key);
                const parsed = JSON.parse(data);
                console.log(`Key: ${key}`, {
                    hasData: !!data,
                    isObject: typeof parsed === 'object',
                    hasEncrypted: !!parsed.encrypted,
                    hasTimestamp: !!parsed.timestamp,
                    timestamp: parsed.timestamp || 'N/A'
                });
            } catch (error) {
                console.log(`Key: ${key} - Error parsing:`, error.message);
            }
        }
    }

    // Method to migrate old data format to new format
    async migrateOldData() {
        console.log('🔄 Migrating old data format...');
        const keys = ['liber_invoices', 'liber_services', 'liber_providers', 'liber_clients', 'liber_invoice_counter'];
        
        for (const key of keys) {
            try {
                const storedData = localStorage.getItem(key);
                if (!storedData) continue;
                
                const parsedData = JSON.parse(storedData);
                
                // If it's already in new format, skip
                if (parsedData.encrypted) continue;
                
                // Migrate old format to new format
                const newFormat = {
                    encrypted: parsedData,
                    timestamp: new Date().toISOString(),
                    version: '1.0'
                };
                
                localStorage.setItem(key, JSON.stringify(newFormat));
                console.log(`✅ Migrated ${key} to new format`);
            } catch (error) {
                console.error(`❌ Error migrating ${key}:`, error.message);
            }
        }
    }

    // Backup data to sessionStorage as fallback
    async backupData(key, data) {
        try {
            const backupKey = `backup_${key}`;
            const backupData = {
                data: data,
                timestamp: new Date().toISOString(),
                user: localStorage.getItem('liber_current_user')
            };
            sessionStorage.setItem(backupKey, JSON.stringify(backupData));
            console.log(`💾 Backup created for ${key}`);
        } catch (error) {
            console.error('Backup error:', error);
        }
    }

    // Restore data from backup if main storage fails
    async restoreFromBackup(key) {
        try {
            const backupKey = `backup_${key}`;
            const backupData = sessionStorage.getItem(backupKey);
            if (!backupData) return null;
            
            const parsed = JSON.parse(backupData);
            const currentUser = localStorage.getItem('liber_current_user');
            
            // Only restore if it's for the same user
            if (parsed.user === currentUser) {
                console.log(`🔄 Restoring ${key} from backup`);
                return parsed.data;
            }
        } catch (error) {
            console.error('Restore error:', error);
        }
        return null;
    }
}

// Initialize crypto manager
window.invoiceCryptoManager = new InvoiceCryptoManager();
