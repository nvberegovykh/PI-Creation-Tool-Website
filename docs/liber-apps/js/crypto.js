/**
 * Crypto Module for Liber Apps Control Panel
 * Handles encryption/decryption of sensitive user data
 */

class CryptoManager {
    constructor() {
        this.algorithm = 'AES-GCM';
        this.keyLength = 256;
        this.ivLength = 12;
        this.saltLength = 16;
        this.iterations = 100000;
    }

    /**
     * Generate a random salt
     */
    generateSalt(length = this.saltLength) {
        const array = new Uint8Array(length);
        crypto.getRandomValues(array);
        return array;
    }

    /**
     * Generate a random IV
     */
    generateIV(length = this.ivLength) {
        const array = new Uint8Array(length);
        crypto.getRandomValues(array);
        return array;
    }

    /**
     * Derive a key from password using PBKDF2
     */
    async deriveKey(password, salt) {
        const encoder = new TextEncoder();
        const passwordBuffer = encoder.encode(password);
        
        const baseKey = await crypto.subtle.importKey(
            'raw',
            passwordBuffer,
            'PBKDF2',
            false,
            ['deriveBits', 'deriveKey']
        );

        const derivedKey = await crypto.subtle.deriveKey(
            {
                name: 'PBKDF2',
                salt: salt,
                iterations: this.iterations,
                hash: 'SHA-256'
            },
            baseKey,
            {
                name: this.algorithm,
                length: this.keyLength
            },
            false,
            ['encrypt', 'decrypt']
        );

        return derivedKey;
    }

    /**
     * Encrypt data
     */
    async encrypt(data, password) {
        try {
            const salt = this.generateSalt();
            const iv = this.generateIV();
            const key = await this.deriveKey(password, salt);
            
            const encoder = new TextEncoder();
            const dataBuffer = encoder.encode(JSON.stringify(data));
            
            const encryptedData = await crypto.subtle.encrypt(
                {
                    name: this.algorithm,
                    iv: iv
                },
                key,
                dataBuffer
            );

            // Combine salt, IV, and encrypted data
            const combined = new Uint8Array(salt.length + iv.length + encryptedData.byteLength);
            combined.set(salt, 0);
            combined.set(iv, salt.length);
            combined.set(new Uint8Array(encryptedData), salt.length + iv.length);

            // Convert to base64 for storage
            return btoa(String.fromCharCode(...combined));
        } catch (error) {
            console.error('Encryption error:', error);
            throw new Error('Failed to encrypt data');
        }
    }

    /**
     * Decrypt data
     */
    async decrypt(encryptedData, password) {
        try {
            // Convert from base64
            const combined = new Uint8Array(
                atob(encryptedData).split('').map(char => char.charCodeAt(0))
            );

            // Extract salt, IV, and encrypted data
            const salt = combined.slice(0, this.saltLength);
            const iv = combined.slice(this.saltLength, this.saltLength + this.ivLength);
            const data = combined.slice(this.saltLength + this.ivLength);

            const key = await this.deriveKey(password, salt);
            
            const decryptedData = await crypto.subtle.decrypt(
                {
                    name: this.algorithm,
                    iv: iv
                },
                key,
                data
            );

            const decoder = new TextDecoder();
            const decryptedString = decoder.decode(decryptedData);
            
            return JSON.parse(decryptedString);
        } catch (error) {
            console.error('Decryption error:', error);
            throw new Error('Failed to decrypt data');
        }
    }

    /**
     * Hash password for storage
     */
    async hashPassword(password) {
        try {
            const encoder = new TextEncoder();
            const data = encoder.encode(password);
            const hashBuffer = await crypto.subtle.digest('SHA-256', data);
            const hashArray = Array.from(new Uint8Array(hashBuffer));
            return hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
        } catch (error) {
            console.error('Password hashing error:', error);
            throw new Error('Failed to hash password');
        }
    }

    /**
     * Generate a secure random string
     */
    generateSecureString(length = 32) {
        const charset = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
        let result = '';
        const array = new Uint8Array(length);
        crypto.getRandomValues(array);
        
        for (let i = 0; i < length; i++) {
            result += charset[array[i] % charset.length];
        }
        
        return result;
    }

    /**
     * Generate a secure token
     */
    generateToken() {
        return this.generateSecureString(64);
    }

    /**
     * Encrypt user data with master key
     */
    async encryptUserData(userData, masterKey) {
        try {
            return await this.encrypt(userData, masterKey);
        } catch (error) {
            console.error('User data encryption failed:', error);
            throw error;
        }
    }

    /**
     * Decrypt user data with master key
     */
    async decryptUserData(encryptedData, masterKey) {
        try {
            return await this.decrypt(encryptedData, masterKey);
        } catch (error) {
            console.error('User data decryption failed:', error);
            throw error;
        }
    }

    /**
     * Verify password against stored hash
     */
    async verifyPassword(password, storedHash) {
        try {
            const passwordHash = await this.hashPassword(password);
            return passwordHash === storedHash;
        } catch (error) {
            console.error('Password verification failed:', error);
            return false;
        }
    }

    /**
     * Secure storage wrapper
     */
    async secureStore(key, data, password) {
        try {
            const encrypted = await this.encrypt(data, password);
            localStorage.setItem(key, encrypted);
            return true;
        } catch (error) {
            console.error('Secure storage failed:', error);
            return false;
        }
    }

    /**
     * Secure retrieval wrapper
     */
    async secureRetrieve(key, password) {
        try {
            const encrypted = localStorage.getItem(key);
            if (!encrypted) return null;
            
            return await this.decrypt(encrypted, password);
        } catch (error) {
            console.error('Secure retrieval failed:', error);
            return null;
        }
    }

    /**
     * Clear secure storage
     */
    clearSecureStorage() {
        const keys = Object.keys(localStorage);
        keys.forEach(key => {
            if (key.startsWith('liber_')) {
                localStorage.removeItem(key);
            }
        });
    }
}

// Create global instance
window.cryptoManager = new CryptoManager();

// Export for module systems
if (typeof module !== 'undefined' && module.exports) {
    module.exports = CryptoManager;
}
