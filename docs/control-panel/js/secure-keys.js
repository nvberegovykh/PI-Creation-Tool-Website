/**
 * Secure Key Management for Liber Apps Control Panel
 * Fetches decryption keys from Google Drive to decrypt sensitive data
 */

class SecureKeyManager {
    constructor() {
        // Default GitHub Gist URL - can be overridden in settings
        // This URL is obfuscated to prevent easy discovery
        this.defaultKeyUrl = this.decodeUrl('aHR0cHM6Ly9naXN0LmdpdGh1YnVzZXJjb250ZW50LmNvbS9udmJlcmVnb3Z5a2gvZmQ1M2JiNzM5MDNlZTA5ZjFlNjJlYTdlMTgwYjg4OGMvcmF3LzNjMWEzYWMxZTVlZmYxNzU5NGJlYWMzMTQ5MTlhZTIyMWU3NDc5NmQvbGliZXItc2VjdXJlLWtleXMuanNvbg==');
        this.keyUrl = null;
        this.cachedKeys = null;
        this.keyCacheExpiry = 30 * 60 * 1000; // 30 minutes
        this.lastFetch = 0;
    }

    /**
     * Decode base64 URL to prevent easy discovery
     */
    decodeUrl(encoded) {
        try {
            return atob(encoded);
        } catch (error) {
            console.error('Failed to decode URL:', error);
            return '';
        }
    }

    /**
     * Set the secure keys URL (GitHub Gist, private repo, etc.)
     * This should be called by admin during setup
     */
    setKeySource(url) {
        this.keyUrl = url;
        localStorage.setItem('liber_keys_url', url);
    }

    /**
     * Get the key source URL
     */
    getKeySource() {
        if (!this.keyUrl) {
            this.keyUrl = localStorage.getItem('liber_keys_url') || this.defaultKeyUrl;
        }
        return this.keyUrl;
    }

    /**
     * Generate default admin credentials for fallback
     */
    async generateDefaultCredentials() {
        // Use environment variables or generate random fallback
        const adminPassword = 'FALLBACK_PASSWORD_' + Math.random().toString(36).substring(2, 15);
        const adminHash = await this.generateAdminHash(adminPassword);
        
        return {
            admin: {
                username: 'admin_fallback',
                email: 'admin@fallback.local',
                passwordHash: adminHash,
                role: 'admin'
            },
            system: {
                masterKeyHash: 'fallback_system_key_' + Math.random().toString(36).substring(2, 15)
            }
        };
    }

    /**
     * Clear all encrypted data when keys change
     */
    clearAllEncryptedData() {
        const keysToRemove = [
            'liber_users',
            'liber_session',
            'liber_current_user',
            'liber_user_password',
            'liber_keys_url' // Also clear the cached URL
        ];
        
        keysToRemove.forEach(key => {
            localStorage.removeItem(key);
        });
        
        // Clear cache
        this.cachedKeys = null;
        this.lastFetch = 0;
        this.keyUrl = null; // Force re-fetch of URL
        
        console.log('Cleared all encrypted data due to key change');
    }

    /**
     * Fetch keys from secure source (GitHub Gist, private repo, etc.)
     */
    async fetchKeys() {
        const url = this.getKeySource();
        
        // If no URL configured, use default credentials
        if (!url) {
            console.warn('No key source URL configured. Using default credentials.');
            return await this.generateDefaultCredentials();
        }

        try {
            // Check cache first
            if (this.cachedKeys && (Date.now() - this.lastFetch) < this.keyCacheExpiry) {
                return this.cachedKeys;
            }

            // Fetch from secure source
            const response = await fetch(url);
            if (!response.ok) {
                console.warn(`Failed to fetch keys from ${url}: ${response.status} ${response.statusText}`);
                console.warn('Falling back to default credentials. Please set up your Gist file.');
                return await this.generateDefaultCredentials();
            }

            const keysData = await response.json();
            
            // Validate keys structure
            if (!this.validateKeys(keysData)) {
                console.warn('Invalid keys format from Gist. Using default credentials.');
                return await this.generateDefaultCredentials();
            }

            // Check if the Gist contains placeholder hash
            const placeholderHash = 'a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456';
            const correctHash = '597ada4b660937a7f075955cea7fb16ba964806bc135f88855d61f370a2f59e2';
            
            if (keysData.admin && keysData.admin.passwordHash === placeholderHash) {
                console.warn('Gist contains placeholder hash. Using default credentials.');
                console.warn('Please update your Gist with the correct hash. Run this in browser console:');
                console.warn('await window.secureKeyManager.getCorrectAdminHash()');
                return await this.generateDefaultCredentials();
            }
            
            // If we have the correct hash, clear any old encrypted data
            if (keysData.admin && keysData.admin.passwordHash === correctHash) {
                console.log('✅ Correct hash detected in Gist.');
                // Only clear data if we're switching from fallback to Gist
                if (this.cachedKeys && this.cachedKeys.admin && 
                    this.cachedKeys.admin.username === 'admin_fallback') {
                    console.log('Switching from fallback to Gist keys. Clearing old encrypted data...');
                    this.clearAllEncryptedData();
                }
            }

            // Check if we're switching from fallback to Gist keys
            const wasUsingFallback = this.cachedKeys && this.cachedKeys.admin && 
                                   this.cachedKeys.admin.username === 'admin_fallback' &&
                                   this.cachedKeys.system.masterKeyHash.startsWith('fallback_system_key_');
            
            const isNowUsingGist = keysData.admin && keysData.admin.username !== 'admin_fallback' &&
                                 !keysData.system.masterKeyHash.startsWith('fallback_system_key_');

            // If switching from fallback to Gist, clear old encrypted data
            if (wasUsingFallback && isNowUsingGist) {
                console.log('Switching from fallback to Gist keys. Clearing old encrypted data...');
                this.clearAllEncryptedData();
            }

            // Cache the keys
            this.cachedKeys = keysData;
            this.lastFetch = Date.now();

            return keysData;
        } catch (error) {
            console.error('Error fetching keys:', error);
            console.warn('Falling back to default credentials. Please set up your Gist file.');
            return await this.generateDefaultCredentials();
        }
    }

    /**
     * Validate keys structure
     */
    validateKeys(keys) {
        return keys && 
               typeof keys === 'object' &&
               keys.admin &&
               keys.system &&
               keys.admin.passwordHash &&
               keys.system.masterKeyHash;
    }

    /**
     * Get admin credentials from secure keys
     */
    async getAdminCredentials() {
        const keys = await this.fetchKeys();
        return keys.admin;
    }

    /**
     * Get system master key from secure keys
     */
    async getSystemKey() {
        const keys = await this.fetchKeys();
        return keys.system.masterKeyHash;
    }

    /**
     * Generate admin password hash for comparison
     */
    async generateAdminHash(password) {
        const salt = 'liber_admin_salt_2024';
        const encoder = new TextEncoder();
        const data = encoder.encode(password + salt);
        const hashBuffer = await crypto.subtle.digest('SHA-256', data);
        const hashArray = Array.from(new Uint8Array(hashBuffer));
        return hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
    }

    /**
     * Get the correct hash for the admin password
     * This is the hash that should be in your Gist file
     */
    async getCorrectAdminHash() {
        // This method should be called with the actual password
        // For security, we don't hardcode the password here
        console.warn('Please provide the actual admin password to generate the correct hash');
        return 'HASH_NOT_AVAILABLE';
    }

    /**
     * Test key connectivity
     */
    async testConnection() {
        try {
            const keys = await this.fetchKeys();
            const url = this.getKeySource();
            
            if (!url) {
                return { success: true, message: 'Using default credentials (no Gist configured)' };
            }
            
            // Try to fetch from Gist
            const response = await fetch(url);
            if (response.ok) {
                return { success: true, message: 'Keys accessible from Gist' };
            } else {
                return { success: false, message: `Gist returned ${response.status}. Using fallback credentials.` };
            }
        } catch (error) {
            return { success: false, message: `Connection failed: ${error.message}. Using fallback credentials.` };
        }
    }

    /**
     * Clear cached keys (for security)
     */
    clearCache() {
        this.cachedKeys = null;
        this.lastFetch = 0;
    }

    /**
     * Get Mailgun configuration from secure Gist
     * 
     * PRODUCTION SETUP:
     * 1. Add your own domain to Mailgun (not sandbox)
     * 2. Configure DNS records as provided by Mailgun
     * 3. Update the domain below to your production domain
     * 4. Update the Gist with your production API key
     * 
     * SANDBOX LIMITATIONS:
     * - Can only send to 5 authorized recipients per month
     * - Cannot send to any email address
     * - For testing only
     */
    async getMailgunConfig() {
        try {
            const gistUrl = this.decodeUrl('aHR0cHM6Ly9naXN0LmdpdGh1YnVzZXJjb250ZW50LmNvbS9udmJlcmVnb3Z5a2gvNjBkYTlmNWFkODA4YWYxNjJkM2M1NzAwYjgzYTEyZWYvcmF3L2JlY2NjNGY2NjBiNWVhMTAzNGU1MDFlOGI3ODM3YjQ5ZDUzNWNkNGEvbWFpbGd1bi1jb25maWcuanNvbg==');
            console.log('Fetching Mailgun config from Gist...');
            const response = await fetch(gistUrl);
            if (!response.ok) {
                throw new Error(`Failed to fetch Mailgun config: ${response.status} ${response.statusText}`);
            }
            const config = await response.json();
            console.log('Mailgun config loaded successfully:', { hasMailgun: !!config.mailgun, hasApiKey: !!config.mailgun?.apiKey });
            if (!config.mailgun || !config.mailgun.apiKey) {
                throw new Error('Invalid Mailgun configuration format - missing mailgun.apiKey');
            }
            
            // PRODUCTION: Replace with your own domain
            const domain = 'mail.liberpict.com'; // Your production domain
            
            // SANDBOX: For testing only (limited to authorized recipients)
            // const domain = 'sandbox96d3d2543629448cba4e500e0da88a60.mailgun.org';
            
            return {
                apiKey: config.mailgun.apiKey,
                domain: domain
            };
        } catch (error) {
            console.error('Error loading Mailgun config from Gist:', error);
            throw new Error('Mailgun configuration not available. Please check your Gist configuration.');
        }
    }
}

// Initialize secure key manager
window.secureKeyManager = new SecureKeyManager();
