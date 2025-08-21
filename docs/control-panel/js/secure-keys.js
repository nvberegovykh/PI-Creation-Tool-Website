/**
 * Secure Key Management for Liber Apps Control Panel
 * Fetches decryption keys from Google Drive to decrypt sensitive data
 */

class SecureKeyManager {
    constructor() {
        // Default GitHub Gist URL - can be overridden in settings
        // This URL is obfuscated to prevent easy discovery
        // New rotated commit URL
        this.defaultKeyUrl = this.decodeUrl('aHR0cHM6Ly9naXN0LmdpdGh1YnVzZXJjb250ZW50LmNvbS9udmJlcmVnb3Z5a2gvZmQ1M2JiNzM5MDNlZTA5ZjFlNjJlYTdlMTgwYjg4OGMvcmF3LzQ1ZjZiYTE3YWU3NjkxYmY1MzkzMTJhZjlkYmZjZGQwODE1M2JjNjAvbGliZXItc2VjdXJlLWtleXMuanNvbg==');
        this.gistUsername = 'nvberegovykh';
        this.gistFilename = 'liber-secure-keys.json';
        this.lastCommitHash = null;
        this.commitCacheExpiry = 5 * 60 * 1000; // 5 mins
        this.lastCommitFetch = 0;
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
        
        if (window.__DEBUG_KEYS__) console.log('Cleared all encrypted data due to key change');
    }

    /**
     * Force refresh of keys from Gist
     */
    forceRefreshKeys() {
        this.cachedKeys = null;
        this.lastFetch = 0;
        this.keyUrl = null;
        if (window.__DEBUG_KEYS__) console.log('Forced refresh of keys from Gist');
    }

    /**
     * Fetch keys from secure source (GitHub Gist, private repo, etc.)
     */
    async fetchKeys() {
        try {
            // Get latest commit hash dynamically
            const rawUrl = await this.getLatestRawUrl();
            if (window.__DEBUG_KEYS__) console.log('Fetching from dynamic URL:', rawUrl.substring(0, 50) + '... (redacted)');
            
            const response = await fetch(rawUrl);
            if (!response.ok) {
                const errMsg = `Gist fetch failed: ${response.status} ${response.statusText}`;
                if (window.__DEBUG_KEYS__) console.error(errMsg);
                throw new Error(errMsg);
            }
            const keysData = await response.json();
            
            // Structure validation only (no hash checks)
            if (!keysData.firebase || !keysData.admin || !keysData.system) {
                throw new Error('Invalid Gist structure');
            }
            
            this.cachedKeys = keysData;
            this.lastFetch = Date.now();
            if (window.__DEBUG_KEYS__) console.log('Keys loaded (redacted):', Object.keys(keysData));
            return keysData;
        } catch (error) {
            console.error('Secure keys load failed:', error);
            // Show UI error instead of fallback
            document.body.innerHTML = '<div style="color:red;text-align:center;padding:20px;">Failed to load secure config from Gist. Check URL/network and reload.</div>';
            throw error;
        }
    }

    async getLatestRawUrl() {
        if (this.lastCommitHash && Date.now() - this.lastCommitFetch < this.commitCacheExpiry) {
            return `https://gist.githubusercontent.com/${this.gistUsername}/${this.gistId}/raw/${this.lastCommitHash}/${this.gistFilename}`;
        }
        
        // Fetch metadata to get latest commit
        const apiUrl = `https://api.github.com/gists/${this.gistId}`;
        const response = await fetch(apiUrl);
        if (!response.ok) throw new Error(`Gist API failed: ${response.status}`);
        
        const data = await response.json();
        this.lastCommitHash = data.history[0].version; // Latest commit hash
        this.lastCommitFetch = Date.now();
        if (window.__DEBUG_KEYS__) console.log('Fetched latest Gist commit:', this.lastCommitHash);
        
        return `https://gist.githubusercontent.com/${this.gistUsername}/${this.gistId}/raw/${this.lastCommitHash}/${this.gistFilename}`;
    }

    /**
     * Validate keys structure
     */
    validateKeys(keys) {
        // Basic validation - admin and system keys are required
        const basicValid = keys && 
               typeof keys === 'object' &&
               keys.admin &&
               keys.system &&
               keys.admin.passwordHash &&
               keys.system.masterKeyHash;
        
        if (!basicValid) {
            console.warn('Basic keys validation failed');
            return false;
        }
        
        // Firebase validation - check if config exists and has basic fields
        if (keys.firebase) {
            console.log('✅ Firebase config found in Gist');
            
            // Basic validation - just check if we have the essential fields
            if (!keys.firebase.apiKey) {
                console.warn('⚠️ Firebase config missing apiKey');
            }
            if (!keys.firebase.projectId) {
                console.warn('⚠️ Firebase config missing projectId');
            }
            
            // Continue even if some fields are missing - Firebase will handle validation
            console.log('✅ Firebase config validation passed (continuing with available fields)');
        } else {
            console.error('❌ Firebase configuration is required but not found in secure keys');
            console.error('Please add Firebase configuration to your Gist file');
            return false;
        }
        
        return true;
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
     * Get all keys (for Firebase service)
     */
    async getKeys() {
        return await this.fetchKeys();
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
     * Generate correct admin hash for the default admin password
     * This is the hash that should be in your Gist configuration
     */
    async generateCorrectAdminHash() {
        try {
            // The default admin password is likely 'admin' or similar
            // You can change this to match your actual admin password
            const adminPassword = 'admin'; // Change this to your actual admin password
            const hash = await this.generateAdminHash(adminPassword);
            console.log('Correct admin hash for password "' + adminPassword + '":', hash);
            console.log('Copy this hash to your Gist configuration file');
            return hash;
        } catch (error) {
            console.error('Error generating admin hash:', error);
            return null;
        }
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
     * Debug Gist configuration
     */
    async debugGistConfig() {
        console.log('=== Gist Configuration Debug (redacted) ===');
        const url = this.getKeySource();
        
        try {
            const response = await fetch(url);
            console.log('Gist response status:', response.status);
            console.log('Gist response ok:', response.ok);
            
            if (response.ok) {
                const data = await response.json();
                console.log('Gist data structure:', Object.keys(data));
                console.log('Has Firebase config:', !!data.firebase);
                return data;
            } else {
                console.error('Gist fetch failed:', response.status, response.statusText);
                return null;
            }
        } catch (error) {
            console.error('Gist fetch error:', error);
            return null;
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
            const mailgunGistUrl = this.decodeUrl('aHR0cHM6Ly9naXN0LmdpdGh1YnVzZXJjb250ZW50LmNvbS9udmJlcmVnb3Z5a2gvNjBkYTlmNWFkODA4YWYxNjJkM2M1NzAwYjgzYTEyZWYvcmF3L2JlY2NjNGY2NjBiNWVhMTAzNGU1MDFlOGI3ODM3YjQ5ZDUzNWNkNGEvbWFpbGd1bi1jb25maWcuanNvbg==');
            console.log('Fetching Mailgun config from separate Gist...');
            const response = await fetch(mailgunGistUrl);
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

    /**
     * Debug function to show Gist configuration
     */
    async debugGistConfig() {
        console.log('=== Debugging Gist Configuration (redacted) ===');
        
        try {
            const url = this.getKeySource();
            // URL redacted in logs
            
            const response = await fetch(url);
            if (!response.ok) {
                console.error('Failed to fetch Gist:', response.status, response.statusText);
                return;
            }
            
            const config = await response.json();
            console.log('Gist configuration loaded (redacted)');
            
            // Check admin hash
            if (config.admin && config.admin.passwordHash) {
                console.log('Admin hash check performed (redacted)');
            }
            
            // Check system key
            if (config.system && config.system.masterKeyHash) {
                console.log('System master key present (redacted)');
            }
            
        } catch (error) {
            console.error('Gist debug error:', error);
        }
    }
}

// Initialize secure key manager
window.secureKeyManager = new SecureKeyManager();

// Add global debug function
window.debugGistConfig = function() {
    if (window.secureKeyManager) {
        return window.secureKeyManager.debugGistConfig();
    } else {
        console.error('Secure key manager not available');
        return null;
    }
};
