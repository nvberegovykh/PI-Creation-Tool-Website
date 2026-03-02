/**
 * WALL-E AI Assistant Integration Module for Liber Apps Control Panel
 * Powered by GPT-4o and OpenAI Assistant API
 */

class ChatGPTIntegration {
    constructor() {
        // Configuration will be loaded from Gist
        this.apiKey = null;
        this.assistantId = null;
        this.proxyUrl = null; // Optional serverless proxy base URL
        this.proxyAuth = null; // Optional proxy auth token
        this.isEnabled = false;
        this.chatHistory = [];
        this.currentContext = 'liber-apps';
        this.fileUploads = [];
        this.maxFileSize = 25 * 1024 * 1024; // 25MB limit
        this.supportedFileTypes = ['image/*', 'application/pdf', 'text/*', 'application/json', 'application/xml', 'text/csv'];
        this.isExpanded = false;
        this.threadId = null;
        this.configLoaded = false;
        this.currentUserId = null;
        this.maxHistoryItems = 50; // Keep last 50 messages
        this.cryptoKey = null; // WebCrypto key for local chat encryption
        this.cryptoReady = false;
        this.responsesModel = 'gpt-5-mini';
        this.chatFallbackModel = 'gpt-5-mini';
        this.generatedLocalReports = new Map();
        this._jsPdfLoadPromise = null;
        
        // Thread management
        this.savedThreads = [];
        this.currentThreadName = 'New Chat';
        this.maxThreads = 20; // Keep last 20 threads
        
        this.init();
    }

    /**
     * Initialize WALL-E integration
     */
    async init() {
        try {
            await this.loadConfiguration();
            if (this.isEnabled) {
                // Assistants API is deprecated; keep startup lightweight and Responses-first.
                Promise.resolve().then(() => {
                    console.log('WALL-E startup: Responses API mode enabled');
                });
            }
        } catch (error) {
            console.warn('⚠️ WALL-E configuration failed, but widget will still be created:', error.message);
        }
        
        // Always create the interface regardless of configuration status
        try {
            await this.initCrypto();
            this.loadSavedThreads(); // Load saved threads
            this.createChatInterface();
            this.setupEventListeners();
            this.loadChatHistory();
            this.displayChatHistory();
            
            // Set initial state based on screen size
            if (window.innerWidth <= 768) {
                // On mobile, let the dashboard control visibility
                this.isExpanded = false;
                // Don't auto-expand on mobile - let dashboard handle it
            } else {
                // On desktop, always ensure widget is visible
                this.isExpanded = true;
                this.expandChat();
            }
            
            console.log('✅ WALL-E widget initialized successfully');
        } catch (error) {
            console.error('❌ Critical error initializing WALL-E widget:', error);
        }
    }

    /**
     * Helper: base URL for OpenAI (proxy-aware)
     */
    getOpenAIBase() {
        return this.proxyUrl || 'https://api.openai.com';
    }

    /**
     * Helper: build headers, optionally JSON content-type and Assistants beta header
     */
    buildOpenAIHeaders({ json = true, beta = null } = {}) {
        const headers = {};
        if (json) headers['Content-Type'] = 'application/json';
        if (beta) headers['OpenAI-Beta'] = beta;
        if (!this.proxyUrl && this.apiKey) {
            headers['Authorization'] = `Bearer ${this.apiKey}`;
        }
        if (this.proxyUrl && this.proxyAuth) {
            headers['X-Proxy-Auth'] = this.proxyAuth;
        }
        return headers;
    }

    /**
     * Helper: perform fetch to OpenAI (or proxy) with sensible defaults
     */
    async openaiFetch(path, { method = 'GET', headers = {}, body = undefined, beta = null, json = true, timeoutMs = 45000 } = {}) {
        const base = this.getOpenAIBase();
        const url = path.startsWith('http') ? path : `${base}${path}`;
        const mergedHeaders = { ...this.buildOpenAIHeaders({ json, beta }), ...headers };
        const controller = new AbortController();
        const timer = setTimeout(() => {
            try { controller.abort(); } catch (_) {}
        }, Math.max(5000, Number(timeoutMs || 45000)));
        try {
            return await fetch(url, { method, headers: mergedHeaders, body, signal: controller.signal });
        } finally {
            clearTimeout(timer);
        }
    }

    /**
     * Ensure assistant supports file attachments
     */
    async ensureAssistantSupportsFiles() {
        try {
            if (!this.assistantId) {
                console.log('Assistant ID not set; skipping file support check');
                return;
            }
            // Try v2 endpoint first (newer API)
            let response = await this.openaiFetch(`/v2/assistants/${this.assistantId}`, {
                beta: 'assistants=v2',
                json: false
            });

            // If v2 fails, try v1 endpoint (fallback)
            if (!response.ok) {
                console.log('v2 endpoint failed for file support check, trying v1...');
                response = await this.openaiFetch(`/v1/assistants/${this.assistantId}`, {
                    beta: 'assistants=v1',
                    json: false
                });
            }

            if (!response.ok) {
                console.warn('⚠️ Could not fetch assistant configuration for file support check, proceeding with current setup');
                console.log('✅ File attachments will be attempted but may not work');
                return;
            }

            const assistant = await response.json();
            
            // Check if assistant uses a model that supports file attachments
            const supportedModels = [
                'gpt-4o',
                'gpt-4o-mini', 
                'gpt-4-turbo',
                'gpt-4-turbo-preview',
                'gpt-4-vision-preview'
            ];

            if (supportedModels.includes(assistant.model)) {
                console.log(`✅ Assistant is using supported model: ${assistant.model}`);
                console.log(`✅ File attachments are supported and ready to use`);
            } else {
                console.warn(`⚠️ Assistant is using model: ${assistant.model} which may not support file attachments`);
                console.warn(`💡 Consider updating to gpt-4o-mini for full file support`);
            }
        } catch (error) {
            console.warn('Error checking assistant configuration:', error);
        }
    }

    /**
     * Check assistant configuration
     */
    async checkAssistantConfig() {
        try {
            if (!this.assistantId) {
                console.log('Assistant ID not set; skipping assistant config check');
                return;
            }
            console.log('Checking assistant configuration...');
            
            // Try v2 endpoint first (newer API)
            let response = await this.openaiFetch(`/v2/assistants/${this.assistantId}`, {
                beta: 'assistants=v2',
                json: false
            });

            // If v2 fails, try v1 endpoint (fallback)
            if (!response.ok) {
                console.log('v2 endpoint failed for config check, trying v1...');
                response = await this.openaiFetch(`/v1/assistants/${this.assistantId}`, {
                    beta: 'assistants=v1',
                    json: false
                });
            }

            if (!response.ok) {
                console.warn(`⚠️ Could not fetch assistant config (${response.status}), but assistant will still work`);
                console.log('✅ Assistant is ready to use despite config check failure');
                return;
            }

            const assistant = await response.json();
            console.log('Assistant Configuration:', {
                id: assistant.id,
                name: assistant.name,
                model: assistant.model,
                instructions: assistant.instructions?.substring(0, 100) + '...',
                tools: assistant.tools?.length || 0
            });
            
            if (assistant.model !== 'gpt-4o-mini') {
                console.warn(`⚠️ Assistant is using model: ${assistant.model}, expected: gpt-4o-mini`);
            } else {
                console.log('✅ Assistant is correctly configured with gpt-4o-mini');
            }
        } catch (error) {
            // Don't crash the widget - just log the error and continue
            console.warn('⚠️ Error checking assistant config (non-critical):', error.message);
            console.log('✅ Assistant will continue to work normally');
        }
    }

    /**
     * Load saved threads for current user
     */
    loadSavedThreads() {
        try {
            const userId = this.getCurrentUserId();
            const threadsKey = `wall_e_saved_threads_${userId}`;
            const savedThreads = localStorage.getItem(threadsKey);
            
            if (savedThreads) {
                let parsed;
                try { parsed = JSON.parse(savedThreads); } catch { parsed = null; }
                if (parsed && parsed.enc && parsed.data) {
                    if (this.cryptoReady) {
                        // Decrypt asynchronously; update selector when done
                        this.decryptString(parsed.data).then((plain) => {
                            try {
                                const arr = JSON.parse(plain || '[]');
                                this.savedThreads = Array.isArray(arr) ? arr : [];
                                if (window.__devLog) window.__devLog('Loaded', this.savedThreads.length, 'encrypted threads');
                                this.updateThreadSelector();
                            } catch {
                                this.savedThreads = [];
                            }
                        });
                    } else {
                        console.warn('Encrypted saved threads present but crypto not ready');
                        this.savedThreads = [];
                    }
                } else if (Array.isArray(parsed)) {
                    this.savedThreads = parsed;
                    if (window.__devLog) window.__devLog('Loaded', this.savedThreads.length, 'saved threads');
                } else {
                    this.savedThreads = [];
                }
            } else {
                if (window.__devLog) window.__devLog('No saved threads found');
            }
        } catch (error) {
            console.error('Failed to load saved threads:', error);
        }
    }

    /**
     * Save threads to localStorage
     */
    saveThreads() {
        try {
            const userId = this.getCurrentUserId();
            const threadsKey = `wall_e_saved_threads_${userId}`;
            
            // Keep only last 20 threads
            const threadsToSave = this.savedThreads.slice(-this.maxThreads);
            
            if (this.cryptoReady) {
                const plain = JSON.stringify(threadsToSave);
                this.encryptString(plain).then((cipher) => {
                    if (cipher) {
                        const envelope = { v: 1, enc: true, data: cipher };
                        localStorage.setItem(threadsKey, JSON.stringify(envelope));
                        console.log(`Saved ${threadsToSave.length} encrypted threads for user: ${userId}`);
                    } else {
                        localStorage.setItem(threadsKey, JSON.stringify(threadsToSave));
                        console.log(`Saved ${threadsToSave.length} (fallback plaintext) threads for user: ${userId}`);
                    }
                }).catch(() => {
                    localStorage.setItem(threadsKey, JSON.stringify(threadsToSave));
                    console.log(`Saved ${threadsToSave.length} (fallback plaintext) threads for user: ${userId}`);
                });
            } else {
                localStorage.setItem(threadsKey, JSON.stringify(threadsToSave));
                console.log(`Saved ${threadsToSave.length} threads for user: ${userId}`);
            }
        } catch (error) {
            console.error('Failed to save threads:', error);
        }
    }

    /**
     * Create a new thread
     */
    async createNewThread(threadName = null) {
        try {
            // Create new thread on OpenAI
            const newThreadId = await this.createThread();
            
            // Generate thread name if not provided
            const name = threadName || `Chat ${new Date().toLocaleString()}`;
            
            // Add to saved threads
            const newThread = {
                id: newThreadId,
                name: name,
                createdAt: new Date().toISOString(),
                lastUsed: new Date().toISOString()
            };
            
            this.savedThreads.push(newThread);
            this.saveThreads();
            
            // Set as current thread
            this.threadId = newThreadId;
            this.currentThreadName = name;
            
            // Clear chat history for new thread
            this.chatHistory = [];
            this.displayChatHistory();
            
            // Update thread selector
            this.updateThreadSelector();
            
            console.log(`Created new thread: ${name} (${newThreadId})`);
            return newThreadId;
        } catch (error) {
            console.error('Failed to create new thread:', error);
            throw error;
        }
    }

    /**
     * Switch to a different thread
     */
    async switchToThread(threadId) {
        try {
            const thread = this.savedThreads.find(t => t.id === threadId);
            if (!thread) {
                throw new Error('Thread not found');
            }
            
            // Update last used timestamp
            thread.lastUsed = new Date().toISOString();
            this.saveThreads();
            
            // Set as current thread
            this.threadId = threadId;
            this.currentThreadName = thread.name;
            
            // Load chat history for this thread
            this.loadChatHistoryForThread(threadId);
            
            // Update thread selector
            this.updateThreadSelector();
            
            console.log(`Switched to thread: ${thread.name} (${threadId})`);
        } catch (error) {
            console.error('Failed to switch thread:', error);
            throw error;
        }
    }

    /**
     * Load chat history for specific thread
     */
    loadChatHistoryForThread(threadId) {
        try {
            const userId = this.getCurrentUserId();
            const historyKey = `wall_e_chat_history_${userId}_${threadId}`;
            const savedHistory = localStorage.getItem(historyKey);
            
            if (savedHistory) {
                let parsed;
                try { parsed = JSON.parse(savedHistory); } catch { parsed = null; }
                if (parsed && parsed.enc && parsed.data) {
                    if (this.cryptoReady) {
                        this.decryptString(parsed.data).then((plain) => {
                            try {
                                const arr = JSON.parse(plain || '[]');
                                this.chatHistory = Array.isArray(arr) ? arr.slice(-this.maxHistoryItems) : [];
                                console.log(`Loaded ${this.chatHistory.length} encrypted messages for thread: ${threadId}`);
                                this.displayChatHistory();
                            } catch {
                                this.chatHistory = [];
                                this.displayChatHistory();
                            }
                        });
                        return;
                    } else {
                        console.warn('Encrypted chat history present but crypto not ready');
                        this.chatHistory = [];
                    }
                } else if (Array.isArray(parsed)) {
                    this.chatHistory = parsed.slice(-this.maxHistoryItems);
                    if (window.__devLog) window.__devLog('Loaded', this.chatHistory.length, 'messages for thread');
                } else {
                    this.chatHistory = [];
                }
            } else {
                this.chatHistory = [];
                if (window.__devLog) window.__devLog('No history found for thread');
            }
            
            this.displayChatHistory();
        } catch (error) {
            console.error('Failed to load thread history:', error);
            this.chatHistory = [];
            this.displayChatHistory();
        }
    }

    /**
     * Save chat history for current thread
     */
    saveChatHistoryForThread() {
        try {
            if (!this.threadId) return;
            
            const userId = this.getCurrentUserId();
            const historyKey = `wall_e_chat_history_${userId}_${this.threadId}`;
            
            // Keep only last 50 messages
            const historyToSave = this.chatHistory.slice(-this.maxHistoryItems);
            if (this.cryptoReady) {
                const plain = JSON.stringify(historyToSave);
                this.encryptString(plain).then((cipher) => {
                    if (cipher) {
                        const envelope = { v: 1, enc: true, data: cipher };
                        localStorage.setItem(historyKey, JSON.stringify(envelope));
                        console.log(`Saved ${historyToSave.length} encrypted messages for thread: ${this.threadId}`);
                    } else {
                        localStorage.setItem(historyKey, JSON.stringify(historyToSave));
                        console.log(`Saved ${historyToSave.length} (fallback plaintext) messages for thread: ${this.threadId}`);
                    }
                }).catch(() => {
                    localStorage.setItem(historyKey, JSON.stringify(historyToSave));
                    console.log(`Saved ${historyToSave.length} (fallback plaintext) messages for thread: ${this.threadId}`);
                });
            } else {
                localStorage.setItem(historyKey, JSON.stringify(historyToSave));
                console.log(`Saved ${historyToSave.length} messages for thread: ${this.threadId}`);
            }
            
            // Update clear history button visibility
            this.updateClearHistoryButton(historyToSave.length > 0);
        } catch (error) {
            console.error('Failed to save thread history:', error);
        }
    }

    /**
     * Delete a thread
     */
    async deleteThread(threadId) {
        try {
            // Remove from saved threads
            this.savedThreads = this.savedThreads.filter(t => t.id !== threadId);
            this.saveThreads();
            
            // Delete thread history
            const userId = this.getCurrentUserId();
            const historyKey = `wall_e_chat_history_${userId}_${threadId}`;
            localStorage.removeItem(historyKey);
            
            // If this was the current thread, create a new one
            if (this.threadId === threadId) {
                await this.createNewThread();
            }
            
            // Update thread selector
            this.updateThreadSelector();
            
            console.log(`Deleted thread: ${threadId}`);
        } catch (error) {
            console.error('Failed to delete thread:', error);
            throw error;
        }
    }

    /**
     * Rename current thread
     */
    renameCurrentThread(newName) {
        try {
            if (!this.threadId || !newName.trim()) return;
            
            const thread = this.savedThreads.find(t => t.id === this.threadId);
            if (thread) {
                thread.name = newName.trim();
                this.currentThreadName = newName.trim();
                this.saveThreads();
                this.updateThreadSelector();
                console.log(`Renamed thread to: ${newName}`);
            }
        } catch (error) {
            console.error('Failed to rename thread:', error);
        }
    }

    /**
     * Update thread selector UI
     */
    updateThreadSelector() {
        const threadSelector = document.getElementById('chatgpt-thread-selector');
        if (!threadSelector) return;
        
        // Update current thread name
        const currentThreadName = document.getElementById('chatgpt-current-thread');
        if (currentThreadName) {
            currentThreadName.textContent = this.currentThreadName;
        }
        
        // Update thread list
        const threadList = document.getElementById('chatgpt-thread-list');
        if (threadList) {
            threadList.innerHTML = '';
            
            this.savedThreads.forEach(thread => {
                const threadItem = document.createElement('div');
                threadItem.className = `thread-item ${thread.id === this.threadId ? 'active' : ''}`;
                threadItem.innerHTML = `
                    <div class="thread-info">
                        <span class="thread-name">${thread.name}</span>
                        <small class="thread-date">${new Date(thread.lastUsed).toLocaleDateString()}</small>
                    </div>
                    <div class="thread-actions">
                        <button class="thread-rename" title="Rename" onclick="chatgptIntegration.renameThreadPrompt('${thread.id}')">
                            <i class="fas fa-edit"></i>
                        </button>
                        <button class="thread-delete" title="Delete" onclick="chatgptIntegration.deleteThread('${thread.id}')">
                            <i class="fas fa-trash"></i>
                        </button>
                    </div>
                `;
                
                threadItem.addEventListener('click', (e) => {
                    if (!e.target.closest('.thread-actions')) {
                        this.switchToThread(thread.id);
                    }
                });
                
                threadList.appendChild(threadItem);
            });
        }
    }

    /**
     * Load configuration from GitHub Gist using existing SecureKeyManager
     */
    async loadConfiguration() {
        try {
            const keys = await window.secureKeyManager.getKeys();

            // 1) Configure secure proxy URL (preferred)
            const region = (keys && keys.firebase && keys.firebase.functionsRegion) || 'europe-west1';
            const projectId = (keys && keys.firebase && keys.firebase.projectId) || 'liber-apps-cca20';
            const defaultProxy = `https://${region}-${projectId}.cloudfunctions.net/openaiProxy`;
            this.proxyUrl = (keys && keys.aiProxyUrl) || defaultProxy;

            // 2) Optional direct keys (not required when proxy is present)
            if (keys && keys.openai) {
                if (keys.openai.apiKey) this.apiKey = keys.openai.apiKey;
                if (keys.openai.assistantId) this.assistantId = keys.openai.assistantId;
                if (keys.openai.responsesModel) this.responsesModel = String(keys.openai.responsesModel || this.responsesModel);
                if (keys.openai.chatFallbackModel) this.chatFallbackModel = String(keys.openai.chatFallbackModel || this.chatFallbackModel);
            }

            // Enable if either proxy or apiKey is available
            this.isEnabled = !!(this.proxyUrl || this.apiKey);
            this.configLoaded = true;
            console.log(this.isEnabled ? 'WALL-E configured (proxy or key present)' : 'WALL-E not configured');
        } catch (error) {
            // Fallback: derive proxy URL from known project/region so widget still works
            this.proxyUrl = 'https://europe-west1-liber-apps-cca20.cloudfunctions.net/openaiProxy';
            this.apiKey = null;
            this.assistantId = null;
            this.isEnabled = true; // proxy assumed available
            this.configLoaded = true;
            console.warn('WALL-E: keys unavailable; using default proxy URL');
        }
    }

    /**
     * Initialize WebCrypto for encrypting chat history at rest
     */
    async initCrypto() {
        try {
            const userId = this.getCurrentUserId() || 'anonymous';
            const deviceSalt = this.getOrCreateDeviceSalt();
            const material = await window.crypto.subtle.importKey(
                'raw',
                new TextEncoder().encode(`${userId}:${deviceSalt}`),
                { name: 'PBKDF2' },
                false,
                ['deriveKey']
            );
            this.cryptoKey = await window.crypto.subtle.deriveKey(
                {
                    name: 'PBKDF2',
                    salt: new TextEncoder().encode('liber.wall_e.local_history'),
                    iterations: 100000,
                    hash: 'SHA-256'
                },
                material,
                { name: 'AES-GCM', length: 256 },
                false,
                ['encrypt', 'decrypt']
            );
            this.cryptoReady = true;
            console.log('WALL-E local encryption initialized');
        } catch (e) {
            console.warn('Local crypto init failed; falling back to plaintext history', e);
            this.cryptoReady = false;
        }
    }

    getOrCreateDeviceSalt() {
        const key = 'wall_e_device_salt_v1';
        let salt = localStorage.getItem(key);
        if (!salt) {
            const arr = new Uint8Array(16);
            window.crypto.getRandomValues(arr);
            salt = Array.from(arr).map(b => b.toString(16).padStart(2, '0')).join('');
            localStorage.setItem(key, salt);
        }
        return salt;
    }

    async encryptString(plaintext) {
        if (!this.cryptoReady || !this.cryptoKey) return null;
        const iv = window.crypto.getRandomValues(new Uint8Array(12));
        const enc = new TextEncoder();
        const cipherBuf = await window.crypto.subtle.encrypt(
            { name: 'AES-GCM', iv },
            this.cryptoKey,
            enc.encode(plaintext)
        );
        const payload = new Uint8Array(iv.length + new Uint8Array(cipherBuf).length);
        payload.set(iv, 0);
        payload.set(new Uint8Array(cipherBuf), iv.length);
        return btoa(String.fromCharCode(...payload));
    }

    async decryptString(b64) {
        if (!this.cryptoReady || !this.cryptoKey) return null;
        try {
            const raw = Uint8Array.from(atob(b64), c => c.charCodeAt(0));
            const iv = raw.slice(0, 12);
            const data = raw.slice(12);
            const plainBuf = await window.crypto.subtle.decrypt(
                { name: 'AES-GCM', iv },
                this.cryptoKey,
                data
            );
            return new TextDecoder().decode(plainBuf);
        } catch (e) {
            console.warn('Decrypt failed; possibly legacy plaintext history', e);
            return null;
        }
    }

    /**
     * Decode base64 URL using the same method as SecureKeyManager
     */
    decodeUrl(encoded) {
        try {
            return atob(encoded);
        } catch (error) {
            console.error('Failed to decode WALL-E URL:', error);
            return '';
        }
    }

    /**
     * Setup event listeners
     */
    setupEventListeners() {
        // Listen for context changes
        document.addEventListener('app-context-changed', (e) => {
            this.updateContext(e.detail.context);
        });
        // When app shell opens, refresh initial guidelines (context may have changed)
        window.addEventListener('liber:app-shell-open', () => {
            if (this.chatHistory.length === 0) {
                setTimeout(() => this.displayChatHistory(), 150);
            }
        });
    }

    /**
     * Create chat interface
     */
    createChatInterface() {
        console.log('Creating chat interface...');
        const chatHTML = `
            <div id="chatgpt-widget" class="chatgpt-widget ${this.isEnabled ? 'enabled' : 'disabled'}">
                <div class="chatgpt-header" id="chatgpt-header">
                    <div class="chatgpt-title">
                        <img src="images/wall_e.svg" alt="WALL-E" class="chatgpt-icon">
                        <span>WALL-E</span>
                    </div>
                    <div class="chatgpt-controls">
                        <button class="chatgpt-new-thread" id="chatgpt-new-thread" title="New Chat">
                            <i class="fas fa-plus"></i>
                        </button>
                        <button class="chatgpt-thread-menu" id="chatgpt-thread-menu" title="Saved Chats">
                            <i class="fas fa-list"></i>
                        </button>
                        <button class="chatgpt-clear-history" id="chatgpt-clear-history" title="Clear Chat History" style="display: none;">
                            <i class="fas fa-trash"></i>
                        </button>
                        <button class="chatgpt-toggle" id="chatgpt-toggle" title="Toggle Chat">
                            <i class="fas fa-chevron-up"></i>
                        </button>
                    </div>
                </div>
                
                <!-- Thread Selector Dropdown -->
                <div class="chatgpt-thread-selector" id="chatgpt-thread-selector" style="display: none;">
                    <div class="thread-selector-header">
                        <span id="chatgpt-current-thread">${this.currentThreadName}</span>
                        <button class="thread-selector-close" id="thread-selector-close">
                            <i class="fas fa-times"></i>
                        </button>
                    </div>
                    <div class="thread-list" id="chatgpt-thread-list">
                        <!-- Thread items will be populated here -->
                    </div>
                </div>
                
                <div class="chatgpt-body" id="chatgpt-body" style="display: ${window.innerWidth <= 768 ? 'flex' : 'none'};">
                    <div class="chatgpt-messages" id="chatgpt-messages">
                        <div class="chatgpt-welcome">
                            <img src="images/wall_e.svg" alt="WALL-E" class="welcome-icon">
                            <h4>Wall-eeeee!</h4>
                            <p>Any help?</p>
                            ${!this.isEnabled ? '<p class="setup-notice"><strong>⚠️ Configuration Required:</strong> WALL-E configuration could not be loaded. Please check the Gist setup.</p>' : ''}
                        </div>
                    </div>
                    <div class="chatgpt-input-area">
                        <div class="chatgpt-file-upload" id="chatgpt-file-upload">
                            <input type="file" id="chatgpt-file-input" multiple accept="${this.supportedFileTypes.join(',')}" style="display: none;">
                            <button class="chatgpt-upload-btn" id="chatgpt-upload-btn" title="Attach files">
                                <i class="fas fa-paperclip"></i>
                            </button>
                        </div>
                        <div class="chatgpt-input-container">
                            <textarea id="chatgpt-input" placeholder="Ask WALL-E anything, upload files, or request image generation..." rows="1"></textarea>
                            <button class="chatgpt-send" id="chatgpt-send" title="Send message">
                                <i class="fas fa-paper-plane"></i>
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        `;

        // Add to body if not exists
        if (!document.getElementById('chatgpt-widget')) {
            console.log('Adding widget to body...');
            document.body.insertAdjacentHTML('beforeend', chatHTML);
            console.log('Widget added successfully');
        } else {
            console.log('Widget already exists');
        }

        this.setupChatEventListeners();
    }

    /**
     * Setup chat event listeners
     */
    setupChatEventListeners() {
        const header = document.getElementById('chatgpt-header');
        const toggle = document.getElementById('chatgpt-toggle');
        const send = document.getElementById('chatgpt-send');
        const input = document.getElementById('chatgpt-input');
        const uploadBtn = document.getElementById('chatgpt-upload-btn');
        const fileInput = document.getElementById('chatgpt-file-input');
        const clearHistoryBtn = document.getElementById('chatgpt-clear-history');
        const newThreadBtn = document.getElementById('chatgpt-new-thread');
        const threadMenuBtn = document.getElementById('chatgpt-thread-menu');
        const threadSelector = document.getElementById('chatgpt-thread-selector');
        const threadSelectorClose = document.getElementById('thread-selector-close');
        const widget = document.getElementById('chatgpt-widget');
        const body = document.getElementById('chatgpt-body');

        // Add header click listener for mobile expansion
        if (header) {
            console.log('Setting up header click listener');
            header.addEventListener('click', (e) => {
                console.log('Header clicked');
                // Don't trigger if clicking on toggle button or other controls
                if (e.target.closest('.chatgpt-controls')) {
                    console.log('Clicked on controls, ignoring');
                    return;
                }
                e.preventDefault();
                e.stopPropagation();
                this.toggleChat();
            });
        } else {
            console.warn('Header element not found');
        }

        // Add specific click listener for WALL-E icon and title
        const title = document.querySelector('.chatgpt-title');
        if (title) {
            console.log('Setting up title click listener');
            title.addEventListener('click', (e) => {
                console.log('Title clicked');
                e.preventDefault();
                e.stopPropagation();
                this.toggleChat();
            });
        } else {
            console.warn('Title element not found');
        }

        // Add click listener for the WALL-E icon specifically
        const icon = document.querySelector('.chatgpt-icon');
        if (icon) {
            console.log('Setting up icon click listener');
            icon.addEventListener('click', (e) => {
                console.log('Icon clicked');
                e.preventDefault();
                e.stopPropagation();
                this.toggleChat();
            });
        } else {
            console.warn('Icon element not found');
        }

        if (toggle) {
            console.log('Setting up toggle click listener');
            toggle.addEventListener('click', (e) => {
                console.log('Toggle clicked');
                e.preventDefault();
                e.stopPropagation();
                this.toggleChat();
            });
        } else {
            console.warn('Toggle element not found');
        }

        // New thread button
        if (newThreadBtn) {
            newThreadBtn.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation();
                this.createNewThread();
            });
        }

        // Thread menu button
        if (threadMenuBtn) {
            threadMenuBtn.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation();
                this.toggleThreadSelector();
            });
        }

        // Thread selector close button
        if (threadSelectorClose) {
            threadSelectorClose.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation();
                this.hideThreadSelector();
            });
        }

        if (clearHistoryBtn) {
            clearHistoryBtn.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation();
                this.clearChatHistory();
            });
        }

        if (send) {
            send.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation();
                this.sendMessage();
            });
        }

        if (input) {
            input.addEventListener('keydown', (e) => {
                if (e.key === 'Enter' && !e.shiftKey) {
                    e.preventDefault();
                    this.sendMessage();
                }
            });

            input.addEventListener('input', () => {
                this.autoResizeTextarea(input);
            });
        }

        if (uploadBtn) {
            uploadBtn.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation();
                fileInput.click();
            });
        }

        if (fileInput) {
            fileInput.addEventListener('change', (e) => this.handleFileUpload(e));
        }

        // Add drag and drop functionality
        if (widget) {
            widget.addEventListener('dragover', (e) => {
                e.preventDefault();
                e.stopPropagation();
                this.handleDragOver(e);
            });

            widget.addEventListener('dragleave', (e) => {
                e.preventDefault();
                e.stopPropagation();
                this.handleDragLeave(e);
            });

            widget.addEventListener('drop', (e) => {
                e.preventDefault();
                e.stopPropagation();
                this.handleDrop(e);
            });
        }

        // Add paste event listener to the entire document for global paste
        document.addEventListener('paste', (e) => {
            // Only handle paste if WALL-E is expanded and focused
            if (this.isExpanded && (e.target === input || input.contains(e.target))) {
                this.handlePaste(e);
            }
        });
    }

    /**
     * Handle paste events for file uploads
     */
    handlePaste(e) {
        // Prevent duplicate processing
        if (e.defaultPrevented) {
            return;
        }
        
        const items = e.clipboardData?.items;
        if (!items) return;

        const files = [];
        let hasFiles = false;

        for (let i = 0; i < items.length; i++) {
            const item = items[i];
            
            if ((item.type || '').indexOf('image') !== -1) {
                const file = item.getAsFile();
                if (file) {
                    files.push(file);
                    hasFiles = true;
                }
            } else if ((item.type || '').indexOf('text') !== -1) {
                // Handle text paste - let it go through normally
                continue;
            }
        }

        if (hasFiles) {
            e.preventDefault();
            if (window.__devLog) window.__devLog('Processing', files.length, 'files from paste');
            this.processFiles(files);
        }
    }

    /**
     * Handle drag over events
     */
    handleDragOver(e) {
        e.preventDefault();
        e.stopPropagation();
        
        const widget = document.getElementById('chatgpt-widget');
        if (widget) {
            widget.classList.add('drag-over');
        }
    }

    /**
     * Handle drag leave events
     */
    handleDragLeave(e) {
        e.preventDefault();
        e.stopPropagation();
        
        const widget = document.getElementById('chatgpt-widget');
        if (widget) {
            widget.classList.remove('drag-over');
        }
    }

    /**
     * Handle drop events
     */
    handleDrop(e) {
        e.preventDefault();
        e.stopPropagation();
        
        const widget = document.getElementById('chatgpt-widget');
        if (widget) {
            widget.classList.remove('drag-over');
        }

        const files = Array.from(e.dataTransfer.files);
        if (files.length > 0) {
            this.processFiles(files);
        }
    }

    /**
     * Process uploaded files
     */
    processFiles(files) {
        files.forEach(file => {
            // Check if file is already in uploads (prevent duplicates)
            const isDuplicate = this.fileUploads.some(existingFile => 
                existingFile.name === file.name && 
                existingFile.size === file.size &&
                existingFile.lastModified === file.lastModified
            );
            
            if (isDuplicate) {
                console.log(`Skipping duplicate file: ${file.name}`);
                return;
            }
            
            if (file.size > this.maxFileSize) {
                this.showError(`File ${file.name} is too large. Maximum size is 25MB.`);
                return;
            }

            if (!this.isFileTypeSupported(file.type)) {
                this.showError(`File type ${file.type} is not supported.`);
                return;
            }

            this.fileUploads.push(file);
        });

        this.updateFileUploadDisplay();
        
        // Show success message
        if (files.length === 1) {
            this.showSuccess(`File "${files[0].name}" uploaded successfully!`);
        } else {
            this.showSuccess(`${files.length} files uploaded successfully!`);
        }
    }

    /**
     * Show success message
     */
    showSuccess(message) {
        if (window.dashboardManager) {
            window.dashboardManager.showSuccess(message);
        } else {
            console.log('WALL-E Success:', message);
            // Show a simple alert if dashboard manager is not available
            alert('WALL-E Success: ' + message);
        }
    }

    /**
     * Show image generation modal
     */
    showImageGenerationModal() {
        if (!this.isEnabled) {
            this.showError('WALL-E is not configured. Please check the Gist setup.');
            return;
        }

        // Remove any existing modal
        const existingModal = document.querySelector('.modal-overlay');
        if (existingModal) {
            existingModal.remove();
        }

        const modal = document.createElement('div');
        modal.className = 'modal-overlay';
        modal.innerHTML = `
            <div class="modal-content image-gen-modal">
                <div class="modal-header">
                    <h3><img src="images/wall_e.svg" alt="WALL-E" style="width: 20px; height: 20px; margin-right: 8px;"> Generate Image with WALL-E</h3>
                    <button class="modal-close">&times;</button>
                </div>
                <div class="modal-body">
                    <div class="setting-group">
                        <label for="image-prompt">Image Description:</label>
                        <textarea id="image-prompt" placeholder="Describe the image you want to generate..." rows="4"></textarea>
                        <small>Be detailed and specific for better results</small>
                    </div>
                    <div class="setting-group">
                        <label for="image-size">Image Size:</label>
                        <select id="image-size">
                            <option value="1024x1024">Square (1024x1024)</option>
                            <option value="1792x1024">Landscape (1792x1024)</option>
                            <option value="1024x1792">Portrait (1024x1792)</option>
                        </select>
                    </div>
                    <div class="setting-group">
                        <label for="image-quality">Quality:</label>
                        <select id="image-quality">
                            <option value="standard">Standard</option>
                            <option value="hd">HD</option>
                        </select>
                    </div>
                </div>
                <div class="modal-actions">
                    <button class="btn secondary" onclick="this.closest('.modal-overlay').remove()">Cancel</button>
                    <button class="btn" onclick="chatgptIntegration.generateImage()">Generate Image</button>
                </div>
            </div>
        `;

        // Setup modal close
        const closeBtn = modal.querySelector('.modal-close');
        closeBtn.addEventListener('click', () => modal.remove());

        modal.addEventListener('click', (e) => {
            if (e.target === modal) modal.remove();
        });

        document.body.appendChild(modal);
        
        // Focus on the textarea
        const textarea = modal.querySelector('#image-prompt');
        if (textarea) {
            setTimeout(() => textarea.focus(), 100);
        }
    }

    /**
     * Generate image using DALL-E
     */
    async generateImage() {
        // Require either proxy or direct API key
        if (!this.proxyUrl && !this.apiKey) {
            this.showError('WALL-E is not configured. Please check the Gist setup.');
            return;
        }

        const promptInput = document.getElementById('image-prompt');
        const sizeInput = document.getElementById('image-size');
        const qualityInput = document.getElementById('image-quality');

        if (!promptInput || !sizeInput || !qualityInput) {
            this.showError('Image generation modal not found.');
            return;
        }

        const prompt = promptInput.value.trim();
        const size = sizeInput.value;
        const quality = qualityInput.value;

        if (!prompt) {
            this.showError('Please enter an image description.');
            return;
        }

        // Close modal
        const modal = document.querySelector('.modal-overlay');
        if (modal) {
            modal.remove();
        }

        // Add user message to chat
        this.addMessage('user', `Generate image: ${prompt}`);

        // Show typing indicator
        this.addTypingIndicator();

        try {
            const imageUrl = await this.callDALLE(prompt, size, quality);
            this.removeTypingIndicator();
            
            // Add image response
            this.addImageMessage(imageUrl, prompt);
        } catch (error) {
            this.removeTypingIndicator();
            this.addMessage('error', `Image generation failed: ${error.message}`);
        }
    }

    /**
     * Call DALL-E API
     */
    async callDALLE(prompt, size, quality) {
        const response = await this.openaiFetch('/v1/images/generations', {
            method: 'POST',
            beta: null,
            body: JSON.stringify({
                model: 'dall-e-3',
                prompt: prompt,
                n: 1,
                size: size,
                quality: quality
            })
        });

        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.error?.message || 'Failed to generate image');
        }

        const data = await response.json();
        return data.data[0].url;
    }

    /**
     * Add image message to chat
     */
    addImageMessage(imageUrl, prompt) {
        const messagesContainer = document.getElementById('chatgpt-messages');
        if (!messagesContainer) return;

        const messageDiv = document.createElement('div');
        messageDiv.className = 'chatgpt-message assistant';

        const messageHTML = `
            <div class="message-avatar">
                <img src="images/wall_e.svg" alt="WALL-E" class="avatar-icon">
            </div>
            <div class="message-content">
                <div class="message-text">
                    <p>Here's your generated image based on: "${prompt}"</p>
                    <div class="generated-image">
                        <img src="${imageUrl}" alt="Generated image" style="max-width: 100%; border-radius: 8px; margin-top: 8px;">
                        <div class="image-actions">
                            <a href="${imageUrl}" target="_blank" class="btn btn-secondary btn-sm">
                                <i class="fas fa-external-link-alt"></i> Open Full Size
                            </a>
                            <button class="btn btn-secondary btn-sm" onclick="chatgptIntegration.downloadImage('${imageUrl}', '${prompt.replace(/[^a-zA-Z0-9]/g, '_')}')">
                                <i class="fas fa-download"></i> Download
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        `;

        messageDiv.innerHTML = messageHTML;
        messagesContainer.appendChild(messageDiv);
        messageDiv.querySelectorAll('.wall-e-file-download').forEach((btn) => {
            btn.addEventListener('click', (e) => {
                e.preventDefault();
                const fileId = String(btn.getAttribute('data-file-id') || '').trim();
                if (!fileId) return;
                this.downloadGeneratedFile(fileId);
            });
        });

        // Scroll to bottom
        messagesContainer.scrollTop = messagesContainer.scrollHeight;

        // Add to history
        this.chatHistory.push({ role: 'assistant', content: `Generated image: ${prompt}`, imageUrl, timestamp: new Date() });

        // Limit history
        if (this.chatHistory.length > 50) {
            this.chatHistory = this.chatHistory.slice(-50);
        }
    }

    /**
     * Download generated image
     */
    async downloadImage(imageUrl, filename) {
        try {
            const response = await fetch(imageUrl);
            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `wall-e-generated-${filename}.png`;
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            document.body.removeChild(a);
        } catch (error) {
            this.showError('Failed to download image');
        }
    }

    /**
     * Toggle chat visibility
     */
    toggleChat() {
        console.log('Toggling chat visibility:', this.isExpanded);
        if (this.isExpanded) {
            this.collapseChat();
        } else {
            this.expandChat();
        }
        
        // Sync with mobile button state
        this.updateMobileButtonState();
    }

    /**
     * Expand chat
     */
    expandChat() {
        console.log('Expanding chat');
        const widget = document.getElementById('chatgpt-widget');
        const body = document.getElementById('chatgpt-body');
        const toggle = document.getElementById('chatgpt-toggle');
        const icon = toggle.querySelector('i');

        if (widget && body && toggle) {
            body.style.display = 'flex';
            icon.className = 'fas fa-chevron-down';
            widget.classList.add('expanded');
            this.isExpanded = true;
            
            // Focus on input
            const input = document.getElementById('chatgpt-input');
            if (input) {
                setTimeout(() => input.focus(), 100);
            }
        }
        
        // Sync mobile button state
        this.updateMobileButtonState();
    }

    /**
     * Collapse chat
     */
    collapseChat() {
        console.log('Collapsing chat');
        const widget = document.getElementById('chatgpt-widget');
        const body = document.getElementById('chatgpt-body');
        const toggle = document.getElementById('chatgpt-toggle');
        const icon = toggle.querySelector('i');

        if (widget && body && toggle) {
            body.style.display = 'none';
            icon.className = 'fas fa-chevron-up';
            widget.classList.remove('expanded');
            this.isExpanded = false;
        }
        
        // Sync mobile button state
        this.updateMobileButtonState();
    }

    /**
     * Update mobile button state to sync with widget state
     */
    updateMobileButtonState() {
        const mobileWallEBtn = document.getElementById('mobile-wall-e-btn');
        if (mobileWallEBtn) {
            if (this.isExpanded) {
                mobileWallEBtn.classList.add('active');
            } else {
                mobileWallEBtn.classList.remove('active');
            }
        }
    }

    /**
     * Send message to WALL-E
     */
    async sendMessage() {
        console.log('=== SEND MESSAGE STARTED ===');
        
        // Require either proxy or direct API key
        if (!this.proxyUrl && !this.apiKey) {
            this.showError('WALL-E is not configured. Please check the Gist setup.');
            return;
        }

        const input = document.getElementById('chatgpt-input');
        const message = input.value.trim();

        console.log('Message from input: [redacted]');
        console.log('File uploads:', this.fileUploads);
        console.log('File uploads length:', this.fileUploads.length);

        if (!message && this.fileUploads.length === 0) {
            console.log('No message and no files, returning');
            return;
        }

        // Create a new thread if one doesn't exist
        if (!this.threadId) {
            try {
                await this.createNewThread();
            } catch (error) {
                console.error('Failed to create new thread:', error);
                this.addMessage('error', `Failed to create new chat: ${error.message}`);
                return;
            }
        }

        // Check if this is an image generation request
        const isImageRequest = this.isImageGenerationRequest(message);
        console.log('Is image generation request:', isImageRequest);

        // Add user message to chat
        this.addMessage('user', message, this.fileUploads);

        // Store file uploads before clearing
        const filesToSend = [...this.fileUploads];
        console.log('Files to send:', filesToSend);

        // Clear input and file uploads
        input.value = '';
        this.fileUploads = [];
        this.updateFileUploadDisplay();

        // Show typing indicator
        this.addTypingIndicator();

        try {
            if (isImageRequest) {
                console.log('Handling image generation request');
                // Handle image generation directly
                await this.handleImageGenerationRequest(message);
            } else {
                console.log('Handling normal chat message with files count:', filesToSend.length);
                // Normal chat message
                const response = await this.callWALLE(message, filesToSend);
                this.removeTypingIndicator();
                let safeResponse = String(response || '').trim() || 'I could not generate visible output for this request. Please retry, or ask me to return a concise text summary.';
                const localReportMarker = await this.maybeGenerateLocalPdfReportMarker(message, safeResponse);
                if (localReportMarker) safeResponse += `\n\n${localReportMarker}`;
                this.addMessage('assistant', safeResponse);
            }
        } catch (error) {
            console.error('Error in sendMessage:', error);
            this.removeTypingIndicator();
            this.addMessage('error', `Error: ${error.message}`);
        }
    }

    /**
     * Check if message is an image generation request
     */
    isImageGenerationRequest(message) {
        const imageKeywords = [
            'generate an image', 'create an image', 'make an image', 'generate a picture', 
            'create a picture', 'make a picture', 'generate image', 'create image', 
            'make image', 'generate picture', 'create picture', 'make picture',
            'draw', 'paint', 'visualize', 'show me', 'picture of', 'image of'
        ];
        
        const lowerMessage = message.toLowerCase();
        return imageKeywords.some(keyword => lowerMessage.includes(keyword));
    }

    isAddressAnalysisRequest(message) {
        const text = String(message || '').toLowerCase();
        if (!text) return false;
        const hasAnalysisIntent = /(zoning|analysis|report|far|floor area|setback|lot|bbl|borough block lot)/i.test(text);
        const hasAddressLike = /\b\d{1,6}\s+[a-z0-9.'-]+\s+(street|st|avenue|ave|road|rd|boulevard|blvd|lane|ln|place|pl|drive|dr|court|ct|terrace|ter|way)\b/i.test(text)
            || /\b(borough|block|lot)\b/i.test(text);
        return hasAnalysisIntent && hasAddressLike;
    }

    looksLikeClarificationInsteadOfReport(text) {
        const s = String(text || '').toLowerCase();
        if (!s) return false;
        const cues = [
            'quick confirmation',
            'please confirm',
            'do you mean',
            'which do you want',
            'if you want me to proceed',
            'confirm borough',
            'can you confirm'
        ];
        return cues.some((c) => s.includes(c));
    }

    sanitizeFilename(input) {
        return String(input || 'zoning-report')
            .replace(/[^a-z0-9\-_.\s]/gi, ' ')
            .replace(/\s+/g, ' ')
            .trim()
            .slice(0, 80)
            .replace(/\s/g, '_') || 'zoning-report';
    }

    async ensureJsPdfLoaded() {
        if (window.jspdf && window.jspdf.jsPDF) return window.jspdf.jsPDF;
        if (this._jsPdfLoadPromise) return this._jsPdfLoadPromise;
        this._jsPdfLoadPromise = new Promise((resolve, reject) => {
            try {
                const s = document.createElement('script');
                s.src = 'https://cdn.jsdelivr.net/npm/jspdf@2.5.1/dist/jspdf.umd.min.js';
                s.async = true;
                s.onload = () => {
                    if (window.jspdf && window.jspdf.jsPDF) resolve(window.jspdf.jsPDF);
                    else reject(new Error('jsPDF loaded but unavailable'));
                };
                s.onerror = () => reject(new Error('Failed to load jsPDF'));
                document.head.appendChild(s);
            } catch (err) {
                reject(err);
            }
        }).finally(() => {
            this._jsPdfLoadPromise = null;
        });
        return this._jsPdfLoadPromise;
    }

    async buildPdfBlobFromText(title, bodyText) {
        const jsPDFCtor = await this.ensureJsPdfLoaded();
        const doc = new jsPDFCtor({ unit: 'pt', format: 'a4' });
        const pageW = doc.internal.pageSize.getWidth();
        const pageH = doc.internal.pageSize.getHeight();
        const margin = 44;
        const maxW = pageW - margin * 2;
        let y = margin;
        doc.setFont('helvetica', 'bold');
        doc.setFontSize(14);
        const titleLines = doc.splitTextToSize(String(title || 'Zoning Analysis Report'), maxW);
        titleLines.forEach((ln) => {
            if (y > pageH - margin) { doc.addPage(); y = margin; }
            doc.text(String(ln), margin, y);
            y += 18;
        });
        y += 6;
        doc.setFont('helvetica', 'normal');
        doc.setFontSize(10.5);
        const text = String(bodyText || '').replace(/\r\n/g, '\n');
        const lines = doc.splitTextToSize(text, maxW);
        lines.forEach((ln) => {
            if (y > pageH - margin) { doc.addPage(); y = margin; }
            doc.text(String(ln), margin, y);
            y += 14;
        });
        return doc.output('blob');
    }

    async maybeGenerateLocalPdfReportMarker(userMessage, assistantText) {
        try {
            if (!this.isAddressAnalysisRequest(userMessage)) return '';
            const title = `Zoning Analysis Report - ${String(userMessage || '').slice(0, 90)}`;
            const reportBlob = await this.buildPdfBlobFromText(title, assistantText);
            const id = `local_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
            const filename = `${this.sanitizeFilename(title)}.pdf`;
            this.generatedLocalReports.set(id, { blob: reportBlob, filename, createdAt: Date.now() });
            return `[Local report: ${id}|Download zoning analysis PDF]`;
        } catch (err) {
            console.warn('Local report generation failed:', err);
            return '';
        }
    }

    downloadLocalReport(reportId) {
        try {
            const id = String(reportId || '').trim();
            const item = this.generatedLocalReports.get(id);
            if (!item || !item.blob) {
                this.showError('Report is unavailable. Please regenerate.');
                return;
            }
            const url = URL.createObjectURL(item.blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = item.filename || `zoning-report-${id}.pdf`;
            document.body.appendChild(a);
            a.click();
            a.remove();
            setTimeout(() => { try { URL.revokeObjectURL(url); } catch (_) {} }, 1200);
        } catch (err) {
            this.showError(`Failed to download local report: ${err?.message || err}`);
        }
    }

    /**
     * Handle image generation request
     */
    async handleImageGenerationRequest(message) {
        try {
            // Extract the image description from the message
            const imageKeywords = [
                'generate an image of', 'create an image of', 'make an image of',
                'generate a picture of', 'create a picture of', 'make a picture of',
                'generate image of', 'create image of', 'make image of',
                'generate picture of', 'create picture of', 'make picture of',
                'draw', 'paint', 'visualize', 'show me', 'picture of', 'image of'
            ];
            
            let prompt = message;
            for (const keyword of imageKeywords) {
                if (message.toLowerCase().includes(keyword)) {
                    prompt = message.substring(message.toLowerCase().indexOf(keyword) + keyword.length).trim();
                    break;
                }
            }
            
            if (!prompt) {
                prompt = message; // Use the full message if no keyword found
            }

            // Generate image using DALL-E
            const imageUrl = await this.callDALLE(prompt, '1024x1024', 'standard');
            this.removeTypingIndicator();
            
            // Add image response
            this.addImageMessage(imageUrl, prompt);
        } catch (error) {
            this.removeTypingIndicator();
            this.addMessage('error', `Image generation failed: ${error.message}`);
        }
    }

    /**
     * Call WALL-E using OpenAI Responses-first flow
     */
    async callWALLE(message, files = []) {
        try {
            // Validate configuration
            // Using proxy allows text-only without exposing apiKey
            if (!this.proxyUrl && !this.apiKey) throw new Error('WALL-E not configured (no proxy or key).');

            // OpenAI-recommended path: use Responses API for text and multimodal inputs.
            console.log('Using grounded responses API (text + files)');
            try {
                let out = await this.callGroundedResponses(message, false, files);
                // Force completion mode for address analyses: avoid "please confirm" loops.
                if (this.isAddressAnalysisRequest(message)) {
                    let attempts = 0;
                    while (attempts < 2 && this.looksLikeClarificationInsteadOfReport(out)) {
                        out = await this.callGroundedResponses(
                            `${String(message || '')}\n\nReturn the full report now with assumptions, table + visual summary + air-rights calculations. Do not ask me to confirm anything.`,
                            true,
                            files
                        );
                        attempts += 1;
                    }
                }
                return out;
            } catch (groundedErr) {
                console.warn('Grounded responses failed:', groundedErr?.message || groundedErr);
                // Do not silently drop multimodal inputs by downgrading to text-only fallback.
                if (Array.isArray(files) && files.length > 0) {
                    throw new Error(`Responses failed for file/image input: ${groundedErr?.message || 'unknown error'}`);
                }
                console.warn('Falling back to chat completions for text-only request');
                return await this.callChatCompletions(message);
            }
        } catch (error) {
            console.error('WALL-E API Error:', error);
            throw error;
        }
    }

    /**
     * Get a short friendly initial message for the current page (shown on load)
     */
    getInitialGuidelinesMessage() {
        const ctx = this.getCurrentContext();
        const hints = {
            'project-tracker': "Hey! I'm here to help with the Project Tracker. You can ask me things like: how to add a team member, where to approve your project, how the status steps work, or how to get back to your project list. Just ask!",
            'project-manager': "Hi there! I can guide you through the Project Manager—like how to respond to clients, send messages with attachments, or manage project status. What do you need?",
            'secure-chat': "Hello! I'm here to help with Connections. Ask me how to start a chat, make a call, or find a contact.",
            'calculator-app': "Hi! I can help with the Calculator or any math questions you have.",
            'invoice-app': "Hello! I can guide you through creating and managing invoices.",
            'liber-apps': "Hi! I'm WALL-E. I can help you find your way around—like opening Project Tracker, Connections, or any other app. Just ask what you'd like to do!"
        };
        return hints[ctx] || hints['liber-apps'];
    }

    /**
     * Get page-specific guidelines for WALL-E (friendly, semi-formal)
     */
    getContextGuidelines() {
        const ctx = this.getCurrentContext();
        const guidelines = {
            'project-tracker': `You are helping with the Project Tracker. The user is viewing their projects and project details.

**What's on this page:**
- My Projects grid: cards showing each project, status, description
- Project detail view: when a project is open, they see status, progress bar (Submitted → Initializing → In Progress → Review → Completed), Chat button, description, response history, team members, project library (Record In / Record Out)
- Actions vary by status: "I Approve" (initializing), "Approve (Complete)" (review), Add members by email, add files to library

**How to help:**
- Explain how to get somewhere: e.g. "Click a project card to open it" or "Use the Back button to return to the list"
- Guide through status steps: "Your project is in Review—when you're satisfied, click Approve (Complete)"
- Explain how to add members: "Type an email in the Team Members section and click Add"
- For new projects: "Submit a request from the main site to create your first project"
- Be warm and semi-formal; use "you" naturally`
            ,
            'project-manager': `You are helping with the Project Manager (admin view). The user manages project requests and responds to clients.

**What's on this page:**
- Projects list, filters by status
- Project detail: status, client info, description, admin response, files
- Send response with message and attachments to notify the client
- Approve or manage projects

**How to help:**
- Explain how to respond: "Add your message and optionally attach files, then click Send Response"
- Guide workflow: "Mark the project In Progress, then send a response so the client is notified"`
            ,
            'secure-chat': `You are helping with Connections (secure chat). The user can message contacts, start calls, or manage connections.

**How to help:**
- Explain how to start a chat: "Select a connection or create a new one"
- For calls: "Click the call icon next to a contact to start a voice or video call"`
            ,
            'calculator-app': `You are helping with the Calculator app. The user can perform math operations.

**How to help:**
- Answer calculation questions or explain how to use the calculator`
            ,
            'invoice-app': `You are helping with the Invoice Generator. The user creates and manages invoices.

**How to help:**
- Guide through creating an invoice, adding line items, or exporting`
            ,
            'file-converter': `You are helping with the File Converter. The user can convert files between formats.

**How to help:**
- Explain supported formats and how to convert files`
            ,
            'media-enhancer': `You are helping with the Media Enhancer. The user can enhance images or media.

**How to help:**
- Guide through uploading and enhancing media`
            ,
            'gallery-control': `You are helping with the Gallery Control. The user manages image galleries.

**How to help:**
- Explain how to browse, organize, or manage gallery content`
            ,
            'liber-apps': `You are helping with the Liber Apps control panel. The user sees the main dashboard with:
- Apps grid: Project Tracker, Connections (chat), Calculator, Invoices, File Converter, Media Enhancer, Gallery Control, Project Manager
- Feed, WaveConnect (audio/video), and other sections depending on view

**How to help:**
- "Click Project Tracker to see your projects" or "Open Connections to chat"
- "Use the search to find an app quickly"
- Guide them to the app that matches what they want to do`
        };
        return guidelines[ctx] || guidelines['liber-apps'];
    }

    /**
     * Build system prompt with base personality and page-specific guidelines
     */
    buildSystemPrompt() {
        const base = `You are WALL-E, a friendly, straightforward, calm expert assistant for LIBER. Be concise, practical, and natural. Keep continuity across turns: remember recent user intents, constraints, and unresolved asks. Prefer direct helpful answers over generic questions. If clarification is needed, ask one focused question and propose a best-effort assumption path in parallel.`;
        const guidelines = this.getContextGuidelines();
        const appKnowledge = `LIBER Control Panel quick map:
- Apps icon (\`fa-th\`) => Apps grid
- Personal Space (\`fa-id-badge\`) => personal profile and posting
- Wall (\`fa-stream\`) => feed posts and suggestions
- WaveConnect (\`fa-music\`) => media platform
- Profile (\`fa-user\`) => my profile
- Admin only: User Management (\`fa-users\`), Settings (\`fa-cog\`)
- WaveConnect main tabs: Audio (\`fa-music\`), Video (\`fa-video\`), Pictures (\`fa-image\`)
- WaveConnect secondary nav: Home (\`fa-house\`), Search (\`fa-magnifying-glass\`), Upload (\`fa-plus\`), Library (\`fa-user\`), Studio (\`fa-sliders\`)`;
        const regulatoryGrounding = `When users ask about zoning/codes/filing/compliance, prioritize current official sources and cite them inline with markdown links:
- NYC Zoning Resolution: https://zoningresolution.planning.nyc.gov/
- NYC ZoLa zoning map: https://zola.planning.nyc.gov/about#9.72/40.7125/-73.733
- NYC Buildings codes page: https://www.nyc.gov/site/buildings/codes/nyc-code.page
Rules:
1) Do not invent section numbers, FAR values, overlays, or filing requirements.
2) If exact parcel analysis is requested, ask for borough/block/lot or address and any district/overlay shown in ZoLa.
3) For exact zoning floor area calculations, show formula and assumptions, and clearly label values as verified vs assumed.
4) Prefer latest adopted text and note effective date if source provides it.
5) For legal/safety-critical guidance, recommend licensed professional verification.
6) For non-NYC regions, first confirm jurisdiction and code edition, then cite the relevant official source before giving compliance guidance.`;
        const liveContext = this.getLiveUiContextSnapshot();
        return `${base}\n\n${appKnowledge}\n\n${regulatoryGrounding}\n\n**Current page context:**\n${guidelines}\n\n**Live UI context snapshot:**\n${liveContext}`;
    }

    getLiveUiContextSnapshot() {
        try {
            const lines = [];
            const path = String(window.location?.pathname || '/');
            lines.push(`Path: ${path}`);
            const shellOpen = !!document.body?.classList?.contains('app-shell-open');
            lines.push(`App shell open: ${shellOpen ? 'yes' : 'no'}`);
            const frame = document.getElementById('app-shell-frame');
            if (shellOpen && frame) {
                const src = String(frame.getAttribute('src') || frame.src || '').trim();
                if (src) lines.push(`Current app iframe src: ${src}`);
            }
            const activeDesktop = document.querySelector('.nav-btn.active[data-section]');
            if (activeDesktop) {
                lines.push(`Active main section: ${String(activeDesktop.getAttribute('data-section') || '').trim()}`);
            }
            const activeWaveMain = document.querySelector('.waveconnect-tabs .btn.active');
            if (activeWaveMain) {
                lines.push(`WaveConnect main tab: ${String(activeWaveMain.textContent || '').trim()}`);
            }
            const activeWaveSub = document.querySelector('#wave-subnav .btn.active[data-wave-subtab]');
            if (activeWaveSub) {
                lines.push(`WaveConnect subtab: ${String(activeWaveSub.getAttribute('data-wave-subtab') || '').trim()}`);
            }
            return lines.join('\n');
        } catch (_) {
            return 'Unavailable';
        }
    }

    getRecentConversationWindow(limit = 10) {
        try {
            const rows = Array.isArray(this.chatHistory) ? this.chatHistory : [];
            const filtered = rows
                .filter((m) => m && (m.role === 'user' || m.role === 'assistant'))
                .map((m) => ({
                    role: m.role === 'assistant' ? 'assistant' : 'user',
                    content: String(m.content || '').trim()
                }))
                .filter((m) => m.content.length > 0);
            if (!filtered.length) return [];
            return filtered.slice(-Math.max(1, Number(limit || 10)));
        } catch (_) {
            return [];
        }
    }

    isRegulatoryOrCodeQuery(message) {
        const text = String(message || '').toLowerCase();
        if (!text) return false;
        const keys = [
            'zoning', 'zola', 'far', 'floor area', 'setback', 'overlay',
            'nyc code', 'building code', 'fire code', 'dob', 'permit',
            'filing', 'occupancy', 'egress', 'sprinkler', 'ceqr',
            'special district', 'lot coverage', 'height factor'
        ];
        return keys.some((k) => text.includes(k));
    }

    extractAssistantTextFromResponsesPayload(data) {
        try {
            const fileIds = new Set();
            if (typeof data?.output_text === 'string' && data.output_text.trim()) {
                return data.output_text.trim();
            }
            if (Array.isArray(data?.output_text)) {
                const joined = data.output_text.map((x) => String((x?.text ?? x ?? '')).trim()).filter(Boolean).join('\n\n');
                if (joined) return joined;
            }
            const output = Array.isArray(data?.output) ? data.output : [];
            const parts = [];
            output.forEach((item) => {
                const direct = String(item?.text?.value ?? item?.text ?? item?.output_text?.value ?? item?.output_text ?? '').trim();
                if (direct) parts.push(direct);
                const content = Array.isArray(item?.content) ? item.content : [];
                content.forEach((c) => {
                    const rawText = c?.text?.value ?? c?.text ?? c?.output_text?.value ?? c?.output_text ?? '';
                    const t = String(rawText || '').trim();
                    if (t) parts.push(t);
                    const refusal = String(c?.refusal?.value ?? c?.refusal ?? '').trim();
                    if (refusal) parts.push(refusal);
                    const directFileId = String(c?.file_id || '').trim();
                    if (directFileId) fileIds.add(directFileId);
                    const anns = Array.isArray(c?.annotations) ? c.annotations : [];
                    anns.forEach((a) => {
                        const ids = [
                            a?.file_id,
                            a?.file_path?.file_id,
                            a?.file_citation?.file_id
                        ].map((x) => String(x || '').trim()).filter(Boolean);
                        ids.forEach((id) => fileIds.add(id));
                    });
                });
            });
            if (parts.length) {
                const body = parts.join('\n\n');
                if (!fileIds.size) return body;
                const filesBlock = Array.from(fileIds).map((id) => `[Generated file: ${id}]`).join('\n');
                return `${body}\n\nGenerated files:\n${filesBlock}`;
            }
            const fallback = String(data?.choices?.[0]?.message?.content || '').trim();
            if (fallback) return fallback;
            const errType = String(data?.error?.type || '').trim();
            const errMsg = String(data?.error?.message || '').trim();
            if (errType || errMsg) return `Model error${errType ? ` (${errType})` : ''}: ${errMsg || 'unknown error'}`;
            return '';
        } catch (_) {
            return '';
        }
    }

    async fileToDataUrl(file) {
        return await new Promise((resolve, reject) => {
            try {
                const reader = new FileReader();
                reader.onload = () => resolve(String(reader.result || ''));
                reader.onerror = () => reject(new Error(`Failed to read file: ${String(file?.name || 'unknown')}`));
                reader.readAsDataURL(file);
            } catch (err) {
                reject(err);
            }
        });
    }

    async downloadGeneratedFile(fileId) {
        try {
            const id = String(fileId || '').trim();
            if (!id) return;
            const resp = await this.openaiFetch(`/v1/files/${encodeURIComponent(id)}/content`, {
                method: 'GET',
                json: false,
                timeoutMs: 45000
            });
            if (!resp.ok) {
                const msg = await resp.text().catch(() => '');
                throw new Error(msg || `Unable to download file (${resp.status})`);
            }
            const blob = await resp.blob();
            const cd = String(resp.headers.get('content-disposition') || '');
            const nameMatch = /filename\*?=(?:UTF-8'')?"?([^";]+)"?/i.exec(cd);
            const filename = decodeURIComponent(String(nameMatch?.[1] || '').trim() || `wall-e-generated-${id}.bin`);
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = filename;
            document.body.appendChild(a);
            a.click();
            a.remove();
            setTimeout(() => { try { URL.revokeObjectURL(url); } catch (_) {} }, 1000);
        } catch (err) {
            this.showError(`Generated file download failed: ${err?.message || err}`);
        }
    }

    async fileToText(file, maxChars = 160000) {
        try {
            const text = await file.text();
            const safe = String(text || '');
            if (safe.length <= maxChars) return safe;
            return `${safe.slice(0, maxChars)}\n\n[Truncated to ${maxChars} chars]`;
        } catch (_) {
            return '';
        }
    }

    async uploadFileToOpenAI(file) {
        try {
            const form = new FormData();
            form.append('purpose', 'user_data');
            form.append('file', file, String(file?.name || 'upload.bin'));
            let resp = await this.openaiFetch('/v1/files', {
                method: 'POST',
                json: false,
                timeoutMs: 60000,
                body: form
            });
            if (!resp.ok) {
                // Compatibility fallback with legacy purpose name.
                const form2 = new FormData();
                form2.append('purpose', 'assistants');
                form2.append('file', file, String(file?.name || 'upload.bin'));
                resp = await this.openaiFetch('/v1/files', {
                    method: 'POST',
                    json: false,
                    timeoutMs: 60000,
                    body: form2
                });
            }
            if (!resp.ok) {
                const msg = await resp.text().catch(() => '');
                throw new Error(msg || `File upload failed (${resp.status})`);
            }
            const data = await resp.json();
            const id = String(data?.id || '').trim();
            if (!id) throw new Error('OpenAI file id missing');
            return id;
        } catch (err) {
            console.warn('uploadFileToOpenAI failed:', err);
            return '';
        }
    }

    async buildResponsesUserContent(message, files = []) {
        const content = [];
        const userText = String(message || '').trim();
        if (userText) content.push({ type: 'input_text', text: userText });

        if (!Array.isArray(files) || files.length === 0) return content;

        const notes = [];
        for (const file of files) {
            if (!file || typeof file !== 'object') continue;
            const name = String(file.name || 'file');
            const type = String(file.type || '').toLowerCase();
            const size = Number(file.size || 0);

            // Keep request payloads predictable.
            if (size > 8 * 1024 * 1024) {
                notes.push(`Skipped large file "${name}" (${Math.round(size / (1024 * 1024))} MB). Please upload a smaller file.`);
                continue;
            }

            if (type.startsWith('image/')) {
                try {
                    const dataUrl = await this.fileToDataUrl(file);
                    if (dataUrl) {
                        content.push({ type: 'input_image', image_url: dataUrl });
                        notes.push(`Included image: "${name}".`);
                        continue;
                    }
                } catch (_) {}
                notes.push(`Could not attach image "${name}".`);
                continue;
            }

            if (type === 'application/pdf') {
                const fileId = await this.uploadFileToOpenAI(file);
                if (fileId) {
                    content.push({ type: 'input_file', file_id: fileId });
                    notes.push(`Included PDF as reference: "${name}".`);
                    continue;
                }
                notes.push(`Could not upload PDF "${name}" for model reading.`);
                continue;
            }

            const isTextLike = type.startsWith('text/') || type === 'application/json' || type === 'application/xml' || type === 'text/csv';
            if (isTextLike) {
                const textBody = await this.fileToText(file);
                if (textBody) {
                    content.push({
                        type: 'input_text',
                        text: `File "${name}" (${type || 'text/plain'}) content:\n\n${textBody}`
                    });
                    continue;
                }
            }

            notes.push(`File "${name}" (${type || 'unknown'}) attached as metadata only. If you need deep analysis, provide text content or image/PDF extract.`);
        }

        if (notes.length > 0) {
            content.push({ type: 'input_text', text: `Attachment notes:\n- ${notes.join('\n- ')}` });
        }
        return content;
    }

    buildResponsesHistoryInput(currentMessage = '') {
        const current = String(currentMessage || '').trim();
        const history = this.getRecentConversationWindow(10);
        if (!history.length) return [];
        // Avoid duplicating the message just typed in this request.
        const deduped = history.filter((m, idx) => !(idx === history.length - 1 && m.role === 'user' && m.content === current));
        return deduped.map((m) => ({
            role: m.role,
            content: [{
                type: m.role === 'assistant' ? 'output_text' : 'input_text',
                text: m.content.slice(0, 3000)
            }]
        }));
    }

    buildChatCompletionsHistory(currentMessage = '') {
        const current = String(currentMessage || '').trim();
        const history = this.getRecentConversationWindow(10);
        if (!history.length) return [];
        const deduped = history.filter((m, idx) => !(idx === history.length - 1 && m.role === 'user' && m.content === current));
        return deduped.map((m) => ({
            role: m.role,
            content: m.content.slice(0, 3000)
        }));
    }

    async callGroundedResponses(message, forceRegulatory = false, files = []) {
        const systemPrompt = this.buildSystemPrompt();
        const requireWeb = forceRegulatory || this.isRegulatoryOrCodeQuery(message);
        const wantsAddressReport = this.isAddressAnalysisRequest(message);
        let instructions = requireWeb
            ? `${systemPrompt}\n\nFor this request, use web search grounding and include source links for factual claims. Keep structure concise and practical.`
            : systemPrompt;
        if (wantsAddressReport) {
            instructions += `\n\nGenerate a comprehensive zoning analysis report for the provided address. Include:
1) Property identification and assumptions used
2) Current zoning district/overlays and use framework
3) FAR/buildable area analysis with formulas and explicit assumptions
4) Bulk controls summary (height, setbacks, lot coverage, yards, parking/loading if applicable)
5) Air-rights analysis:
   - Available development rights on lot (sq ft) = max zoning floor area - existing built floor area
   - Potential added floor area from purchased air rights (sq ft) and resulting total achievable floor area
   - Practical transferability caveats and constraints
6) Maximum buildable options as scenarios (base / moderate / aggressive) with constraints
7) Constraints/risks and filing considerations
8) Practical next steps and data still needed
9) A "References" section with source links inline and at the end.
Formatting requirements:
- Use clean section headers
- Include at least one table for scenario comparison
- Include a simple visual summary (ASCII bar chart or score bars) for buildable options
Behavior requirements:
- Do NOT ask for confirmation first; proceed with best-match interpretation and clearly state assumptions
- If address ambiguity exists, continue with best likely parcel and list alternatives in assumptions.
Do not omit references.`;
        }
        const userContent = await this.buildResponsesUserContent(message, files);
        const payload = {
            model: this.responsesModel || 'gpt-5-mini',
            input: [
                { role: 'system', content: [{ type: 'input_text', text: instructions }] },
                ...this.buildResponsesHistoryInput(message),
                { role: 'user', content: userContent.length ? userContent : [{ type: 'input_text', text: String(message || '') }] }
            ],
            max_output_tokens: 1400
        };
        if (requireWeb) {
            payload.tools = [{ type: 'web_search_preview', search_context_size: 'medium' }];
        }
        const response = await this.openaiFetch('/v1/responses', {
            method: 'POST',
            beta: null,
            timeoutMs: 35000,
            body: JSON.stringify(payload)
        });
        if (!response.ok) {
            const errorData = await response.text();
            throw new Error(`Responses API failed: ${response.status} - ${errorData || response.statusText}`);
        }
        const data = await response.json();
        let out = this.extractAssistantTextFromResponsesPayload(data);
        if (String(out || '').trim()) return out;

        // Some responses finish asynchronously; try fetching by response id once.
        const respId = String(data?.id || '').trim();
        if (respId) {
            try {
                await new Promise((r) => setTimeout(r, 900));
                const poll = await this.openaiFetch(`/v1/responses/${encodeURIComponent(respId)}`, {
                    method: 'GET',
                    json: false,
                    timeoutMs: 15000
                });
                if (poll.ok) {
                    const polledData = await poll.json();
                    out = this.extractAssistantTextFromResponsesPayload(polledData);
                    if (String(out || '').trim()) return out;
                }
            } catch (_) {}
        }

        // If web-grounded call returned empty, retry once without tools to avoid blank replies.
        if (requireWeb) {
            const retryPayload = {
                model: this.responsesModel || 'gpt-5-mini',
                input: [
                    { role: 'system', content: [{ type: 'input_text', text: `${systemPrompt}\n\nProvide the best possible answer now. If external lookup is unavailable, clearly state assumptions and required verification steps.` }] },
                    { role: 'user', content: userContent.length ? userContent : [{ type: 'input_text', text: String(message || '') }] }
                ],
                max_output_tokens: 1400
            };
            const retry = await this.openaiFetch('/v1/responses', {
                method: 'POST',
                beta: null,
                timeoutMs: 22000,
                body: JSON.stringify(retryPayload)
            });
            if (retry.ok) {
                const retryData = await retry.json();
                out = this.extractAssistantTextFromResponsesPayload(retryData);
                if (String(out || '').trim()) return out;
            }
        }

        throw new Error('Empty model output from Responses API');
    }

    /**
     * Use Chat Completions API for text-only messages
     */
    async callChatCompletions(message) {
        try {
            console.log('Using chat completions API fallback (text-only)');
            const systemPrompt = this.buildSystemPrompt();
            const historyMessages = this.buildChatCompletionsHistory(message);
            
            const response = await this.openaiFetch('/v1/chat/completions', {
                method: 'POST',
                beta: null,
                body: JSON.stringify({
                    model: this.chatFallbackModel || 'gpt-5-mini',
                    messages: [
                        {
                            role: 'system',
                            content: systemPrompt
                        },
                        ...historyMessages,
                        {
                            role: 'user',
                            content: message
                        }
                    ],
                    max_completion_tokens: 1000
                })
            });

            if (!response.ok) {
                const errorData = await response.text();
                console.error('Chat completions failed:', response.status, errorData);
                throw new Error(`Chat completions failed: ${response.status} - ${response.statusText}`);
            }

            const data = await response.json();
            console.log('Chat completions response received');
            
            return data.choices[0].message.content;
        } catch (error) {
            console.error('Chat completions error:', error);
            throw error;
        }
    }

    /**
     * Create a new thread
     */
    async createThread() {
        try {
            const response = await this.openaiFetch('/v1/threads', {
                method: 'POST',
                beta: 'assistants=v2'
            });

            if (!response.ok) {
                const errorData = await response.text();
                console.error('Thread creation failed:', response.status, errorData);
                throw new Error(`Failed to create thread: ${response.status} - ${response.statusText}`);
            }

            const data = await response.json();
            return data.id;
        } catch (error) {
            console.error('Thread creation error:', error);
            throw new Error(`Thread creation failed: ${error.message}`);
        }
    }

    /**
     * Add message to thread - HANDLES ALL POSSIBLE INPUT COMBINATIONS
     */
    async addMessageToThread(message, files = []) {
        try {
            console.log('=== ADDING MESSAGE TO THREAD ===');
            console.log('Message: [redacted]');
            console.log('Files:', files);
            console.log('Files length:', files.length);
            
            // SCENARIO 1: No message and no files - INVALID
            if (!message && (!files || files.length === 0)) {
                throw new Error('Cannot send empty message with no files');
            }
            
            // SCENARIO 2: Text-only message (no files)
            if (message && (!files || files.length === 0)) {
                console.log('SCENARIO 2: Text-only message');
                
                console.log('Sending text-only content as string: [redacted]');
                
                const response = await this.openaiFetch(`/v1/threads/${this.threadId}/messages`, {
                    method: 'POST',
                    beta: 'assistants=v2',
                    body: JSON.stringify({
                        role: 'user',
                        content: message.trim()
                    })
                });

                if (!response.ok) {
                    const errorData = await response.text();
                    console.error('Failed to add text-only message:', response.status, errorData);
                    throw new Error(`Failed to add message to thread: ${response.status} - ${response.statusText}`);
                }

                const result = await response.json();
                console.log('Text-only message added successfully:', result);
                return result;
            }
            
            // SCENARIO 3: Files only (no message)
            if (!message && files && files.length > 0) {
                console.log('SCENARIO 3: Files only');
                return await this.handleFilesOnlyMessage(files);
            }
            
            // SCENARIO 4: Message with files
            if (message && files && files.length > 0) {
                console.log('SCENARIO 4: Message with files');
                return await this.handleMessageWithFiles(message, files);
            }
            
            // Fallback - should never reach here
            throw new Error('Invalid input combination');
            
        } catch (error) {
            console.error('Error adding message to thread:', error);
            throw error;
        }
    }

    /**
     * Handle files-only message (no text)
     */
    async handleFilesOnlyMessage(files) {
        console.log('Processing files-only message');
        
        // Upload all files first
        const uploadedFiles = await this.uploadAllFiles(files);
        
        if (uploadedFiles.length === 0) {
            throw new Error('No files were successfully uploaded');
        }
        
        // Separate files by type
        const { imageFiles, otherFiles } = this.separateFilesByType(uploadedFiles);
        
        // Create content array
        const content = [];
        
        // Add image files directly to message content
        for (const { fileId } of imageFiles) {
            content.push({
                type: 'image_file',
                image_file: {
                    file_id: fileId
                }
            });
        }
        
        // For non-image files, attach them to the assistant first
        if (otherFiles.length > 0) {
            const otherFileIds = otherFiles.map(({ fileId }) => fileId);
            await this.attachFilesToAssistant(otherFileIds);
        }
        
        // If we have no content (shouldn't happen), add a default text
        if (content.length === 0) {
            console.log('No files uploaded, sending default text message');
            
            const response = await this.openaiFetch(`/v1/threads/${this.threadId}/messages`, {
                method: 'POST',
                beta: 'assistants=v2',
                body: JSON.stringify({
                    role: 'user',
                    content: 'Please analyze the attached files.'
                })
            });

            if (!response.ok) {
                const errorData = await response.text();
                console.error('Failed to add default message:', response.status, errorData);
                throw new Error(`Failed to add message to thread: ${response.status} - ${response.statusText}`);
            }

            const result = await response.json();
            console.log('Default message added successfully:', result);
            return result;
        }
        
        console.log('Sending files-only content:', JSON.stringify(content, null, 2));
        
        const response = await this.openaiFetch(`/v1/threads/${this.threadId}/messages`, {
            method: 'POST',
            beta: 'assistants=v2',
            body: JSON.stringify({
                role: 'user',
                content: content
            })
        });

        if (!response.ok) {
            const errorData = await response.text();
            console.error('Failed to add files-only message:', response.status, errorData);
            throw new Error(`Failed to add message to thread: ${response.status} - ${response.statusText}`);
        }

        const result = await response.json();
        console.log('Files-only message added successfully:', result);
        return result;
    }

    /**
     * Handle message with files
     */
    async handleMessageWithFiles(message, files) {
        console.log('Processing message with files');
        
        // Upload all files first
        const uploadedFiles = await this.uploadAllFiles(files);
        
        if (uploadedFiles.length === 0) {
            console.warn('No files were successfully uploaded, falling back to text-only');
            // Fall back to text-only message
            console.log('Fallback to text-only message');
            
            const response = await this.openaiFetch(`/v1/threads/${this.threadId}/messages`, {
                method: 'POST',
                beta: 'assistants=v2',
                body: JSON.stringify({
                    role: 'user',
                    content: message.trim()
                })
            });

            if (!response.ok) {
                const errorData = await response.text();
                console.error('Failed to add fallback message:', response.status, errorData);
                throw new Error(`Failed to add message to thread: ${response.status} - ${response.statusText}`);
            }

            const result = await response.json();
            console.log('Fallback message added successfully:', result);
            return result;
        }
        
        // Separate files by type
        const { imageFiles, otherFiles } = this.separateFilesByType(uploadedFiles);
        
        // Create content array starting with text
        const content = [
            {
                type: 'text',
                text: message.trim()
            }
        ];
        
        // Add image files directly to message content
        for (const { fileId } of imageFiles) {
            content.push({
                type: 'image_file',
                image_file: {
                    file_id: fileId
                }
            });
        }
        
        // For non-image files, attach them to the assistant first
        if (otherFiles.length > 0) {
            const otherFileIds = otherFiles.map(({ fileId }) => fileId);
            await this.attachFilesToAssistant(otherFileIds);
        }
        
        console.log('Sending message with files content:', JSON.stringify(content, null, 2));
        
        const response = await this.openaiFetch(`/v1/threads/${this.threadId}/messages`, {
            method: 'POST',
            beta: 'assistants=v2',
            body: JSON.stringify({
                role: 'user',
                content: content
            })
        });

        if (!response.ok) {
            const errorData = await response.text();
            console.error('Failed to add message with files:', response.status, errorData);
            throw new Error(`Failed to add message to thread: ${response.status} - ${response.statusText}`);
        }

        const result = await response.json();
        console.log('Message with files added successfully:', result);
        return result;
    }

    /**
     * Upload all files and return array of { file, fileId }
     */
    async uploadAllFiles(files) {
        const uploadedFiles = [];
        
        for (const file of files) {
            try {
                console.log('Uploading file:', file.name, file.type, file.size);
                const fileId = await this.uploadFile(file);
                uploadedFiles.push({ file, fileId });
                console.log('File uploaded successfully:', file.name, '->', fileId);
            } catch (fileError) {
                console.error('Failed to upload file:', fileError);
                // Continue with other files
            }
        }
        
        return uploadedFiles;
    }

    /**
     * Separate files by type (images vs others)
     */
    separateFilesByType(uploadedFiles) {
        const imageFiles = [];
        const otherFiles = [];
        
        for (const { file, fileId } of uploadedFiles) {
            if (file.type && file.type.startsWith('image/')) {
                imageFiles.push({ file, fileId });
            } else {
                otherFiles.push({ file, fileId });
            }
        }
        
        console.log('Separated files - Images:', imageFiles.length, 'Others:', otherFiles.length);
        return { imageFiles, otherFiles };
    }

    /**
     * Upload file to OpenAI
     */
    async uploadFile(file) {
        try {
            // Create FormData for file upload
            const formData = new FormData();
            formData.append('file', file);
            formData.append('purpose', 'assistants');

            const response = await this.openaiFetch('/v1/files', {
                method: 'POST',
                json: false,
                body: formData
            });

            if (!response.ok) {
                const errorData = await response.text();
                console.error('File upload failed:', response.status, errorData);
                throw new Error(`File upload failed: ${response.status} - ${response.statusText}`);
            }

            const data = await response.json();
            console.log(`File uploaded successfully: ${file.name} -> ${data.id}`);
            return data.id;
        } catch (error) {
            console.error('Error uploading file:', error);
            throw error;
        }
    }

    /**
     * Attach files to the assistant
     */
    async attachFilesToAssistant(fileIds) {
        try {
            console.log('Attaching files to assistant:', fileIds);
            
            // First, get the current assistant configuration
            const getResponse = await this.openaiFetch(`/v1/assistants/${this.assistantId}`, {
                method: 'GET',
                beta: 'assistants=v2',
                json: false
            });

            if (!getResponse.ok) {
                const errorData = await getResponse.text();
                console.error('Failed to get assistant config:', getResponse.status, errorData);
                throw new Error(`Failed to get assistant config: ${getResponse.status} - ${getResponse.statusText}`);
            }

            const assistant = await getResponse.json();
            console.log('Current assistant config:', assistant);
            
            // Get existing file IDs
            const existingFileIds = assistant.file_ids || [];
            console.log('Existing file IDs:', existingFileIds);
            
            // Merge with new file IDs, avoiding duplicates
            const allFileIds = [...new Set([...existingFileIds, ...fileIds])];
            console.log('All file IDs (merged):', allFileIds);
            
            // Update the assistant with all file IDs
            const updateResponse = await this.openaiFetch(`/v1/assistants/${this.assistantId}`, {
                method: 'POST',
                beta: 'assistants=v2',
                body: JSON.stringify({
                    file_ids: allFileIds
                })
            });

            if (!updateResponse.ok) {
                const errorData = await updateResponse.text();
                console.error('Failed to update assistant with files:', updateResponse.status, errorData);
                throw new Error(`Failed to update assistant with files: ${updateResponse.status} - ${updateResponse.statusText}`);
            }

            const data = await updateResponse.json();
            console.log('Assistant updated with files successfully:', data);
            return data;
        } catch (error) {
            console.error('Error attaching files to assistant:', error);
            throw error;
        }
    }

    /**
     * Run the assistant
     */
    async runAssistant() {
        try {
            console.log('Starting assistant run...');
            const response = await this.openaiFetch(`/v1/threads/${this.threadId}/runs`, {
                method: 'POST',
                beta: 'assistants=v2',
                body: JSON.stringify({
                    assistant_id: this.assistantId
                })
            });

            if (!response.ok) {
                const errorData = await response.text();
                console.error('Assistant run failed:', response.status, errorData);
                throw new Error(`Failed to run assistant: ${response.status} - ${response.statusText}`);
            }

            const data = await response.json();
            console.log('Assistant run started successfully:', data);
            return data.id;
        } catch (error) {
            console.error('Error in runAssistant:', error);
            throw error;
        }
    }

    /**
     * Wait for assistant response
     */
    async waitForResponse(runId) {
        let attempts = 0;
        const maxAttempts = 30; // 30 seconds timeout

        console.log(`Waiting for assistant response (runId: ${runId})...`);

        while (attempts < maxAttempts) {
            try {
                const response = await this.openaiFetch(`/v1/threads/${this.threadId}/runs/${runId}`, {
                    beta: 'assistants=v2',
                    json: false
                });

                if (!response.ok) {
                    const errorData = await response.text();
                    console.error('Failed to check run status:', response.status, errorData);
                    throw new Error(`Failed to check run status: ${response.status} - ${response.statusText}`);
                }

                const data = await response.json();
                console.log(`Run status (attempt ${attempts + 1}):`, data.status);

                if (data.status === 'completed') {
                    console.log('Assistant run completed successfully');
                    // Get the response message
                    return await this.getLastMessage();
                } else if (data.status === 'failed') {
                    console.error('Assistant run failed:', data);
                    throw new Error(`Assistant run failed: ${data.last_error?.message || 'Unknown error'}`);
                } else if (data.status === 'requires_action') {
                    console.error('Assistant requires action:', data);
                    throw new Error(`Assistant requires action: ${data.required_action?.type || 'Unknown action'}`);
                } else if (data.status === 'cancelled') {
                    console.error('Assistant run was cancelled');
                    throw new Error('Assistant run was cancelled');
                } else if (data.status === 'expired') {
                    console.error('Assistant run expired');
                    throw new Error('Assistant run expired');
                }

                // Wait 1 second before checking again
                await new Promise(resolve => setTimeout(resolve, 1000));
                attempts++;
            } catch (error) {
                console.error(`Error checking run status (attempt ${attempts + 1}):`, error);
                throw error;
            }
        }

        console.error('Assistant run timed out after', maxAttempts, 'attempts');
        throw new Error('Response timeout');
    }

    /**
     * Get the last message from the thread
     */
    async getLastMessage() {
        const response = await this.openaiFetch(`/v1/threads/${this.threadId}/messages`, {
            beta: 'assistants=v2',
            json: false
        });

        if (!response.ok) {
            throw new Error('Failed to get messages');
        }

        const data = await response.json();
        const messages = data.data;
        
        if (messages.length > 0) {
            const lastMessage = messages[0];
            if (lastMessage.content && lastMessage.content.length > 0) {
                const content = lastMessage.content[0];
                
                // Handle different content types
                if (content.type === 'text') {
                    return content.text.value;
                } else if (content.type === 'image_file') {
                    // Handle image file attachments
                    const fileId = content.image_file.file_id;
                    return `[Image attachment: ${fileId}]`;
                } else if (content.type === 'file') {
                    // Handle file attachments
                    const fileId = content.file.file_id;
                    return `[File attachment: ${fileId}]`;
                }
            }
        }

        return 'No response received';
    }

    /**
     * Get current context (active app in shell takes precedence over main page)
     */
    getCurrentContext() {
        const shell = document.getElementById('app-shell');
        const frame = document.getElementById('app-shell-frame');
        if (shell && frame && document.body.classList.contains('app-shell-open')) {
            try {
                const src = String(frame.src || frame.getAttribute('src') || '').toLowerCase();
                if (src && src !== 'about:blank') {
                    if (src.includes('/apps/project-tracker/')) return 'project-tracker';
                    if (src.includes('/apps/project-manager/')) return 'project-manager';
                    if (src.includes('/apps/secure-chat/')) return 'secure-chat';
                    if (src.includes('/apps/calculator/')) return 'calculator-app';
                    if (src.includes('/apps/invoices-app/')) return 'invoice-app';
                    if (src.includes('/apps/file-converter/')) return 'file-converter';
                    if (src.includes('/apps/media-enhancer/')) return 'media-enhancer';
                    if (src.includes('/apps/gallery-control/')) return 'gallery-control';
                }
            } catch (_) {}
        }
        const path = window.location.pathname.toLowerCase();
        if (path.includes('/apps/calculator/')) return 'calculator-app';
        if (path.includes('/apps/invoices-app/')) return 'invoice-app';
        if (path.includes('/apps/project-tracker/')) return 'project-tracker';
        if (path.includes('/apps/project-manager/')) return 'project-manager';
        if (path.includes('/control-panel') || path.includes('/liber-apps')) return 'liber-apps';
        return 'liber-apps';
    }

    /**
     * Get app information
     */
    getAppInfo() {
        const context = this.getCurrentContext();
        const appMap = {
            'calculator-app': { name: 'Calculator', description: 'Mathematical operations and calculations' },
            'invoice-app': { name: 'Invoice Generator', description: 'Create and manage invoices' },
            'liber-apps': { name: 'Control Panel', description: 'Main dashboard for app management' }
        };
        return appMap[context] || { name: 'Unknown App', description: 'Unknown application' };
    }

    /**
     * Add message to chat
     */
    addMessage(role, content, files = []) {
        const messagesContainer = document.getElementById('chatgpt-messages');
        if (!messagesContainer) return;

        const messageDiv = document.createElement('div');
        messageDiv.className = `chatgpt-message ${role}`;

        let messageHTML = '';
        
        if (role === 'user') {
            messageHTML = `
                <div class="message-content">
                    <div class="message-text">${this.escapeHtml(content)}</div>
                    ${this.renderFileAttachments(files)}
                </div>
                <div class="message-avatar">
                    <i class="fas fa-user"></i>
                </div>
            `;
        } else if (role === 'assistant') {
            // Check if content contains file attachments
            const hasFileAttachments = content.includes('[Image attachment:') || content.includes('[File attachment:');
            
            messageHTML = `
                <div class="message-avatar">
                    <img src="images/wall_e.svg" alt="WALL-E" class="avatar-icon">
                </div>
                <div class="message-content">
                    <div class="message-text">
                        ${this.formatResponse(content)}
                        ${hasFileAttachments ? '<p><em>📎 Assistant has attached files to this response</em></p>' : ''}
                    </div>
                </div>
            `;
        } else if (role === 'error') {
            messageHTML = `
                <div class="message-avatar">
                    <i class="fas fa-exclamation-triangle"></i>
                </div>
                <div class="message-content">
                    <div class="message-text error">${this.escapeHtml(content)}</div>
                </div>
            `;
        }

        messageDiv.innerHTML = messageHTML;
        messagesContainer.appendChild(messageDiv);
        messageDiv.querySelectorAll('.wall-e-file-download').forEach((btn) => {
            btn.addEventListener('click', (e) => {
                e.preventDefault();
                const fileId = String(btn.getAttribute('data-file-id') || '').trim();
                if (!fileId) return;
                this.downloadGeneratedFile(fileId);
            });
        });
        messageDiv.querySelectorAll('.wall-e-local-report-download').forEach((btn) => {
            btn.addEventListener('click', (e) => {
                e.preventDefault();
                const reportId = String(btn.getAttribute('data-local-report-id') || '').trim();
                if (!reportId) return;
                this.downloadLocalReport(reportId);
            });
        });

        // Scroll to bottom
        messagesContainer.scrollTop = messagesContainer.scrollHeight;

        // Add to history
        this.chatHistory.push({ role, content, files, timestamp: new Date() });

        // Limit history to last 50 messages
        if (this.chatHistory.length > this.maxHistoryItems) {
            this.chatHistory = this.chatHistory.slice(-this.maxHistoryItems);
        }

        // Save history to localStorage
        this.saveChatHistory();
    }

    /**
     * Render file attachments in message
     */
    renderFileAttachments(files) {
        if (!files || files.length === 0) return '';

        return `
            <div class="file-attachments">
                ${files.map(file => {
                    // Safety check for file object
                    if (!file || typeof file !== 'object') {
                        return '';
                    }
                    
                    const fileName = file.name || 'Unknown file';
                    const fileType = file.type || '';
                    const fileSize = file.size || 0;
                    
                    return `
                        <div class="file-attachment">
                            <i class="fas ${this.getFileIcon(fileType)}"></i>
                            <span>${fileName}</span>
                            <small>(${this.formatFileSize(fileSize)})</small>
                        </div>
                    `;
                }).join('')}
            </div>
        `;
    }

    /**
     * Get file icon based on type
     */
    getFileIcon(type) {
        // Handle undefined, null, or empty type
        if (!type || typeof type !== 'string') {
            return 'fa-file';
        }
        
        if (type.startsWith('image/')) return 'fa-image';
        if (type === 'application/pdf') return 'fa-file-pdf';
        if (type.startsWith('text/')) return 'fa-file-text';
        if (type === 'application/json') return 'fa-file-code';
        if (type === 'application/xml') return 'fa-file-code';
        if (type === 'text/csv') return 'fa-file-csv';
        return 'fa-file';
    }

    /**
     * Format file size
     */
    formatFileSize(bytes) {
        if (bytes === 0) return '0 Bytes';
        const k = 1024;
        const sizes = ['Bytes', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }

    /**
     * Handle file upload
     */
    handleFileUpload(event) {
        const files = Array.from(event.target.files);
        this.processFiles(files);
        event.target.value = ''; // Clear input
    }

    /**
     * Check if file type is supported
     */
    isFileTypeSupported(type) {
        return this.supportedFileTypes.some(supportedType => {
            if (supportedType.endsWith('/*')) {
                return type.startsWith(supportedType.slice(0, -1));
            }
            return type === supportedType;
        });
    }

    /**
     * Update file upload display
     */
    updateFileUploadDisplay() {
        const uploadArea = document.getElementById('chatgpt-file-upload');
        if (!uploadArea) return;

        const existingFiles = uploadArea.querySelectorAll('.uploaded-file');
        existingFiles.forEach(file => file.remove());

        this.fileUploads.forEach((file, index) => {
            // Safety check for file object
            if (!file || typeof file !== 'object') {
                return;
            }
            
            const fileName = file.name || 'Unknown file';
            const fileType = file.type || '';
            
            const fileDiv = document.createElement('div');
            fileDiv.className = 'uploaded-file';
            fileDiv.innerHTML = `
                <i class="fas ${this.getFileIcon(fileType)}"></i>
                <span>${fileName}</span>
                <button class="remove-file" onclick="chatgptIntegration.removeFile(${index})">
                    <i class="fas fa-times"></i>
                </button>
            `;
            uploadArea.appendChild(fileDiv);
        });
    }

    /**
     * Remove file from uploads
     */
    removeFile(index) {
        this.fileUploads.splice(index, 1);
        this.updateFileUploadDisplay();
    }

    /**
     * Add typing indicator
     */
    addTypingIndicator() {
        const messagesContainer = document.getElementById('chatgpt-messages');
        if (!messagesContainer) return;

        const typingDiv = document.createElement('div');
        typingDiv.className = 'chatgpt-message assistant typing';
        typingDiv.id = 'typing-indicator';
        typingDiv.innerHTML = `
            <div class="message-avatar">
                <img src="images/wall_e.svg" alt="WALL-E" class="avatar-icon">
            </div>
            <div class="message-content">
                <div class="typing-indicator">
                    <span></span>
                    <span></span>
                    <span></span>
                </div>
            </div>
        `;
        messagesContainer.appendChild(typingDiv);
        messagesContainer.scrollTop = messagesContainer.scrollHeight;
    }

    /**
     * Remove typing indicator
     */
    removeTypingIndicator() {
        const typingIndicator = document.getElementById('typing-indicator');
        if (typingIndicator) {
            typingIndicator.remove();
        }
    }

    /**
     * Auto-resize textarea
     */
    autoResizeTextarea(textarea) {
        textarea.style.height = 'auto';
        textarea.style.height = Math.min(textarea.scrollHeight, 120) + 'px';
    }

    /**
     * Format response with markdown-like formatting
     */
    formatResponse(text) {
        // Convert markdown-like formatting to HTML
        return text
            .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
            .replace(/\*(.*?)\*/g, '<em>$1</em>')
            .replace(/`(.*?)`/g, '<code>$1</code>')
            .replace(/\[Generated file:\s*([A-Za-z0-9_-]+)\]/g, '<button type="button" class="btn btn-secondary btn-sm wall-e-file-download" data-file-id="$1">Download generated file</button>')
            .replace(/\[Local report:\s*([A-Za-z0-9_-]+)\|([^\]]+)\]/g, '<button type="button" class="btn btn-secondary btn-sm wall-e-local-report-download" data-local-report-id="$1">$2</button>')
            .replace(/\n/g, '<br>');
    }

    /**
     * Escape HTML
     */
    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    /**
     * Show error message
     */
    showError(message) {
        if (window.dashboardManager) {
            window.dashboardManager.showError(message);
        } else {
            console.error('WALL-E Error:', message);
            // Show a simple alert if dashboard manager is not available
            alert('WALL-E Error: ' + message);
        }
    }

    /**
     * Update context
     */
    updateContext(context) {
        this.currentContext = context;
    }

    /**
     * Get chat history
     */
    getChatHistory() {
        return this.chatHistory;
    }

    /**
     * Get current user ID for history storage
     */
    getCurrentUserId() {
        // Try to get user ID from various sources
        if (window.authManager && window.authManager.currentUser) {
            return window.authManager.currentUser.username || window.authManager.currentUser.email;
        }
        
        // Fallback to session storage or generate temporary ID
        const sessionId = sessionStorage.getItem('wall_e_session_id');
        if (sessionId) {
            return sessionId;
        }
        
        // Generate new session ID
        const newSessionId = 'guest_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
        sessionStorage.setItem('wall_e_session_id', newSessionId);
        return newSessionId;
    }

    /**
     * Load chat history for current user
     */
    loadChatHistory() {
        try {
            const userId = this.getCurrentUserId();
            this.currentUserId = userId;
            
            // If we have a current thread, load its history
            if (this.threadId) {
                this.loadChatHistoryForThread(this.threadId);
            } else {
                // Legacy: load from old format
                const historyKey = `wall_e_chat_history_${userId}`;
                const savedHistory = localStorage.getItem(historyKey);
                
                if (savedHistory) {
                    let parsed;
                    try { parsed = JSON.parse(savedHistory); } catch { parsed = null; }
                    if (parsed && parsed.enc && parsed.data) {
                        if (this.cryptoReady) {
                            this.decryptString(parsed.data).then((plain) => {
                                try {
                                    const arr = JSON.parse(plain || '[]');
                                    this.chatHistory = Array.isArray(arr) ? arr.slice(-this.maxHistoryItems) : [];
                                    console.log(`Loaded ${this.chatHistory.length} encrypted legacy messages for user: ${userId}`);
                                    this.displayChatHistory();
                                } catch {
                                    this.chatHistory = [];
                                    this.displayChatHistory();
                                }
                            });
                            return;
                        } else {
                            console.warn('Encrypted legacy history present but crypto not ready');
                            this.chatHistory = [];
                        }
                    } else if (Array.isArray(parsed)) {
                        this.chatHistory = parsed.slice(-this.maxHistoryItems);
                        console.log(`Loaded ${this.chatHistory.length} chat history items for user: ${userId}`);
                        this.displayChatHistory();
                    } else {
                        console.log('No valid legacy history found');
                    }
                } else {
                    console.log(`No chat history found for user: ${userId}`);
                }
            }
        } catch (error) {
            console.error('Failed to load chat history:', error);
        }
    }

    /**
     * Display chat history in the messages container
     */
    displayChatHistory() {
        const messagesContainer = document.getElementById('chatgpt-messages');
        if (!messagesContainer) return;

        if (this.chatHistory.length === 0) {
            const initialGuidelines = this.getInitialGuidelinesMessage();
            messagesContainer.innerHTML = `
                <div class="chatgpt-welcome">
                    <img src="images/wall_e.svg" alt="WALL-E" class="welcome-icon">
                    <h4>Wall-eeeee!</h4>
                    <p>Any help?</p>
                    <p class="wall-e-initial-guidelines">${this.escapeHtml(initialGuidelines)}</p>
                    ${!this.isEnabled ? '<p class="setup-notice"><strong>⚠️ Configuration Required:</strong> WALL-E configuration could not be loaded. Please check the Gist setup.</p>' : ''}
                </div>
            `;
            this.updateClearHistoryButton(false);
            return;
        }

        // Clear welcome message
        messagesContainer.innerHTML = '';

        // Display each message from history
        this.chatHistory.forEach(item => {
            if (item.role === 'user') {
                this.addMessageToDisplay('user', item.content, item.files || []);
            } else if (item.role === 'assistant') {
                this.addMessageToDisplay('assistant', item.content);
            } else if (item.role === 'error') {
                this.addMessageToDisplay('error', item.content);
            }
        });

        // Scroll to bottom
        messagesContainer.scrollTop = messagesContainer.scrollHeight;
        
        // Show clear history button
        this.updateClearHistoryButton(this.chatHistory.length > 0);
    }

    /**
     * Add message to display without saving to history (for loading history)
     */
    addMessageToDisplay(role, content, files = []) {
        const messagesContainer = document.getElementById('chatgpt-messages');
        if (!messagesContainer) return;

        const messageDiv = document.createElement('div');
        messageDiv.className = `chatgpt-message ${role}`;

        let messageHTML = '';
        
        if (role === 'user') {
            messageHTML = `
                <div class="message-content">
                    <div class="message-text">${this.escapeHtml(content)}</div>
                    ${this.renderFileAttachments(files)}
                </div>
                <div class="message-avatar">
                    <i class="fas fa-user"></i>
                </div>
            `;
        } else if (role === 'assistant') {
            // Check if content contains file attachments
            const hasFileAttachments = content.includes('[Image attachment:') || content.includes('[File attachment:');
            
            messageHTML = `
                <div class="message-avatar">
                    <img src="images/wall_e.svg" alt="WALL-E" class="avatar-icon">
                </div>
                <div class="message-content">
                    <div class="message-text">
                        ${this.formatResponse(content)}
                        ${hasFileAttachments ? '<p><em>📎 Assistant has attached files to this response</em></p>' : ''}
                    </div>
                </div>
            `;
        } else if (role === 'error') {
            messageHTML = `
                <div class="message-avatar">
                    <i class="fas fa-exclamation-triangle"></i>
                </div>
                <div class="message-content">
                    <div class="message-text error">${this.escapeHtml(content)}</div>
                </div>
            `;
        }

        messageDiv.innerHTML = messageHTML;
        messagesContainer.appendChild(messageDiv);
    }

    /**
     * Clear chat history for current user
     */
    clearChatHistory() {
        try {
            if (this.threadId) {
                // Clear current thread history
                const userId = this.getCurrentUserId();
                const historyKey = `wall_e_chat_history_${userId}_${this.threadId}`;
                localStorage.removeItem(historyKey);
            }
            
            this.chatHistory = [];
            
            // Reset to welcome message
            const messagesContainer = document.getElementById('chatgpt-messages');
            if (messagesContainer) {
                messagesContainer.innerHTML = `
                    <div class="chatgpt-welcome">
                        <img src="images/wall_e.svg" alt="WALL-E" class="welcome-icon">
                        <h4>Wall-eeeee!</h4>
                        <p>Any help?</p>
                        ${!this.isEnabled ? '<p class="setup-notice"><strong>⚠️ Configuration Required:</strong> WALL-E configuration could not be loaded. Please check the Gist setup.</p>' : ''}
                    </div>
                `;
            }
            
            console.log(`Cleared chat history for thread: ${this.threadId}`);
        } catch (error) {
            console.error('Failed to clear chat history:', error);
        }
    }

    /**
     * Update clear history button visibility
     */
    updateClearHistoryButton(show) {
        const clearHistoryBtn = document.getElementById('chatgpt-clear-history');
        if (clearHistoryBtn) {
            if (show) {
                clearHistoryBtn.style.display = 'flex';
                clearHistoryBtn.classList.add('visible');
            } else {
                clearHistoryBtn.style.display = 'none';
                clearHistoryBtn.classList.remove('visible');
            }
        }
    }

    /**
     * Save chat history for current user
     */
    saveChatHistory() {
        // Use the new thread-based save method
        this.saveChatHistoryForThread();
    }

    /**
     * Show rename thread prompt
     */
    renameThreadPrompt(threadId) {
        const thread = this.savedThreads.find(t => t.id === threadId);
        if (!thread) return;
        
        const newName = prompt('Enter new thread name:', thread.name);
        if (newName && newName.trim() && newName.trim() !== thread.name) {
            thread.name = newName.trim();
            if (threadId === this.threadId) {
                this.currentThreadName = newName.trim();
            }
            this.saveThreads();
            this.updateThreadSelector();
        }
    }

    /**
     * Toggle thread selector visibility
     */
    toggleThreadSelector() {
        const threadSelector = document.getElementById('chatgpt-thread-selector');
        
        if (!threadSelector) {
            console.error('Thread selector element not found!');
            return;
        }
        
        if (threadSelector.style.display === 'none' || !threadSelector.style.display) {
            this.showThreadSelector();
        } else {
            this.hideThreadSelector();
        }
    }

    /**
     * Show thread selector
     */
    showThreadSelector() {
        const threadSelector = document.getElementById('chatgpt-thread-selector');
        if (!threadSelector) {
            console.error('Thread selector element not found in showThreadSelector!');
            return;
        }
        
        threadSelector.style.display = 'block';
        threadSelector.style.visibility = 'visible';
        threadSelector.style.opacity = '1';
        
        // Update the thread list
        this.updateThreadSelector();
    }

    /**
     * Hide thread selector
     */
    hideThreadSelector() {
        const threadSelector = document.getElementById('chatgpt-thread-selector');
        if (!threadSelector) {
            console.error('Thread selector element not found in hideThreadSelector!');
            return;
        }
        
        threadSelector.style.display = 'none';
    }
}

// Create global instance
window.chatgptIntegration = new ChatGPTIntegration();
window.wallE = window.chatgptIntegration; // Alias for compatibility

// Export for module systems
if (typeof module !== 'undefined' && module.exports) {
    module.exports = ChatGPTIntegration;
}