/**
 * WALL-E AI Assistant Integration Module for Liber Apps Control Panel
 * Powered by GPT-4o and OpenAI Assistant API
 */

class ChatGPTIntegration {
    constructor() {
        // Configuration will be loaded from Gist
        this.apiKey = null;
        this.assistantId = null;
        this.isEnabled = false;
        this.chatHistory = [];
        this.currentContext = 'control-panel';
        this.fileUploads = [];
        this.maxFileSize = 25 * 1024 * 1024; // 25MB limit
        this.supportedFileTypes = ['image/*', 'application/pdf', 'text/*', 'application/json', 'application/xml', 'text/csv'];
        this.isExpanded = false;
        this.threadId = null;
        this.configLoaded = false;
        this.currentUserId = null;
        this.maxHistoryItems = 50; // Keep last 50 messages
        
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
        await this.loadConfiguration();
        if (this.isEnabled) {
            await this.ensureAssistantSupportsFiles();
            await this.checkAssistantConfig(); // Check assistant configuration
        }
        this.loadSavedThreads(); // Load saved threads
        this.createChatInterface();
        this.setupEventListeners();
        this.loadChatHistory();
        this.displayChatHistory();
        
        // Set initial state based on screen size
        if (window.innerWidth <= 768) {
            this.isExpanded = true;
            this.updateMobileButtonState();
        }
    }

    /**
     * Ensure assistant supports file attachments
     */
    async ensureAssistantSupportsFiles() {
        try {
            // Get current assistant configuration
            const response = await fetch(`https://api.openai.com/v1/assistants/${this.assistantId}`, {
                headers: {
                    'Authorization': `Bearer ${this.apiKey}`,
                    'OpenAI-Beta': 'assistants=v2'
                }
            });

            if (!response.ok) {
                console.warn('Could not fetch assistant configuration, proceeding with current setup');
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
            console.log('Checking assistant configuration...');
            const response = await fetch(`https://api.openai.com/v1/assistants/${this.assistantId}`, {
                headers: {
                    'Authorization': `Bearer ${this.apiKey}`,
                    'OpenAI-Beta': 'assistants=v2'
                }
            });

            if (!response.ok) {
                const errorData = await response.text();
                console.error('Failed to get assistant config:', response.status, errorData);
                return;
            }

            const assistant = await response.json();
            console.log('Assistant Configuration:', {
                id: assistant.id,
                name: assistant.name,
                model: assistant.model,
                instructions: assistant.instructions,
                tools: assistant.tools
            });
            
            if (assistant.model !== 'gpt-4o-mini') {
                console.warn(`⚠️ Assistant is using model: ${assistant.model}, expected: gpt-4o-mini`);
            } else {
                console.log('✅ Assistant is correctly configured with gpt-4o-mini');
            }
        } catch (error) {
            console.error('Error checking assistant config:', error);
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
                this.savedThreads = JSON.parse(savedThreads);
                console.log(`Loaded ${this.savedThreads.length} saved threads for user: ${userId}`);
            } else {
                console.log(`No saved threads found for user: ${userId}`);
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
            
            localStorage.setItem(threadsKey, JSON.stringify(threadsToSave));
            console.log(`Saved ${threadsToSave.length} threads for user: ${userId}`);
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
                const parsedHistory = JSON.parse(savedHistory);
                this.chatHistory = parsedHistory.slice(-this.maxHistoryItems);
                console.log(`Loaded ${this.chatHistory.length} messages for thread: ${threadId}`);
            } else {
                this.chatHistory = [];
                console.log(`No history found for thread: ${threadId}`);
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
            
            localStorage.setItem(historyKey, JSON.stringify(historyToSave));
            console.log(`Saved ${historyToSave.length} messages for thread: ${this.threadId}`);
            
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
            // Use the correct Gist URL with your API keys and assistant ID
            const wallEGistUrl = this.decodeUrl('aHR0cHM6Ly9naXN0LmdpdGh1YnVzZXJjb250ZW50LmNvbS9udmJlcmVnb3Z5a2gvMTM2Yjg1NmM5NDY3OTRmYWQ4MDBjNjM2M2E4ZmE4NmUvcmF3L2M2YzU3MTA2MzM2YmZhNDllOTczYmJhMTZkYzU3Nzk5OGRlOTMwMDgvd2FsbC1lLWNvbmZpZy5qc29u');
            
            // Load configuration directly from Gist raw URL
            const response = await fetch(wallEGistUrl);
            
            if (!response.ok) {
                throw new Error(`Failed to load configuration: ${response.status} - ${response.statusText}`);
            }
            
            const config = await response.json();
            
            // Validate configuration
            if (!config.openai || !config.openai.apiKey || !config.openai.assistantId) {
                throw new Error('Invalid configuration format. Missing OpenAI API key or assistant ID.');
            }
            
            // Set configuration values
            this.apiKey = config.openai.apiKey;
            this.assistantId = config.openai.assistantId;
            this.isEnabled = true;
            this.configLoaded = true;
            
            console.log('WALL-E configuration loaded successfully');
            
        } catch (error) {
            console.error('Failed to load WALL-E configuration:', error);
            this.showError(`WALL-E Configuration Error: ${error.message}`);
            this.isEnabled = false;
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
            
            if (item.type.indexOf('image') !== -1) {
                const file = item.getAsFile();
                if (file) {
                    files.push(file);
                    hasFiles = true;
                }
            } else if (item.type.indexOf('text') !== -1) {
                // Handle text paste - let it go through normally
                continue;
            }
        }

        if (hasFiles) {
            e.preventDefault();
            console.log(`Processing ${files.length} files from paste event`);
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
        if (!this.isEnabled || !this.apiKey) {
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
        const response = await fetch('https://api.openai.com/v1/images/generations', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${this.apiKey}`
            },
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
        
        if (!this.isEnabled || !this.apiKey) {
            this.showError('WALL-E is not configured. Please check the Gist setup.');
            return;
        }

        const input = document.getElementById('chatgpt-input');
        const message = input.value.trim();

        console.log('Message from input:', message);
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
                console.log('Handling normal chat message with files:', filesToSend.length);
                // Normal chat message
                const response = await this.callWALLE(message, filesToSend);
                this.removeTypingIndicator();
                this.addMessage('assistant', response);
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
     * Call WALL-E Assistant API
     */
    async callWALLE(message, files = []) {
        try {
            // Validate configuration
            if (!this.apiKey || !this.assistantId) {
                throw new Error('WALL-E not configured. Please check your Gist configuration.');
            }

            // If no files, use chat completions API (more reliable for text-only)
            if (!files || files.length === 0) {
                console.log('No files detected, using chat completions API');
                return await this.callChatCompletions(message);
            }

            // If files are present, use assistants API
            console.log('Files detected, using assistants API');
            
            // Create or get thread
            if (!this.threadId) {
                this.threadId = await this.createThread();
            }

            // Add message to thread (with files if any)
            await this.addMessageToThread(message, files);

            // Run assistant
            const runId = await this.runAssistant();

            // Wait for completion and get response
            const response = await this.waitForResponse(runId);

            return response;
        } catch (error) {
            console.error('WALL-E API Error:', error);
            throw error;
        }
    }

    /**
     * Use Chat Completions API for text-only messages
     */
    async callChatCompletions(message) {
        try {
            console.log('Using chat completions API for text-only message');
            
            const response = await fetch('https://api.openai.com/v1/chat/completions', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${this.apiKey}`
                },
                body: JSON.stringify({
                    model: 'gpt-4o-mini',
                    messages: [
                        {
                            role: 'system',
                            content: 'You are WALL-E, a friendly, straight forward, peaceful and calm part of architectural team, that helps both users and our worker to reach any goals using logic, research, math and actual solutions, you have to be patient and well-thinking, sometimes it\'s better to take time to think, than find fastest but incorrect approach, your goal is to save everyone\'s time finding correct approaches. You are based on GPT-4o-mini.'
                        },
                        {
                            role: 'user',
                            content: message
                        }
                    ],
                    max_tokens: 1000,
                    temperature: 0.7
                })
            });

            if (!response.ok) {
                const errorData = await response.text();
                console.error('Chat completions failed:', response.status, errorData);
                throw new Error(`Chat completions failed: ${response.status} - ${response.statusText}`);
            }

            const data = await response.json();
            console.log('Chat completions response:', data);
            
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
            const response = await fetch('https://api.openai.com/v1/threads', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${this.apiKey}`,
                    'OpenAI-Beta': 'assistants=v2'
                }
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
            console.log('Message:', message);
            console.log('Files:', files);
            console.log('Files length:', files.length);
            
            // SCENARIO 1: No message and no files - INVALID
            if (!message && (!files || files.length === 0)) {
                throw new Error('Cannot send empty message with no files');
            }
            
            // SCENARIO 2: Text-only message (no files)
            if (message && (!files || files.length === 0)) {
                console.log('SCENARIO 2: Text-only message');
                
                console.log('Sending text-only content as string:', message.trim());
                
                const response = await fetch(`https://api.openai.com/v1/threads/${this.threadId}/messages`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': `Bearer ${this.apiKey}`,
                        'OpenAI-Beta': 'assistants=v2'
                    },
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
            
            const response = await fetch(`https://api.openai.com/v1/threads/${this.threadId}/messages`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${this.apiKey}`,
                    'OpenAI-Beta': 'assistants=v2'
                },
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
        
        const response = await fetch(`https://api.openai.com/v1/threads/${this.threadId}/messages`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${this.apiKey}`,
                'OpenAI-Beta': 'assistants=v2'
            },
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
            
            const response = await fetch(`https://api.openai.com/v1/threads/${this.threadId}/messages`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${this.apiKey}`,
                    'OpenAI-Beta': 'assistants=v2'
                },
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
        
        const response = await fetch(`https://api.openai.com/v1/threads/${this.threadId}/messages`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${this.apiKey}`,
                'OpenAI-Beta': 'assistants=v2'
            },
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

            const response = await fetch('https://api.openai.com/v1/files', {
                method: 'POST',
                headers: {
                    'Authorization': `Bearer ${this.apiKey}`
                },
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
            const getResponse = await fetch(`https://api.openai.com/v1/assistants/${this.assistantId}`, {
                method: 'GET',
                headers: {
                    'Authorization': `Bearer ${this.apiKey}`,
                    'OpenAI-Beta': 'assistants=v2'
                }
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
            const updateResponse = await fetch(`https://api.openai.com/v1/assistants/${this.assistantId}`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${this.apiKey}`,
                    'OpenAI-Beta': 'assistants=v2'
                },
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
            const response = await fetch(`https://api.openai.com/v1/threads/${this.threadId}/runs`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${this.apiKey}`,
                    'OpenAI-Beta': 'assistants=v2'
                },
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
                const response = await fetch(`https://api.openai.com/v1/threads/${this.threadId}/runs/${runId}`, {
                    headers: {
                        'Authorization': `Bearer ${this.apiKey}`,
                        'OpenAI-Beta': 'assistants=v2'
                    }
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
        const response = await fetch(`https://api.openai.com/v1/threads/${this.threadId}/messages`, {
            headers: {
                'Authorization': `Bearer ${this.apiKey}`,
                'OpenAI-Beta': 'assistants=v2'
            }
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
     * Get current context
     */
    getCurrentContext() {
        const path = window.location.pathname;
        if (path.includes('/apps/calculator/')) return 'calculator-app';
        if (path.includes('/apps/invoices-app/')) return 'invoice-app';
        if (path.includes('/control-panel')) return 'control-panel';
        return 'unknown';
    }

    /**
     * Get app information
     */
    getAppInfo() {
        const context = this.getCurrentContext();
        const appMap = {
            'calculator-app': { name: 'Calculator', description: 'Mathematical operations and calculations' },
            'invoice-app': { name: 'Invoice Generator', description: 'Create and manage invoices' },
            'control-panel': { name: 'Control Panel', description: 'Main dashboard for app management' }
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
                    const parsedHistory = JSON.parse(savedHistory);
                    this.chatHistory = parsedHistory.slice(-this.maxHistoryItems);
                    console.log(`Loaded ${this.chatHistory.length} chat history items for user: ${userId}`);
                    this.displayChatHistory();
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
            // Show welcome message if no history
            messagesContainer.innerHTML = `
                <div class="chatgpt-welcome">
                    <img src="images/wall_e.svg" alt="WALL-E" class="welcome-icon">
                    <h4>Wall-eeeee!</h4>
                    <p>Any help?</p>
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