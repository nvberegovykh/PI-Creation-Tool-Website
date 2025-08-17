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
        this.init();
    }

    /**
     * Initialize WALL-E integration
     */
    async init() {
        await this.loadConfiguration();
        this.createChatInterface();
        this.setupEventListeners();
        
        // Initialize in collapsed state
        this.collapseChat();
    }

    /**
     * Load configuration from GitHub Gist
     */
    async loadConfiguration() {
        try {
            // Gist ID - using the actual Gist created by the user
            const gistId = '136b856c946794fad800c6363a8fa86e';
            const filename = 'wall-e-config.json';
            
            // Load configuration from Gist
            const response = await fetch(`https://api.github.com/gists/${gistId}`);
            
            if (!response.ok) {
                throw new Error(`Failed to load configuration: ${response.status}`);
            }
            
            const gist = await response.json();
            const configFile = gist.files[filename];
            
            if (!configFile) {
                throw new Error(`Configuration file '${filename}' not found in Gist`);
            }
            
            const config = JSON.parse(configFile.content);
            
            // Set configuration values
            this.apiKey = config.openai.apiKey;
            this.assistantId = config.openai.assistantId;
            this.isEnabled = true;
            this.configLoaded = true;
            
            console.log('WALL-E configuration loaded successfully');
            
        } catch (error) {
            console.error('Failed to load WALL-E configuration:', error);
            this.showError('Failed to load WALL-E configuration. Please check the Gist setup.');
            this.isEnabled = false;
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
        const chatHTML = `
            <div id="chatgpt-widget" class="chatgpt-widget ${this.isEnabled ? 'enabled' : 'disabled'}">
                <div class="chatgpt-header">
                    <div class="chatgpt-title">
                        <img src="images/wall_e.svg" alt="WALL-E" class="chatgpt-icon" onerror="this.style.display='none'; this.nextElementSibling.style.display='block';">
                        <i class="fas fa-robot" style="display: none;"></i>
                        <span>WALL-E</span>
                    </div>
                    <div class="chatgpt-controls">
                        <button class="chatgpt-toggle" id="chatgpt-toggle" title="Toggle Chat">
                            <i class="fas fa-chevron-up"></i>
                        </button>
                    </div>
                </div>
                <div class="chatgpt-body" id="chatgpt-body" style="display: none;">
                    <div class="chatgpt-messages" id="chatgpt-messages">
                        <div class="chatgpt-welcome">
                            <img src="images/wall_e.svg" alt="WALL-E" class="welcome-icon" onerror="this.style.display='none'; this.nextElementSibling.style.display='block';">
                            <i class="fas fa-robot" style="display: none;"></i>
                            <h4>Welcome to WALL-E!</h4>
                            <p>I'm your AI assistant powered by GPT-4o. I can help you with:</p>
                            <ul>
                                <li>Using this application</li>
                                <li>Analyzing images and PDFs</li>
                                <li>Generating images from descriptions</li>
                                <li>Answering questions</li>
                                <li>Providing guidance</li>
                            </ul>
                            <p>Upload files, ask questions, or request image generation!</p>
                            ${!this.isEnabled ? '<p class="setup-notice"><strong>⚠️ Configuration Required:</strong> WALL-E configuration could not be loaded. Please check the Gist setup.</p>' : ''}
                        </div>
                    </div>
                    <div class="chatgpt-input-area">
                        <div class="chatgpt-file-upload" id="chatgpt-file-upload">
                            <input type="file" id="chatgpt-file-input" multiple accept="${this.supportedFileTypes.join(',')}" style="display: none;">
                            <button class="chatgpt-upload-btn" id="chatgpt-upload-btn" title="Attach files">
                                <i class="fas fa-paperclip"></i>
                            </button>
                            <button class="chatgpt-image-gen-btn" id="chatgpt-image-gen-btn" title="Generate Image">
                                <i class="fas fa-image"></i>
                            </button>
                        </div>
                        <div class="chatgpt-input-container">
                            <textarea id="chatgpt-input" placeholder="Ask WALL-E anything or request image generation..." rows="1"></textarea>
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
            document.body.insertAdjacentHTML('beforeend', chatHTML);
        }

        this.setupChatEventListeners();
    }

    /**
     * Setup chat event listeners
     */
    setupChatEventListeners() {
        const toggle = document.getElementById('chatgpt-toggle');
        const send = document.getElementById('chatgpt-send');
        const input = document.getElementById('chatgpt-input');
        const uploadBtn = document.getElementById('chatgpt-upload-btn');
        const imageGenBtn = document.getElementById('chatgpt-image-gen-btn');
        const fileInput = document.getElementById('chatgpt-file-input');

        if (toggle) {
            toggle.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation();
                this.toggleChat();
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

        if (imageGenBtn) {
            imageGenBtn.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation();
                this.showImageGenerationModal();
            });
        }

        if (fileInput) {
            fileInput.addEventListener('change', (e) => this.handleFileUpload(e));
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

        const modal = document.createElement('div');
        modal.className = 'modal-overlay';
        modal.innerHTML = `
            <div class="modal-content image-gen-modal">
                <div class="modal-header">
                    <h3><img src="images/wall_e.svg" alt="WALL-E" style="width: 20px; height: 20px; margin-right: 8px;" onerror="this.style.display='none'; this.nextElementSibling.style.display='inline';"><i class="fas fa-robot" style="display: none;"></i> Generate Image with WALL-E</h3>
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
    }

    /**
     * Generate image using DALL-E
     */
    async generateImage() {
        if (!this.isEnabled || !this.apiKey) {
            this.showError('WALL-E is not configured. Please check the Gist setup.');
            return;
        }

        const prompt = document.getElementById('image-prompt').value.trim();
        const size = document.getElementById('image-size').value;
        const quality = document.getElementById('image-quality').value;

        if (!prompt) {
            this.showError('Please enter an image description.');
            return;
        }

        // Close modal
        const modal = document.querySelector('.image-gen-modal');
        if (modal) {
            modal.closest('.modal-overlay').remove();
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
                <img src="images/wall_e.svg" alt="WALL-E" class="avatar-icon" onerror="this.style.display='none'; this.nextElementSibling.style.display='block';">
                <i class="fas fa-robot" style="display: none;"></i>
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
        if (this.isExpanded) {
            this.collapseChat();
        } else {
            this.expandChat();
        }
    }

    /**
     * Expand chat
     */
    expandChat() {
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
    }

    /**
     * Collapse chat
     */
    collapseChat() {
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
    }

    /**
     * Send message to WALL-E
     */
    async sendMessage() {
        if (!this.isEnabled || !this.apiKey) {
            this.showError('WALL-E is not configured. Please check the Gist setup.');
            return;
        }

        const input = document.getElementById('chatgpt-input');
        const message = input.value.trim();

        if (!message && this.fileUploads.length === 0) return;

        // Add user message to chat
        this.addMessage('user', message, this.fileUploads);

        // Clear input and file uploads
        input.value = '';
        this.fileUploads = [];
        this.updateFileUploadDisplay();

        // Show typing indicator
        this.addTypingIndicator();

        try {
            const response = await this.callWALLE(message);
            this.removeTypingIndicator();
            this.addMessage('assistant', response);
        } catch (error) {
            this.removeTypingIndicator();
            this.addMessage('error', `Error: ${error.message}`);
        }
    }

    /**
     * Call WALL-E Assistant API
     */
    async callWALLE(message) {
        try {
            // Create or get thread
            if (!this.threadId) {
                this.threadId = await this.createThread();
            }

            // Add message to thread
            await this.addMessageToThread(message);

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
     * Create a new thread
     */
    async createThread() {
        const response = await fetch('https://api.openai.com/v1/threads', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${this.apiKey}`,
                'OpenAI-Beta': 'assistants=v1'
            }
        });

        if (!response.ok) {
            throw new Error('Failed to create thread');
        }

        const data = await response.json();
        return data.id;
    }

    /**
     * Add message to thread
     */
    async addMessageToThread(message) {
        const response = await fetch(`https://api.openai.com/v1/threads/${this.threadId}/messages`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${this.apiKey}`,
                'OpenAI-Beta': 'assistants=v1'
            },
            body: JSON.stringify({
                role: 'user',
                content: message
            })
        });

        if (!response.ok) {
            throw new Error('Failed to add message to thread');
        }

        return await response.json();
    }

    /**
     * Run the assistant
     */
    async runAssistant() {
        const response = await fetch(`https://api.openai.com/v1/threads/${this.threadId}/runs`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${this.apiKey}`,
                'OpenAI-Beta': 'assistants=v1'
            },
            body: JSON.stringify({
                assistant_id: this.assistantId
            })
        });

        if (!response.ok) {
            throw new Error('Failed to run assistant');
        }

        const data = await response.json();
        return data.id;
    }

    /**
     * Wait for assistant response
     */
    async waitForResponse(runId) {
        let attempts = 0;
        const maxAttempts = 30; // 30 seconds timeout

        while (attempts < maxAttempts) {
            const response = await fetch(`https://api.openai.com/v1/threads/${this.threadId}/runs/${runId}`, {
                headers: {
                    'Authorization': `Bearer ${this.apiKey}`,
                    'OpenAI-Beta': 'assistants=v1'
                }
            });

            if (!response.ok) {
                throw new Error('Failed to check run status');
            }

            const data = await response.json();

            if (data.status === 'completed') {
                // Get the response message
                return await this.getLastMessage();
            } else if (data.status === 'failed') {
                throw new Error('Assistant run failed');
            } else if (data.status === 'requires_action') {
                throw new Error('Assistant requires action');
            }

            // Wait 1 second before checking again
            await new Promise(resolve => setTimeout(resolve, 1000));
            attempts++;
        }

        throw new Error('Assistant response timeout');
    }

    /**
     * Get the last message from the thread
     */
    async getLastMessage() {
        const response = await fetch(`https://api.openai.com/v1/threads/${this.threadId}/messages?limit=1`, {
            headers: {
                'Authorization': `Bearer ${this.apiKey}`,
                'OpenAI-Beta': 'assistants=v1'
            }
        });

        if (!response.ok) {
            throw new Error('Failed to get messages');
        }

        const data = await response.json();
        if (data.data && data.data.length > 0) {
            const message = data.data[0];
            if (message.content && message.content.length > 0) {
                return message.content[0].text.value;
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
            messageHTML = `
                <div class="message-avatar">
                    <img src="images/wall_e.svg" alt="WALL-E" class="avatar-icon" onerror="this.style.display='none'; this.nextElementSibling.style.display='block';">
                    <i class="fas fa-robot" style="display: none;"></i>
                </div>
                <div class="message-content">
                    <div class="message-text">${this.formatResponse(content)}</div>
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
        this.chatHistory.push({ role, content, timestamp: new Date() });

        // Limit history
        if (this.chatHistory.length > 50) {
            this.chatHistory = this.chatHistory.slice(-50);
        }
    }

    /**
     * Render file attachments
     */
    renderFileAttachments(files) {
        if (!files || files.length === 0) return '';

        return `
            <div class="file-attachments">
                ${files.map(file => `
                    <div class="file-attachment">
                        <i class="fas ${this.getFileIcon(file.type)}"></i>
                        <span>${file.name}</span>
                        <small>(${this.formatFileSize(file.size)})</small>
                    </div>
                `).join('')}
            </div>
        `;
    }

    /**
     * Get file icon based on type
     */
    getFileIcon(type) {
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
        
        files.forEach(file => {
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
            const fileDiv = document.createElement('div');
            fileDiv.className = 'uploaded-file';
            fileDiv.innerHTML = `
                <i class="fas ${this.getFileIcon(file.type)}"></i>
                <span>${file.name}</span>
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
                <img src="images/wall_e.svg" alt="WALL-E" class="avatar-icon" onerror="this.style.display='none'; this.nextElementSibling.style.display='block';">
                <i class="fas fa-robot" style="display: none;"></i>
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
     * Clear chat history
     */
    clearChatHistory() {
        this.chatHistory = [];
        const messagesContainer = document.getElementById('chatgpt-messages');
        if (messagesContainer) {
            messagesContainer.innerHTML = `
                <div class="chatgpt-welcome">
                    <img src="images/wall_e.svg" alt="WALL-E" class="welcome-icon" onerror="this.style.display='none'; this.nextElementSibling.style.display='block';">
                    <i class="fas fa-robot" style="display: none;"></i>
                    <h4>Welcome to WALL-E!</h4>
                    <p>I'm your AI assistant powered by GPT-4o. I can help you with:</p>
                    <ul>
                        <li>Using this application</li>
                        <li>Analyzing images and PDFs</li>
                        <li>Generating images from descriptions</li>
                        <li>Answering questions</li>
                        <li>Providing guidance</li>
                    </ul>
                    <p>Upload files, ask questions, or request image generation!</p>
                    ${!this.isEnabled ? '<p class="setup-notice"><strong>⚠️ Configuration Required:</strong> WALL-E configuration could not be loaded. Please check the Gist setup.</p>' : ''}
                </div>
            `;
        }
    }
}

// Create global instance
window.chatgptIntegration = new ChatGPTIntegration();

// Export for module systems
if (typeof module !== 'undefined' && module.exports) {
    module.exports = ChatGPTIntegration;
}
