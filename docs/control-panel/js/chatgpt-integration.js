/**
 * ChatGPT Integration Module for Liber Apps Control Panel
 * Provides AI assistance across the platform with file upload support
 */

class ChatGPTIntegration {
    constructor() {
        this.apiKey = null;
        this.isEnabled = false;
        this.chatHistory = [];
        this.currentContext = 'control-panel';
        this.fileUploads = [];
        this.maxFileSize = 25 * 1024 * 1024; // 25MB limit
        this.supportedFileTypes = ['image/*', 'application/pdf', 'text/*'];
        this.init();
    }

    /**
     * Initialize ChatGPT integration
     */
    init() {
        this.loadSettings();
        this.setupEventListeners();
        this.createChatInterface();
    }

    /**
     * Load ChatGPT settings
     */
    loadSettings() {
        const settings = localStorage.getItem('chatgpt-settings');
        if (settings) {
            const parsed = JSON.parse(settings);
            this.apiKey = parsed.apiKey;
            this.isEnabled = parsed.isEnabled || false;
        }
    }

    /**
     * Save ChatGPT settings
     */
    saveSettings() {
        const settings = {
            apiKey: this.apiKey,
            isEnabled: this.isEnabled,
            lastUpdated: new Date().toISOString()
        };
        localStorage.setItem('chatgpt-settings', JSON.stringify(settings));
    }

    /**
     * Setup event listeners
     */
    setupEventListeners() {
        // Listen for settings changes
        document.addEventListener('chatgpt-settings-changed', (e) => {
            this.loadSettings();
        });

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
                        <i class="fas fa-robot"></i>
                        <span>AI Assistant</span>
                    </div>
                    <div class="chatgpt-controls">
                        <button class="chatgpt-toggle" id="chatgpt-toggle">
                            <i class="fas fa-chevron-up"></i>
                        </button>
                        <button class="chatgpt-settings" id="chatgpt-settings">
                            <i class="fas fa-cog"></i>
                        </button>
                    </div>
                </div>
                <div class="chatgpt-body" id="chatgpt-body">
                    <div class="chatgpt-messages" id="chatgpt-messages">
                        <div class="chatgpt-welcome">
                            <i class="fas fa-lightbulb"></i>
                            <h4>Welcome to AI Assistant!</h4>
                            <p>I can help you with:</p>
                            <ul>
                                <li>Using this application</li>
                                <li>Analyzing images and PDFs</li>
                                <li>Answering questions</li>
                                <li>Providing guidance</li>
                            </ul>
                            <p>Upload files or ask me anything!</p>
                        </div>
                    </div>
                    <div class="chatgpt-input-area">
                        <div class="chatgpt-file-upload" id="chatgpt-file-upload">
                            <input type="file" id="chatgpt-file-input" multiple accept="${this.supportedFileTypes.join(',')}" style="display: none;">
                            <button class="chatgpt-upload-btn" id="chatgpt-upload-btn">
                                <i class="fas fa-paperclip"></i>
                            </button>
                        </div>
                        <div class="chatgpt-input-container">
                            <textarea id="chatgpt-input" placeholder="Ask me anything..." rows="1"></textarea>
                            <button class="chatgpt-send" id="chatgpt-send">
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
        const settings = document.getElementById('chatgpt-settings');
        const send = document.getElementById('chatgpt-send');
        const input = document.getElementById('chatgpt-input');
        const uploadBtn = document.getElementById('chatgpt-upload-btn');
        const fileInput = document.getElementById('chatgpt-file-input');

        if (toggle) {
            toggle.addEventListener('click', () => this.toggleChat());
        }

        if (settings) {
            settings.addEventListener('click', () => this.showSettings());
        }

        if (send) {
            send.addEventListener('click', () => this.sendMessage());
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
            uploadBtn.addEventListener('click', () => fileInput.click());
        }

        if (fileInput) {
            fileInput.addEventListener('change', (e) => this.handleFileUpload(e));
        }
    }

    /**
     * Toggle chat visibility
     */
    toggleChat() {
        const widget = document.getElementById('chatgpt-widget');
        const body = document.getElementById('chatgpt-body');
        const toggle = document.getElementById('chatgpt-toggle');
        const icon = toggle.querySelector('i');

        if (body.style.display === 'none') {
            body.style.display = 'block';
            icon.className = 'fas fa-chevron-up';
            widget.classList.add('expanded');
        } else {
            body.style.display = 'none';
            icon.className = 'fas fa-chevron-down';
            widget.classList.remove('expanded');
        }
    }

    /**
     * Show settings modal
     */
    showSettings() {
        const modal = this.createSettingsModal();
        document.body.appendChild(modal);
    }

    /**
     * Create settings modal
     */
    createSettingsModal() {
        const modal = document.createElement('div');
        modal.className = 'modal-overlay';
        modal.innerHTML = `
            <div class="modal-content chatgpt-settings-modal">
                <div class="modal-header">
                    <h3><i class="fas fa-robot"></i> AI Assistant Settings</h3>
                    <button class="modal-close">&times;</button>
                </div>
                <div class="modal-body">
                    <div class="setting-group">
                        <label for="chatgpt-api-key">OpenAI API Key:</label>
                        <input type="password" id="chatgpt-api-key" placeholder="sk-..." value="${this.apiKey || ''}">
                        <small>Get your API key from <a href="https://platform.openai.com/api-keys" target="_blank">OpenAI Platform</a></small>
                    </div>
                    <div class="setting-group">
                        <label>
                            <input type="checkbox" id="chatgpt-enabled" ${this.isEnabled ? 'checked' : ''}>
                            Enable AI Assistant
                        </label>
                    </div>
                    <div class="setting-group">
                        <label for="chatgpt-model">Model:</label>
                        <select id="chatgpt-model">
                            <option value="gpt-4o">GPT-4o (Recommended)</option>
                            <option value="gpt-4o-mini">GPT-4o Mini (Faster)</option>
                            <option value="gpt-4-turbo">GPT-4 Turbo</option>
                        </select>
                    </div>
                    <div class="setting-group">
                        <label for="chatgpt-context">Context Awareness:</label>
                        <select id="chatgpt-context">
                            <option value="full">Full Context (Current app + page)</option>
                            <option value="app">App Context Only</option>
                            <option value="minimal">Minimal Context</option>
                        </select>
                    </div>
                </div>
                <div class="modal-actions">
                    <button class="btn secondary" onclick="this.closest('.modal-overlay').remove()">Cancel</button>
                    <button class="btn" onclick="chatgptIntegration.saveSettingsFromModal()">Save Settings</button>
                </div>
            </div>
        `;

        // Setup modal close
        const closeBtn = modal.querySelector('.modal-close');
        closeBtn.addEventListener('click', () => modal.remove());

        modal.addEventListener('click', (e) => {
            if (e.target === modal) modal.remove();
        });

        return modal;
    }

    /**
     * Save settings from modal
     */
    saveSettingsFromModal() {
        const apiKey = document.getElementById('chatgpt-api-key').value.trim();
        const enabled = document.getElementById('chatgpt-enabled').checked;
        const model = document.getElementById('chatgpt-model').value;
        const context = document.getElementById('chatgpt-context').value;

        this.apiKey = apiKey;
        this.isEnabled = enabled;
        this.model = model;
        this.contextLevel = context;

        this.saveSettings();
        this.updateInterface();

        // Close modal
        document.querySelector('.chatgpt-settings-modal').closest('.modal-overlay').remove();

        // Show success message
        if (window.dashboardManager) {
            window.dashboardManager.showSuccess('AI Assistant settings saved successfully!');
        }
    }

    /**
     * Update interface based on settings
     */
    updateInterface() {
        const widget = document.getElementById('chatgpt-widget');
        if (widget) {
            widget.classList.toggle('enabled', this.isEnabled);
            widget.classList.toggle('disabled', !this.isEnabled);
        }
    }

    /**
     * Send message to ChatGPT
     */
    async sendMessage() {
        const input = document.getElementById('chatgpt-input');
        const message = input.value.trim();

        if (!message && this.fileUploads.length === 0) return;

        if (!this.isEnabled || !this.apiKey) {
            this.showError('Please configure your OpenAI API key in settings.');
            return;
        }

        // Add user message to chat
        this.addMessage('user', message, this.fileUploads);

        // Clear input and file uploads
        input.value = '';
        this.fileUploads = [];
        this.updateFileUploadDisplay();

        // Show typing indicator
        this.addTypingIndicator();

        try {
            const response = await this.callChatGPT(message);
            this.removeTypingIndicator();
            this.addMessage('assistant', response);
        } catch (error) {
            this.removeTypingIndicator();
            this.addMessage('error', `Error: ${error.message}`);
        }
    }

    /**
     * Call ChatGPT API
     */
    async callChatGPT(message) {
        const messages = this.buildMessages(message);
        const model = this.model || 'gpt-4o';

        const response = await fetch('https://api.openai.com/v1/chat/completions', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${this.apiKey}`
            },
            body: JSON.stringify({
                model: model,
                messages: messages,
                max_tokens: 1000,
                temperature: 0.7
            })
        });

        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.error?.message || 'Failed to get response from ChatGPT');
        }

        const data = await response.json();
        return data.choices[0].message.content;
    }

    /**
     * Build messages for API call
     */
    buildMessages(userMessage) {
        const messages = [
            {
                role: 'system',
                content: this.buildSystemPrompt()
            }
        ];

        // Add chat history
        this.chatHistory.forEach(msg => {
            if (msg.role !== 'error') {
                messages.push({
                    role: msg.role,
                    content: msg.content
                });
            }
        });

        // Add current message
        if (userMessage) {
            messages.push({
                role: 'user',
                content: userMessage
            });
        }

        return messages;
    }

    /**
     * Build system prompt based on context
     */
    buildSystemPrompt() {
        const context = this.getCurrentContext();
        const appInfo = this.getAppInfo();

        let prompt = `You are a helpful AI assistant integrated into the Liber Apps Control Panel. `;

        if (context === 'control-panel') {
            prompt += `You're currently in the main control panel. Help users navigate and use the available apps and features. `;
        } else {
            prompt += `You're currently in the ${appInfo.name} app. Help users with this specific application. `;
        }

        prompt += `Be concise, helpful, and user-friendly. If users upload files, analyze them and provide relevant insights. `;
        prompt += `Current context: ${context}`;

        return prompt;
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
                    <i class="fas fa-robot"></i>
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
        const typingDiv = document.createElement('div');
        typingDiv.className = 'chatgpt-message assistant typing';
        typingDiv.id = 'typing-indicator';
        typingDiv.innerHTML = `
            <div class="message-avatar">
                <i class="fas fa-robot"></i>
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
            console.error('ChatGPT Error:', message);
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
                    <i class="fas fa-lightbulb"></i>
                    <h4>Welcome to AI Assistant!</h4>
                    <p>I can help you with:</p>
                    <ul>
                        <li>Using this application</li>
                        <li>Analyzing images and PDFs</li>
                        <li>Answering questions</li>
                        <li>Providing guidance</li>
                    </ul>
                    <p>Upload files or ask me anything!</p>
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
