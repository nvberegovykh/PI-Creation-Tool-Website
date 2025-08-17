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
        
        // Load chat history for current user
        this.loadChatHistory();
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
        const chatHTML = `
            <div id="chatgpt-widget" class="chatgpt-widget ${this.isEnabled ? 'enabled' : 'disabled'}">
                <div class="chatgpt-header" id="chatgpt-header">
                    <div class="chatgpt-title">
                        <img src="images/wall_e.svg" alt="WALL-E" class="chatgpt-icon" onerror="this.style.display='none'; this.nextElementSibling.style.display='block';">
                        <i class="fas fa-robot" style="display: none;"></i>
                        <span>WALL-E</span>
                    </div>
                    <div class="chatgpt-controls">
                        <button class="chatgpt-clear-history" id="chatgpt-clear-history" title="Clear Chat History" style="display: none;">
                            <i class="fas fa-trash"></i>
                        </button>
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
                            <p><strong>💡 Tip:</strong> You can ask me to generate images by saying "generate an image of..." or "create a picture of..."</p>
                            <p><strong>📁 File Upload:</strong> Drag & drop files, paste images from clipboard, or use the paperclip button!</p>
                            <p><strong>💾 Chat History:</strong> Your conversations are automatically saved and will be restored when you return.</p>
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
            document.body.insertAdjacentHTML('beforeend', chatHTML);
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
        const widget = document.getElementById('chatgpt-widget');
        const body = document.getElementById('chatgpt-body');

        // Add header click listener for mobile expansion
        if (header) {
            header.addEventListener('click', (e) => {
                // Don't trigger if clicking on toggle button or other controls
                if (e.target.closest('.chatgpt-controls')) {
                    return;
                }
                e.preventDefault();
                e.stopPropagation();
                this.toggleChat();
            });
        }

        // Add specific click listener for WALL-E icon and title
        const title = document.querySelector('.chatgpt-title');
        if (title) {
            title.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation();
                this.toggleChat();
            });
        }

        // Add click listener for the WALL-E icon specifically
        const icon = document.querySelector('.chatgpt-icon');
        if (icon) {
            icon.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation();
                this.toggleChat();
            });
        }

        if (toggle) {
            toggle.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation();
                this.toggleChat();
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

            // Add paste event listener for file uploads
            input.addEventListener('paste', (e) => {
                this.handlePaste(e);
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

        // Check if this is an image generation request
        const isImageRequest = this.isImageGenerationRequest(message);

        // Add user message to chat
        this.addMessage('user', message, this.fileUploads);

        // Store file uploads before clearing
        const filesToSend = [...this.fileUploads];

        // Clear input and file uploads
        input.value = '';
        this.fileUploads = [];
        this.updateFileUploadDisplay();

        // Show typing indicator
        this.addTypingIndicator();

        try {
            if (isImageRequest) {
                // Handle image generation directly
                await this.handleImageGenerationRequest(message);
            } else {
                // Normal chat message
                const response = await this.callWALLE(message, filesToSend);
                this.removeTypingIndicator();
                this.addMessage('assistant', response);
            }
        } catch (error) {
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
     * Add message to thread
     */
    async addMessageToThread(message, files = []) {
        try {
            // Prepare message content
            const content = [];
            
            // Add text content if message exists
            if (message && message.trim()) {
                content.push({
                    type: 'text',
                    text: {
                        value: message.trim()
                    }
                });
            }

            // Add file content if files exist
            if (files && files.length > 0) {
                for (const file of files) {
                    try {
                        // Upload file to OpenAI
                        const fileId = await this.uploadFile(file);
                        
                        // Add file reference to content
                        content.push({
                            type: 'file',
                            file: {
                                file_id: fileId
                            }
                        });
                    } catch (fileError) {
                        console.error('Failed to upload file:', fileError);
                        // Continue with other files
                    }
                }
            }

            // If no content to send, return early
            if (content.length === 0) {
                throw new Error('No content to send');
            }

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
                console.error('Failed to add message to thread:', response.status, errorData);
                throw new Error(`Failed to add message to thread: ${response.status} - ${response.statusText}`);
            }

            return await response.json();
        } catch (error) {
            console.error('Error adding message to thread:', error);
            throw error;
        }
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
     * Run the assistant
     */
    async runAssistant() {
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
                    'OpenAI-Beta': 'assistants=v2'
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
                    <img src="images/wall_e.svg" alt="WALL-E" class="avatar-icon" onerror="this.style.display='none'; this.nextElementSibling.style.display='block';">
                    <i class="fas fa-robot" style="display: none;"></i>
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
            
            const historyKey = `wall_e_chat_history_${userId}`;
            const savedHistory = localStorage.getItem(historyKey);
            
            if (savedHistory) {
                const parsedHistory = JSON.parse(savedHistory);
                this.chatHistory = parsedHistory.slice(-this.maxHistoryItems); // Keep only last 50 items
                console.log(`Loaded ${this.chatHistory.length} chat history items for user: ${userId}`);
                
                // Display chat history
                this.displayChatHistory();
            } else {
                console.log(`No chat history found for user: ${userId}`);
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
                    <p><strong>💡 Tip:</strong> You can ask me to generate images by saying "generate an image of..." or "create a picture of..."</p>
                    <p><strong>💾 Chat History:</strong> Your conversations are automatically saved and will be restored when you return.</p>
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
        this.updateClearHistoryButton(true);
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
                    <img src="images/wall_e.svg" alt="WALL-E" class="avatar-icon" onerror="this.style.display='none'; this.nextElementSibling.style.display='block';">
                    <i class="fas fa-robot" style="display: none;"></i>
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
            const userId = this.getCurrentUserId();
            const historyKey = `wall_e_chat_history_${userId}`;
            
            localStorage.removeItem(historyKey);
            this.chatHistory = [];
            
            // Reset to welcome message
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
                        <p><strong>💡 Tip:</strong> You can ask me to generate images by saying "generate an image of..." or "create a picture of..."</p>
                        <p><strong>💾 Chat History:</strong> Your conversations are automatically saved and will be restored when you return.</p>
                        ${!this.isEnabled ? '<p class="setup-notice"><strong>⚠️ Configuration Required:</strong> WALL-E configuration could not be loaded. Please check the Gist setup.</p>' : ''}
                    </div>
                `;
            }
            
            console.log(`Cleared chat history for user: ${userId}`);
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
        try {
            const userId = this.getCurrentUserId();
            const historyKey = `wall_e_chat_history_${userId}`;
            
            // Keep only last 50 messages
            const historyToSave = this.chatHistory.slice(-this.maxHistoryItems);
            
            localStorage.setItem(historyKey, JSON.stringify(historyToSave));
            console.log(`Saved ${historyToSave.length} chat history items for user: ${userId}`);
            
            // Update clear history button visibility
            this.updateClearHistoryButton(historyToSave.length > 0);
        } catch (error) {
            console.error('Failed to save chat history:', error);
        }
    }
}

// Create global instance
window.chatgptIntegration = new ChatGPTIntegration();

// Export for module systems
if (typeof module !== 'undefined' && module.exports) {
    module.exports = ChatGPTIntegration;
}
