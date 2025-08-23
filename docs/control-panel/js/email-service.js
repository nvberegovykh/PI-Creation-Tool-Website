/**
 * Email Service Module for Liber Apps Control Panel
 * Handles email verification and password reset via Mailgun
 */

class EmailService {
    constructor() {
        this.mailgunDomain = null;
        this.mailgunApiKey = null;
        // Update baseUrl to point to your control panel
        this.baseUrl = window.location.origin + window.location.pathname;
        this.init();
    }

    async init() {
        try {
            await this.loadMailgunConfig();
            // Test the configuration
            await this.testMailgunConfig();
        } catch (error) {
            console.error('Failed to load Mailgun configuration:', error);
        }
    }

    /**
     * Test Mailgun configuration
     */
    async testMailgunConfig() {
        try {
            console.log('Testing Mailgun configuration...');
            await this.loadMailgunConfig();
            console.log('✅ Mailgun configuration test successful');
            console.log('Domain:', this.mailgunDomain);
            console.log('API Key available:', !!this.mailgunApiKey);
            return true;
        } catch (error) {
            console.error('❌ Mailgun configuration test failed:', error);
            return false;
        }
    }

    /**
     * Load Mailgun configuration from secure storage
     */
    async loadMailgunConfig() {
        try {
            // Get Mailgun config from secure storage
            const config = await window.secureKeyManager.getMailgunConfig();
            this.mailgunDomain = config.domain;
            this.mailgunApiKey = config.apiKey;
            console.log('Mailgun configuration loaded successfully');
        } catch (error) {
            console.error('Error loading Mailgun config:', error);
            throw new Error('Mailgun configuration not available');
        }
    }

    /**
     * Send email via Mailgun API
     */
    async sendEmail(to, subject, htmlContent) {
        // Prefer secure server-side callable if Functions available
        try{
            if (window.firebaseService && window.firebaseService.functions && window.firebaseModular && window.firebaseModular.httpsCallable){
                const callable = window.firebaseModular.httpsCallable(window.firebaseService.functions, 'sendMail');
                const res = await callable({ to, subject, html: htmlContent });
                if (res && res.data) return res.data;
            } else if (window.firebaseService && typeof window.firebaseService.callFunction === 'function'){
                const res = await window.firebaseService.callFunction('sendMail', { to, subject, html: htmlContent });
                if (res) return res;
            }
        }catch(e){ console.warn('Server email fallback to client:', e?.message||e); }

        // Fallback: direct Mailgun if configured locally
        if (!this.mailgunDomain || !this.mailgunApiKey) {
            throw new Error('Email unavailable (no functions and Mailgun not configured)');
        }
        const url = new URL(`https://api.mailgun.net/v3/${this.mailgunDomain}/messages`);
        url.searchParams.append('from', `Liber Apps <postmaster@${this.mailgunDomain}>`);
        url.searchParams.append('to', to);
        url.searchParams.append('subject', subject);
        url.searchParams.append('html', htmlContent);
        const response = await fetch(url.toString(), { method:'POST', headers:{ 'Authorization': `Basic ${btoa(`api:${this.mailgunApiKey}`)}` } });
        if (!response.ok) { const t = await response.text(); throw new Error(`Mailgun API error: ${response.status} - ${t}`); }
        return await response.json();
    }

    /**
     * Generate email verification token
     */
    generateVerificationToken() {
        return crypto.getRandomValues(new Uint8Array(32))
            .reduce((acc, val) => acc + val.toString(16).padStart(2, '0'), '');
    }

    /**
     * Generate password reset token
     */
    generateResetToken() {
        return crypto.getRandomValues(new Uint8Array(32))
            .reduce((acc, val) => acc + val.toString(16).padStart(2, '0'), '');
    }

    /**
     * Send verification email to new user
     * 
     * IMPORTANT: Mailgun sandbox domains can only send to authorized recipients.
     * To add authorized recipients:
     * 1. Go to https://app.mailgun.com/
     * 2. Navigate to Sending → Authorized Recipients
     * 3. Add the email address you want to send to
     * 4. Verify the email address when you receive the confirmation
     */
    async sendVerificationEmail(email, username, token) {
        const verificationUrl = `${this.baseUrl}?action=verify&token=${token}&email=${encodeURIComponent(email)}`;
        
        const htmlContent = `
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="UTF-8">
                <title>Verify Your Email - Liber Apps</title>
                <style>
                    body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; }
                    .container { max-width: 600px; margin: 0 auto; padding: 20px; }
                    .header { background: #007bff; color: white; padding: 20px; text-align: center; }
                    .content { padding: 20px; background: #f9f9f9; }
                    .button { display: inline-block; background: #007bff; color: white; padding: 12px 24px; text-decoration: none; border-radius: 5px; }
                    .footer { text-align: center; padding: 20px; color: #666; font-size: 12px; }
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="header">
                        <h1>LIBER/APPS</h1>
                        <p>Email Verification</p>
                    </div>
                    <div class="content">
                        <h2>Hello ${username}!</h2>
                        <p>Thank you for registering with Liber Apps Control Panel. To complete your registration, please verify your email address by clicking the button below:</p>
                        <p style="text-align: center;">
                            <a href="${verificationUrl}" class="button">Verify Email Address</a>
                        </p>
                        <p>If the button doesn't work, you can copy and paste this link into your browser:</p>
                        <p style="word-break: break-all; color: #007bff;">${verificationUrl}</p>
                        <p>This verification link will expire in 24 hours.</p>
                        <p>If you didn't create this account, you can safely ignore this email.</p>
                    </div>
                    <div class="footer">
                        <p>This email was sent from Liber Apps Control Panel</p>
                        <p>If you have any questions, please contact support.</p>
                    </div>
                </div>
            </body>
            </html>
        `;

        return await this.sendEmail(email, 'Verify Your Email - Liber Apps', htmlContent);
    }

    /**
     * Send password reset email
     */
    async sendPasswordResetEmail(email, username, token) {
        const resetUrl = `${this.baseUrl}?action=reset&token=${token}&email=${encodeURIComponent(email)}`;
        
        const htmlContent = `
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="UTF-8">
                <title>Reset Your Password - Liber Apps</title>
                <style>
                    body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; }
                    .container { max-width: 600px; margin: 0 auto; padding: 20px; }
                    .header { background: #007bff; color: white; padding: 20px; text-align: center; }
                    .content { padding: 20px; background: #f9f9f9; }
                    .button { display: inline-block; background: #007bff; color: white; padding: 12px 24px; text-decoration: none; border-radius: 5px; }
                    .footer { text-align: center; padding: 20px; color: #666; font-size: 12px; }
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="header">
                        <h1>LIBER/APPS</h1>
                        <p>Password Reset</p>
                    </div>
                    <div class="content">
                        <h2>Hello ${username}!</h2>
                        <p>We received a request to reset your password for your Liber Apps Control Panel account. Click the button below to create a new password:</p>
                        <p style="text-align: center;">
                            <a href="${resetUrl}" class="button">Reset Password</a>
                        </p>
                        <p>If the button doesn't work, you can copy and paste this link into your browser:</p>
                        <p style="word-break: break-all; color: #007bff;">${resetUrl}</p>
                        <p>This reset link will expire in 1 hour.</p>
                        <p>If you didn't request a password reset, you can safely ignore this email. Your password will remain unchanged.</p>
                    </div>
                    <div class="footer">
                        <p>This email was sent from Liber Apps Control Panel</p>
                        <p>If you have any questions, please contact support.</p>
                    </div>
                </div>
            </body>
            </html>
        `;

        return await this.sendEmail(email, 'Reset Your Password - Liber Apps', htmlContent);
    }

    /**
     * Verify email verification token
     */
    async verifyToken(token, email) {
        try {
            console.log('Verifying token for email:', email);
            console.log('Token received:', token);
            
            const users = await window.authManager.getUsers();
            console.log('Total users in storage:', users.length);
            console.log('All users:', users);
            
            const user = users.find(u => u.email === email && u.verificationToken === token);
            console.log('User found:', !!user);
            console.log('User data:', user);
            
            if (!user) {
                // Try to find user by email only to see if user exists
                const userByEmail = users.find(u => u.email === email);
                console.log('User by email only:', userByEmail);
                if (userByEmail) {
                    console.log('User exists but token mismatch. Expected:', userByEmail.verificationToken, 'Received:', token);
                    console.log('User verification status:', userByEmail.isVerified);
                    console.log('User status:', userByEmail.status);
                    
                    // If user is already verified, show appropriate message
                    if (userByEmail.isVerified) {
                        throw new Error('Email is already verified. You can login directly.');
                    }
                }
                throw new Error('Invalid or expired verification token');
            }

            // Check if token is expired (24 hours)
            const tokenAge = Date.now() - user.verificationTokenCreated;
            console.log('Token age (hours):', tokenAge / (60 * 60 * 1000));
            if (tokenAge > 24 * 60 * 60 * 1000) {
                throw new Error('Verification token has expired');
            }

            // Mark user as verified and approved
            user.isVerified = true;
            user.status = 'approved'; // Also set status to approved for admin panel
            user.verificationToken = null;
            user.verificationTokenCreated = null;
            user.verifiedAt = new Date().toISOString();

            // Update user in storage
            const updatedUsers = users.map(u => u.email === email ? user : u);
            await window.authManager.saveUsers(updatedUsers);
            
            console.log('User verified successfully:', user);

            return user;
        } catch (error) {
            console.error('Token verification error:', error);
            throw error;
        }
    }

    /**
     * Verify password reset token
     */
    async verifyResetToken(token, email) {
        try {
            const users = await window.authManager.getUsers();
            const user = users.find(u => u.email === email && u.resetToken === token);
            
            if (!user) {
                throw new Error('Invalid or expired reset token');
            }

            // Check if token is expired (1 hour)
            const tokenAge = Date.now() - user.resetTokenCreated;
            if (tokenAge > 60 * 60 * 1000) {
                throw new Error('Reset token has expired');
            }

            return user;
        } catch (error) {
            console.error('Reset token verification error:', error);
            throw error;
        }
    }

    /**
     * Update user password after reset
     */
    async updatePassword(email, newPassword) {
        try {
            const users = await window.authManager.getUsers();
            const user = users.find(u => u.email === email);
            
            if (!user) {
                throw new Error('User not found');
            }

            // Hash the new password
            const hashedPassword = await window.cryptoManager.hashPassword(newPassword);
            
            // Update user password
            user.passwordHash = hashedPassword;
            user.resetToken = null;
            user.resetTokenCreated = null;
            user.passwordUpdatedAt = new Date().toISOString();

            // Update user in storage
            const updatedUsers = users.map(u => u.email === email ? user : u);
            await window.authManager.saveUsers(updatedUsers);

            return user;
        } catch (error) {
            console.error('Password update error:', error);
            throw error;
        }
    }
}

// Create global instance
window.emailService = new EmailService();
