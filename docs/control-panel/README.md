# Liber Apps Control Panel

A modern, secure web-based control panel for managing and launching applications with user authentication and encrypted data storage.

## Features

- 🔐 **Secure Authentication**: Admin and user roles with encrypted password storage
- 👥 **User Management**: Admin can register and manage users
- 🚀 **App Launcher**: Launch applications from a centralized dashboard
- 📱 **Responsive Design**: Works on desktop, tablet, and mobile devices
- 🔒 **Data Encryption**: All user data is encrypted using Web Crypto API
- 🎨 **Modern UI**: Black background with white elements, centered layout
- ⚡ **Fast Performance**: Optimized for speed and efficiency

## Admin Credentials

- **Username**: `nvberegovykh`
- **Password**: `[Configured in external secure storage]`

**Note**: Admin credentials are stored securely in external configuration and are not visible in the source code.

## Quick Start

1. **Clone or download** this repository
2. **Open** `index.html` in a web browser
3. **Login** with admin credentials (configured externally) or register a new user
4. **Start** managing your applications!

## Project Structure

```
liber-apps/
├── index.html              # Main application file
├── css/                    # Stylesheets
│   ├── style.css          # Global styles
│   ├── auth.css           # Authentication styles
│   ├── dashboard.css      # Dashboard layout
│   ├── apps.css           # Apps section styles
│   └── responsive.css     # Responsive design
├── js/                    # JavaScript modules
│   ├── crypto.js          # Encryption utilities
│   ├── auth.js            # Authentication system
│   ├── dashboard.js       # Dashboard functionality
│   ├── apps.js            # Apps management
│   ├── users.js           # User management
│   └── main.js            # Main application
├── apps/                  # Applications directory
│   ├── calculator/
│   ├── notepad/
│   ├── calendar/
│   └── ... (your apps)
└── README.md              # This file
```

## Adding Applications

To add new applications to the control panel:

1. Create a new folder in the `apps/` directory
2. Add your application files (HTML, CSS, JS)
3. Create an `index.html` file for your app
4. The app will be automatically detected and listed

### Example App Structure

```
apps/my-app/
├── index.html
├── style.css
├── script.js
└── assets/
    └── images/
```

## Security Features

- **Password Hashing**: All passwords are hashed using SHA-256
- **Data Encryption**: User data is encrypted using AES-GCM
- **Session Management**: Secure session handling with timeouts
- **Input Validation**: All user inputs are validated and sanitized

## Browser Support

- Chrome 60+
- Firefox 55+
- Safari 12+
- Edge 79+

## Deployment

### GitHub Pages

1. Push your code to a GitHub repository
2. Go to Settings > Pages
3. Select source branch (usually `main`)
4. Your control panel will be available at `https://yourusername.github.io/repository-name`

### Other Hosting

Upload all files to your web server. The application works as a static website and doesn't require a backend server.

## Configuration

### Customizing Admin Credentials

Admin credentials are managed through external secure storage (GitHub Gist). To update credentials:

1. **Update your GitHub Gist** with new credentials
2. **Update the secure keys URL** in the settings
3. **Clear encrypted data** to force re-authentication

**Security Note**: Never hardcode credentials in the source code.

### Changing Session Timeout

Modify the session timeout in `js/auth.js`:

```javascript
this.sessionTimeout = 30 * 60 * 1000; // 30 minutes
```

### Customizing App Discovery

Modify the `getSampleApps()` function in `js/apps.js` to scan your actual apps directory or integrate with your app management system.

## API Reference

### Global Objects

- `window.liberApps` - Main application instance
- `window.authManager` - Authentication manager
- `window.dashboardManager` - Dashboard manager
- `window.appsManager` - Apps manager
- `window.usersManager` - Users manager
- `window.cryptoManager` - Encryption utilities

### Key Methods

#### Authentication
```javascript
// Login
await authManager.login(username, password);

// Register
await authManager.register(username, email, password);

// Logout
authManager.logout();

// Check if user is admin
authManager.isAdmin();
```

#### Apps Management
```javascript
// Get all apps
const apps = await appsManager.getApps();

// Launch an app
appsManager.launchApp(appId);

// Get app info
const app = appsManager.getAppById(appId);
```

#### User Management (Admin Only)
```javascript
// Add new user
await authManager.addUser(userData);

// Delete user
await authManager.deleteUser(username);

// Get all users
const users = await authManager.getUsers();
```

## Keyboard Shortcuts

- `Ctrl/Cmd + K` - Focus search
- `Ctrl/Cmd + L` - Logout
- `Ctrl/Cmd + R` - Refresh dashboard
- `Escape` - Close modals
- `1-4` - Navigate sections (Overview, Apps, Users, Settings)

## Troubleshooting

### Common Issues

1. **Apps not loading**: Check browser console for errors
2. **Login not working**: Clear browser cache and localStorage
3. **Styling issues**: Ensure all CSS files are loaded
4. **Encryption errors**: Check browser compatibility

### Debug Mode

Open browser console and check for error messages. The application logs detailed information for debugging.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## License

This project is open source and available under the MIT License.

## Support

For support or questions:
- Create an issue on GitHub
- Check the browser console for error messages
- Ensure all files are properly loaded

## Changelog

### Version 1.0.0
- Initial release
- Basic authentication system
- App launcher functionality
- User management
- Responsive design
- Data encryption

---

**Liber Apps Control Panel** - A modern way to manage your applications.
