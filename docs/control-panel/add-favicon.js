#!/usr/bin/env node

/**
 * Script to automatically add favicon to HTML files
 * Usage: node add-favicon.js [directory]
 */

const fs = require('fs');
const path = require('path');

// Default favicon paths relative to the HTML file
const faviconPaths = {
    root: {
        png: 'images/LIBER LOGO.png',
        svg: 'images/LIBER LOGO.svg'
    },
    apps: {
        png: '../images/LIBER LOGO.png',
        svg: '../images/LIBER LOGO.svg'
    }
};

function addFaviconToFile(filePath) {
    try {
        const content = fs.readFileSync(filePath, 'utf8');
        
        // Check if favicon already exists
        if (content.includes('rel="icon"')) {
            console.log(`✅ Favicon already exists in ${filePath}`);
            return;
        }
        
        // Determine the relative path for favicon
        const isInAppsDir = filePath.includes('/apps/') || filePath.includes('\\apps\\');
        const faviconPath = isInAppsDir ? faviconPaths.apps : faviconPaths.root;
        
        // Find the title tag and add favicon after it
        const titleMatch = content.match(/<title>.*?<\/title>/);
        if (!titleMatch) {
            console.log(`⚠️  No title tag found in ${filePath}`);
            return;
        }
        
        const faviconLinks = `\n    <link rel="icon" type="image/png" href="${faviconPath.png}">\n    <link rel="icon" type="image/svg+xml" href="${faviconPath.svg}">`;
        
        const newContent = content.replace(
            titleMatch[0],
            titleMatch[0] + faviconLinks
        );
        
        fs.writeFileSync(filePath, newContent, 'utf8');
        console.log(`✅ Added favicon to ${filePath}`);
        
    } catch (error) {
        console.error(`❌ Error processing ${filePath}:`, error.message);
    }
}

function findHtmlFiles(dir) {
    const files = [];
    
    function scanDirectory(currentDir) {
        const items = fs.readdirSync(currentDir);
        
        for (const item of items) {
            const fullPath = path.join(currentDir, item);
            const stat = fs.statSync(fullPath);
            
            if (stat.isDirectory()) {
                scanDirectory(fullPath);
            } else if (item.endsWith('.html')) {
                files.push(fullPath);
            }
        }
    }
    
    scanDirectory(dir);
    return files;
}

function main() {
    const targetDir = process.argv[2] || '.';
    
    if (!fs.existsSync(targetDir)) {
        console.error(`❌ Directory ${targetDir} does not exist`);
        process.exit(1);
    }
    
    console.log(`🔍 Scanning for HTML files in ${targetDir}...`);
    const htmlFiles = findHtmlFiles(targetDir);
    
    if (htmlFiles.length === 0) {
        console.log('No HTML files found');
        return;
    }
    
    console.log(`Found ${htmlFiles.length} HTML file(s):`);
    htmlFiles.forEach(file => {
        console.log(`  - ${file}`);
    });
    
    console.log('\n📝 Adding favicons...');
    htmlFiles.forEach(addFaviconToFile);
    
    console.log('\n✅ Done!');
}

if (require.main === module) {
    main();
}

module.exports = { addFaviconToFile, findHtmlFiles };
