#!/bin/bash

# Script to set up git hooks for the repository

echo "🔧 Setting up git hooks..."

# Option 1: Use pre-commit framework (recommended)
if command -v pre-commit &> /dev/null; then
    echo "📦 Installing pre-commit hooks..."
    pre-commit install
    echo "✅ Pre-commit hooks installed! Hooks will run automatically on commit."
    echo "💡 You can run 'pre-commit run --all-files' to check all files manually."
elif command -v python3 &> /dev/null || command -v python &> /dev/null; then
    echo "📦 Installing pre-commit framework..."
    if command -v pip3 &> /dev/null; then
        pip3 install pre-commit
    elif command -v pip &> /dev/null; then
        pip install pre-commit
    else
        echo "❌ pip not found. Please install pre-commit manually: https://pre-commit.com/#installation"
        exit 1
    fi
    pre-commit install
    echo "✅ Pre-commit hooks installed!"
else
    # Option 2: Use simple git hooks
    echo "🪝 Setting up simple git hooks (pre-commit not available)..."
    
    # Set up git hooks directory
    git config core.hooksPath .githooks
    
    echo "✅ Git hooks configured!"
    echo "💡 Run 'npm run format' to manually format all files."
fi

echo ""
echo "🎨 Available formatting commands:"
echo "  npm run format      - Format all TypeScript/JS + Rust files"
echo "  npm run lint:fix    - Run ESLint with auto-fix"
echo "  cargo fmt --all     - Format only Rust files"
echo "  npx prettier --write . - Format only TypeScript/JS files"
echo ""
echo "✨ Setup complete! Your commits will now be automatically formatted."