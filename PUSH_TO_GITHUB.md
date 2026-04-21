# GitHub Upload Guide

Use this after creating an empty repository on GitHub.

## 1. Open terminal in this folder

## 2. Initialize Git

```bash
git init
git add .
git commit -m "Initial commit: oil narrative engine"
```

## 3. Connect your GitHub repository

Replace the URL below with your own repository URL.

```bash
git branch -M main
git remote add origin <YOUR_GITHUB_REPO_URL>
git push -u origin main
```

## Example

```bash
git remote add origin https://github.com/yourname/oil-narrative-engine.git
git push -u origin main
```
