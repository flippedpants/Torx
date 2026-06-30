#!/usr/bin/env bash

set -e

echo "====================================="
echo "       Welcome to the Torx Installer "
echo "====================================="
echo ""

# OS Detection
OS="$(uname -s)"
case "${OS}" in
    Linux*)     OS_TYPE="linux";;
    Darwin*)    OS_TYPE="mac";;
    CYGWIN*|MINGW32*|MSYS*|MINGW*) OS_TYPE="windows";;
    *)          OS_TYPE="unknown";;
esac

echo "Detected Operating System: $OS_TYPE"
if [ "$OS_TYPE" = "unknown" ]; then
    echo "Unsupported OS: $OS. Exiting."
    exit 1
fi

# The user specified a single cross-platform binary named "Torx"
GITHUB_BINARY="Torx"

if [ "$OS_TYPE" = "windows" ]; then
    LOCAL_BINARY="torx.exe"
else
    LOCAL_BINARY="torx"
fi

DOWNLOAD_URL="https://github.com/flippedpants/Torx/releases/latest/download/$GITHUB_BINARY"

echo ""
read -p "Do you want to proceed with downloading and installing Torx? (y/N): " proceed
if [[ "$proceed" != "y" && "$proceed" != "Y" ]]; then
    echo "Installation cancelled."
    exit 0
fi

# Check for curl
if ! command -v curl &> /dev/null; then
    echo "Error: 'curl' is required but not installed. Please install curl and try again."
    exit 1
fi

echo ""
echo "Downloading Torx..."
echo "URL: $DOWNLOAD_URL"

# Download the binary to a temporary directory
TMP_DIR="$(mktemp -d)"
TMP_BIN="$TMP_DIR/$LOCAL_BINARY"

if ! curl -f -L "$DOWNLOAD_URL" -o "$TMP_BIN"; then
    echo "Error: Failed to download the binary. Please check if the release exists on GitHub."
    rm -rf "$TMP_DIR"
    exit 1
fi

chmod +x "$TMP_BIN"

echo ""
if [ "$OS_TYPE" = "windows" ]; then
    DEFAULT_INSTALL_DIR="$HOME/AppData/Local/Microsoft/WindowsApps"
else
    DEFAULT_INSTALL_DIR="/usr/local/bin"
fi

read -p "Where would you like to install Torx? [Default: $DEFAULT_INSTALL_DIR]: " custom_dir

INSTALL_DIR="${custom_dir:-$DEFAULT_INSTALL_DIR}"

# Check if we need to create the directory
if [ ! -d "$INSTALL_DIR" ]; then
    read -p "Directory $INSTALL_DIR does not exist. Do you want to create it? (y/N): " create_dir
    if [[ "$create_dir" == "y" || "$create_dir" == "Y" ]]; then
        if [ "$OS_TYPE" = "windows" ]; then
            mkdir -p "$INSTALL_DIR" || { echo "Failed to create directory."; rm -rf "$TMP_DIR"; exit 1; }
        else
            mkdir -p "$INSTALL_DIR" 2>/dev/null || sudo mkdir -p "$INSTALL_DIR" || { echo "Failed to create directory. Try running with sudo."; rm -rf "$TMP_DIR"; exit 1; }
        fi
    else
        echo "Installation cancelled."
        rm -rf "$TMP_DIR"
        exit 1
    fi
fi

echo ""
echo "Installing Torx to $INSTALL_DIR..."

if [ "$OS_TYPE" = "windows" ]; then
    cp "$TMP_BIN" "$INSTALL_DIR/$LOCAL_BINARY"
else
    # Try to copy without sudo first
    if ! cp "$TMP_BIN" "$INSTALL_DIR/$LOCAL_BINARY" 2>/dev/null; then
        echo "Permission denied. Attempting to install with sudo..."
        sudo cp "$TMP_BIN" "$INSTALL_DIR/$LOCAL_BINARY"
    fi
fi

# Clean up temporary files
rm -rf "$TMP_DIR"

echo "====================================="
echo " Torx successfully installed!        "
echo " You can now run 'torx' in your terminal."
echo "====================================="
