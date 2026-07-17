#!/usr/bin/env sh
# torx installer
# supports: Linux (x86_64, aarch64), macOS (x86_64, arm64), Windows (x86_64) via Git Bash / WSL

set -e

REPO="flippedpanst/torx"
BINARY="torx"
INSTALL_DIR=""
VERSION=""

# ─────────────────────────────────────────────
# colours
# ─────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
RESET='\033[0m'

info()    { printf "${CYAN}[torx]${RESET} %s\n" "$1"; }
success() { printf "${GREEN}[torx]${RESET} %s\n" "$1"; }
warn()    { printf "${YELLOW}[torx]${RESET} %s\n" "$1"; }
error()   { printf "${RED}[torx] error:${RESET} %s\n" "$1" >&2; exit 1; }

# ─────────────────────────────────────────────
# detect OS and architecture
# ─────────────────────────────────────────────
detect_platform() {
    OS="$(uname -s 2>/dev/null || echo "unknown")"
    ARCH="$(uname -m 2>/dev/null || echo "unknown")"

    case "$OS" in
        Linux*)
            case "$ARCH" in
                x86_64)  PLATFORM="x86_64-unknown-linux-gnu" ;;
                aarch64) PLATFORM="aarch64-unknown-linux-gnu" ;;
                armv7l)  PLATFORM="armv7-unknown-linux-gnueabihf" ;;
                *)       error "unsupported Linux architecture: $ARCH" ;;
            esac
            EXT="tar.gz"
            DEFAULT_INSTALL_DIR="/usr/local/bin"
            ;;
        Darwin*)
            case "$ARCH" in
                x86_64)          PLATFORM="x86_64-apple-darwin" ;;
                arm64 | aarch64) PLATFORM="aarch64-apple-darwin" ;;
                *)               error "unsupported macOS architecture: $ARCH" ;;
            esac
            EXT="tar.gz"
            DEFAULT_INSTALL_DIR="/usr/local/bin"
            ;;
        MINGW* | MSYS* | CYGWIN*)
            error "for native Windows, use the PowerShell installer instead:
  irm https://raw.githubusercontent.com/$REPO/main/install.ps1 | iex"
            ;;
        *)
            error "unsupported OS: $OS. Please build from source: https://github.com/$REPO"
            ;;
    esac

    info "detected platform: $PLATFORM"
}

# ─────────────────────────────────────────────
# fetch latest release tag from GitHub
# ─────────────────────────────────────────────
fetch_latest_version() {
    if [ -n "$VERSION" ]; then
        info "using specified version: $VERSION"
        return
    fi

    info "fetching latest release..."

    if command -v curl >/dev/null 2>&1; then
        VERSION=$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest" \
            | grep '"tag_name"' \
            | sed -E 's/.*"tag_name": *"([^"]+)".*/\1/')
    elif command -v wget >/dev/null 2>&1; then
        VERSION=$(wget -qO- "https://api.github.com/repos/$REPO/releases/latest" \
            | grep '"tag_name"' \
            | sed -E 's/.*"tag_name": *"([^"]+)".*/\1/')
    else
        error "curl or wget is required to download torx"
    fi

    [ -z "$VERSION" ] && error "could not determine latest version. check https://github.com/$REPO/releases"
    info "latest version: $VERSION"
}

# ─────────────────────────────────────────────
# download and extract
# ─────────────────────────────────────────────
download_and_extract() {
    ARCHIVE="torx-${VERSION}-${PLATFORM}.${EXT}"
    URL="https://github.com/$REPO/releases/download/$VERSION/$ARCHIVE"
    TMP_DIR="$(mktemp -d)"

    info "downloading $ARCHIVE..."

    if command -v curl >/dev/null 2>&1; then
        curl -fsSL --progress-bar "$URL" -o "$TMP_DIR/$ARCHIVE" \
            || error "download failed. check https://github.com/$REPO/releases"
    elif command -v wget >/dev/null 2>&1; then
        wget -q --show-progress "$URL" -O "$TMP_DIR/$ARCHIVE" \
            || error "download failed. check https://github.com/$REPO/releases"
    fi

    info "extracting..."

    case "$EXT" in
        tar.gz)
            tar -xzf "$TMP_DIR/$ARCHIVE" -C "$TMP_DIR"
            ;;
        zip)
            if command -v unzip >/dev/null 2>&1; then
                unzip -q "$TMP_DIR/$ARCHIVE" -d "$TMP_DIR"
            else
                error "unzip is required on Windows (Git Bash). install it or extract manually."
            fi
            ;;
    esac

    # find the binary (may be nested in a folder inside the archive)
    BINARY_PATH="$(find "$TMP_DIR" -name "$BINARY" -type f | head -n 1)"
    [ -z "$BINARY_PATH" ] && error "binary '$BINARY' not found in archive"

    chmod +x "$BINARY_PATH"
    echo "$BINARY_PATH"
}

# ─────────────────────────────────────────────
# install binary
# ─────────────────────────────────────────────
install_binary() {
    BINARY_PATH="$1"
    INSTALL_DIR="${INSTALL_DIR:-$DEFAULT_INSTALL_DIR}"

    # create install dir if it doesn't exist
    if [ ! -d "$INSTALL_DIR" ]; then
        mkdir -p "$INSTALL_DIR" 2>/dev/null || {
            warn "could not create $INSTALL_DIR, trying with sudo..."
            sudo mkdir -p "$INSTALL_DIR"
        }
    fi

    TARGET="$INSTALL_DIR/$BINARY"

    # try without sudo first, fall back to sudo
    if cp "$BINARY_PATH" "$TARGET" 2>/dev/null; then
        success "installed torx to $TARGET"
    else
        warn "permission denied, trying with sudo..."
        sudo cp "$BINARY_PATH" "$TARGET"
        success "installed torx to $TARGET (with sudo)"
    fi
}

# ─────────────────────────────────────────────
# check PATH and print shell instructions
# ─────────────────────────────────────────────
check_path() {
    INSTALL_DIR="${INSTALL_DIR:-$DEFAULT_INSTALL_DIR}"

    if echo ":$PATH:" | grep -q ":$INSTALL_DIR:"; then
        success "torx is ready — run: torx"
    else
        warn "$INSTALL_DIR is not in your PATH"
        echo ""
        echo "  add it by running one of:"
        echo ""
        echo "  ${BOLD}bash/zsh:${RESET}"
        echo "    echo 'export PATH=\"$INSTALL_DIR:\$PATH\"' >> ~/.bashrc && source ~/.bashrc"
        echo ""
        echo "  ${BOLD}zsh only:${RESET}"
        echo "    echo 'export PATH=\"$INSTALL_DIR:\$PATH\"' >> ~/.zshrc && source ~/.zshrc"
        echo ""
        echo "  ${BOLD}fish:${RESET}"
        echo "    fish_add_path $INSTALL_DIR"
        echo ""
    fi
}

# ─────────────────────────────────────────────
# verify installation
# ─────────────────────────────────────────────
verify() {
    INSTALL_DIR="${INSTALL_DIR:-$DEFAULT_INSTALL_DIR}"
    if "$INSTALL_DIR/$BINARY" --version >/dev/null 2>&1; then
        VER=$("$INSTALL_DIR/$BINARY" --version 2>&1)
        success "verified: $VER"
    else
        warn "installed but could not verify — run '$BINARY --version' to check"
    fi
}

# ─────────────────────────────────────────────
# parse args
# ─────────────────────────────────────────────
parse_args() {
    while [ "$#" -gt 0 ]; do
        case "$1" in
            --version|-v)
                VERSION="$2"
                shift 2
                ;;
            --install-dir|-d)
                INSTALL_DIR="$2"
                shift 2
                ;;
            --help|-h)
                echo "torx installer"
                echo ""
                echo "usage: install.sh [options]"
                echo ""
                echo "options:"
                echo "  --version,     -v <tag>   install a specific version (e.g. v0.1.0)"
                echo "  --install-dir, -d <path>  install to a custom directory"
                echo "  --help,        -h          show this help"
                echo ""
                echo "examples:"
                echo "  curl -fsSL https://raw.githubusercontent.com/$REPO/main/install.sh | sh"
                echo "  curl -fsSL https://raw.githubusercontent.com/$REPO/main/install.sh | sh -s -- --version v0.2.0"
                echo "  curl -fsSL https://raw.githubusercontent.com/$REPO/main/install.sh | sh -s -- --install-dir ~/.local/bin"
                exit 0
                ;;
            *)
                error "unknown argument: $1. run with --help for usage."
                ;;
        esac
    done
}

# ─────────────────────────────────────────────
# main
# ─────────────────────────────────────────────
main() {
    echo ""
    printf "${BOLD}${CYAN}torx${RESET} — BitTorrent client installer\n"
    echo "────────────────────────────────────────"
    echo ""

    parse_args "$@"
    detect_platform
    fetch_latest_version
    BINARY_PATH="$(download_and_extract)"
    install_binary "$BINARY_PATH"
    check_path
    verify

    echo ""
    success "done! start downloading:"
    echo "  torx <path/to/file.torrent>"
    echo ""
}

main "$@"