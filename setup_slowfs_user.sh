#!/bin/bash

# SlowFS User Setup Script
# This script converts a user's home directory to use slowfs for IO limiting

set -e  # Exit on any error

# Configuration
SLOWFS_BINARY="./slowfs-cli"
CONFIG_FILE="ideal50mb_config.json"
CONFIG_NAME="ideal50mb"
SECURE_BASE_DIR="/home/.slowfs"

# Function to check if running as root
check_root() {
    if [[ $EUID -ne 0 ]]; then
        echo "ERROR: This script must be run as root (use sudo)"
        exit 1
    fi
}

# Function to check if user exists
check_user_exists() {
    local username="$1"
    if ! id "$username" &>/dev/null; then
        echo "ERROR: User '$username' does not exist"
        exit 1
    fi
}

# Function to check if slowfs binary exists
check_slowfs_binary() {
    if [[ ! -f "$SLOWFS_BINARY" ]]; then
        echo "ERROR: SlowFS binary not found at $SLOWFS_BINARY"
        echo "Please compile slowfs first: go build -o slowfs main.go"
        exit 1
    fi
    
    if [[ ! -x "$SLOWFS_BINARY" ]]; then
        echo "ERROR: SlowFS binary is not executable"
        exit 1
    fi
}

# Function to check if config file exists
check_config_file() {
    if [[ ! -f "$CONFIG_FILE" ]]; then
        echo "ERROR: Config file not found at $CONFIG_FILE"
        exit 1
    fi
}

# Function to get user information
get_user_info() {
    local username="$1"
    USER_HOME=$(eval echo "~$username")
    USER_UID=$(id -u "$username")
    USER_GID=$(id -g "$username")
    
    echo "User: $username"
    echo "Home: $USER_HOME" 
    echo "UID: $USER_UID, GID: $USER_GID"
}

# Function to check if user home is already converted
check_already_converted() {
    local username="$1"
    local secure_path="$SECURE_BASE_DIR/$username"
    
    if [[ -d "$secure_path" ]]; then
        echo "WARNING: User '$username' appears to already be converted to slowfs"
        echo "Secure directory exists: $secure_path"
        read -p "Do you want to continue anyway? [y/N]: " -r
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo "Operation cancelled"
            exit 0
        fi
    fi
}

# Function to setup slowfs for user
setup_slowfs() {
    local username="$1"
    local mount_point="$USER_HOME"
    
    echo "Setting up slowfs for user '$username'..."
    
    # Create systemd service file
    local service_file="/etc/systemd/system/slowfs-$username.service"
    echo "Creating systemd service: $service_file"
    
    cat > "$service_file" << EOF
[Unit]
Description=SlowFS for user $username
After=local-fs.target
Requires=local-fs.target

[Service]
Type=simple
User=root
Group=root
ExecStart=$(pwd)/$SLOWFS_BINARY --config-file $(pwd)/$CONFIG_FILE --config-name $CONFIG_NAME --backing-dir $mount_point --mount-dir $mount_point --secure-mode
ExecStop=/bin/fusermount -u $mount_point
Restart=on-failure
RestartSec=5
KillMode=process
TimeoutStopSec=10

[Install]
WantedBy=multi-user.target
EOF

    # Reload systemd and enable service
    echo "Enabling and starting slowfs service for $username"
    systemctl daemon-reload
    systemctl enable "slowfs-$username.service"
    systemctl start "slowfs-$username.service"
    
    # Wait a moment for mount to complete
    sleep 2
    
    # Verify mount
    if mountpoint -q "$mount_point"; then
        echo "SUCCESS: SlowFS successfully mounted at $mount_point"
        echo "Service status:"
        systemctl status "slowfs-$username.service" --no-pager -l
    else
        echo "ERROR: Failed to mount slowfs at $mount_point"
        echo "Service logs:"
        journalctl -u "slowfs-$username.service" --no-pager -l --since="1 minute ago"
        exit 1
    fi
}

# Function to show usage
show_usage() {
    cat << EOF
Usage: $0 [OPTIONS] <username>

Convert a user's home directory to use slowfs for IO limiting.

Options:
    -c, --config FILE     Use custom config file (default: $CONFIG_FILE)
    -n, --config-name NAME Use custom config name (default: $CONFIG_NAME)
    -r, --remove         Remove slowfs for user
    -h, --help           Show this help message

Examples:
    $0 user1                    # Convert user1's home with default config
    $0 -c custom.json user2     # Convert user2's home with custom config
    $0 --config-name hdd25mb user3  # Use different config from same file
    $0 --remove user1           # Remove slowfs for user1

Requirements:
    - Must run as root
    - SlowFS binary must exist at $SLOWFS_BINARY
    - Config file must exist
    - User must exist on system
EOF
}

# Function to remove slowfs for user (cleanup)
remove_slowfs() {
    local username="$1"
    local service_name="slowfs-$username.service"
    
    echo "Removing slowfs for user '$username'..."
    
    # Stop and disable service
    if systemctl is-active --quiet "$service_name"; then
        echo "Stopping service $service_name"
        systemctl stop "$service_name"
    fi
    
    if systemctl is-enabled --quiet "$service_name"; then
        echo "Disabling service $service_name"
        systemctl disable "$service_name"
    fi
    
    # Remove service file
    local service_file="/etc/systemd/system/$service_name"
    if [[ -f "$service_file" ]]; then
        echo "Removing service file $service_file"
        rm "$service_file"
        systemctl daemon-reload
    fi
    
    echo "SUCCESS: SlowFS removed for user '$username'"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -c|--config)
            CONFIG_FILE="$2"
            shift 2
            ;;
        -n|--config-name)
            CONFIG_NAME="$2"
            shift 2
            ;;
        -r|--remove)
            REMOVE_MODE=1
            shift
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        -*)
            echo "ERROR: Unknown option: $1"
            show_usage
            exit 1
            ;;
        *)
            if [[ -z "$USERNAME" ]]; then
                USERNAME="$1"
            else
                echo "ERROR: Multiple usernames provided"
                show_usage
                exit 1
            fi
            shift
            ;;
    esac
done

# Check if username was provided
if [[ -z "$USERNAME" ]]; then
    echo "ERROR: Username is required"
    show_usage
    exit 1
fi

# Main execution
main() {
    echo "SlowFS User Setup Script"
    echo "========================"
    
    # Perform checks
    check_root
    check_user_exists "$USERNAME"
    
    if [[ "$REMOVE_MODE" == "1" ]]; then
        remove_slowfs "$USERNAME"
        exit 0
    fi
    
    check_slowfs_binary
    check_config_file
    get_user_info "$USERNAME"
    check_already_converted "$USERNAME"
    
    # Confirm before proceeding
    echo "WARNING: This will convert user '$USERNAME' home directory to use slowfs"
    echo "Home directory: $USER_HOME"
    echo "Config: $CONFIG_FILE ($CONFIG_NAME)"
    read -p "Do you want to continue? [y/N]: " -r
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Operation cancelled"
        exit 0
    fi
    
    # Setup slowfs
    setup_slowfs "$USERNAME"
    
    echo "SUCCESS: SlowFS setup completed for user '$USERNAME'"
    echo "The user's home directory is now limited by slowfs"
    echo "To remove: $0 --remove $USERNAME"
    echo "To check status: systemctl status slowfs-$USERNAME.service"
}

# Run main function
main "$@"