#!/bin/bash

set -e

# Show usage
usage() {
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo ""
    echo "Commands:"
    echo "  up      Start the services (default)"
    echo "  down    Stop and remove the services"
    echo ""
    echo "Options for 'up':"
    echo "  --new-license    Prompt for a new license key"
    echo "  -d, --detach     Run in detached mode"
    echo ""
    echo "Examples:"
    echo "  $0               # Start services interactively"
    echo "  $0 up            # Start services interactively"
    echo "  $0 up -d         # Start services in background"
    echo "  $0 up --new-license  # Prompt for new license before starting"
    echo "  $0 down          # Stop and cleanup services"
}

# Parse command
COMMAND="up"
if [ $# -gt 0 ] && [[ ! "$1" =~ ^- ]]; then
    COMMAND="$1"
    shift
fi

# Detect docker or podman early
if command -v docker &> /dev/null; then
    DOCKER_CMD="docker"
elif command -v podman &> /dev/null; then
    DOCKER_CMD="podman"
else
    echo "Error: Neither docker nor podman found. Please install Docker or Podman."
    exit 1
fi

case "$COMMAND" in
    up)
        # Check for --new-license flag
        NEW_LICENSE=false
        REMAINING_ARGS=()
        while [ $# -gt 0 ]; do
            case "$1" in
                --new-license)
                    NEW_LICENSE=true
                    ;;
                *)
                    REMAINING_ARGS+=("$1")
                    ;;
            esac
            shift
        done

        # Check if .env file exists or if --new-license flag was passed
        if [ ! -f .env ] || [ "$NEW_LICENSE" = true ]; then
            if [ "$NEW_LICENSE" = true ] && [ -f .env ]; then
                echo "Replacing existing license key..."
                echo ""
            fi
            echo "==================================================================="
            echo "  Lakekeeper Plus License Key Required"
            echo "==================================================================="
            echo ""
            echo "This example requires a valid Lakekeeper Plus license key."
            echo ""
            echo "To generate a license key, run:"
            echo "  cargo run --bin lakekeeper-license -- \\"
            echo "    generate \\"
            echo "    --customer \"your-customer-name\" \\"
            echo "    --expires \"2026-12-31T23:59:59Z\" \\"
            echo "    --key \"/path/to/your/private/key.pem\" \\"
            echo "    --type evaluation"
            echo ""
            echo "==================================================================="
            echo ""
            
            # Prompt for license key
            read -p "Enter your license key: " LICENSE_KEY
            
            if [ -z "$LICENSE_KEY" ]; then
                echo "Error: License key cannot be empty"
                exit 1
            fi
            
            # Create .env file
            echo "LAKEKEEPER__LICENSE__KEY=$LICENSE_KEY" > .env
            echo ""
            echo "✓ License key saved to .env file"
            echo ""
        fi

        # Run docker compose up
        echo "Starting services..."
        $DOCKER_CMD compose up "${REMAINING_ARGS[@]}"
        ;;
    
    down)
        echo "Stopping and removing services..."
        $DOCKER_CMD compose down "$@"
        ;;
    
    help|--help|-h)
        usage
        exit 0
        ;;
    
    *)
        echo "Error: Unknown command '$COMMAND'"
        echo ""
        usage
        exit 1
        ;;
esac
