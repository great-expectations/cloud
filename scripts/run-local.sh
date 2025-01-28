function is_poetry_shell() {
  [[ -n "$POETRY_ACTIVE" ]]
}

function gx_agent() {

    if ! is_poetry_shell; then
      echo '"make runner" must be run from within a poetry shell.'
      exit 1
    fi

    pip install --upgrade pip 1> /dev/null

    # Initialize variables
    local token=""
    local profile=false
    local dev=false
    local org=false
    local org_id=""

    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case "$1" in
            -t|--token)
                token=true
                shift
                ;;
            -p|--profile)
                profile=true
                shift
                ;;
            -d|--dev)
                dev=true
                shift
                ;;
            -o|--org)
                org=true
                shift
                ;;
            *)
                if [[ $token == true && -z "$GX_CLOUD_ACCESS_TOKEN" ]]; then
                    export GX_CLOUD_ACCESS_TOKEN="$1"
                elif [[ $org == true && -z "$org_id" ]]; then
                    org_id="$1"
                fi
                shift
                ;;
        esac
    done

    if [[ $token == true ]]; then
        if [[ $dev == true ]]; then
            echo "connecting to dev\c"
            export GX_CLOUD_BASE_URL="https://api.dev.greatexpectations.io"
        else
            echo "connecting to local\c"
            export GX_CLOUD_BASE_URL="http://localhost:7000"
        fi

        if [[ $org == true ]]; then
            echo " with org id \"$org_id\""
            export GX_CLOUD_ORGANIZATION_ID="$org_id"
        else
            echo ""
            export GX_CLOUD_ORGANIZATION_ID="0ccac18e-7631-4bdd-8a42-3c35cce574c6"
        fi

        if [[ $profile == true ]]; then
            if ! pip list | grep -q memray; then
                echo "installing memray to initiate profiling session..."
                pip install memray 1>/dev/null
            fi
            python -m memray run --live-remote great_expectations_cloud/agent/cli.py
        else
            python great_expectations_cloud/agent/cli.py
        fi
    fi
}

gx_agent "$@"
