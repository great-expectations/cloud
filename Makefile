.PHONY: agent help

help:
	@echo "Available commands:"
	@echo " make agent token=<your-token> [org=<org_id>] [profile=true] [dev=true] - Run gx-runner with specified token"
	@echo "\t\ttoken (required)   | mercury API token"
	@echo "\t\torg                | attach the agent to a particular org, connects to default if omitted"
	@echo "\t\tprofile            | install and run memray to spawn a profiling session for the runner"
	@echo "\t\tdev                | connect to https://api.dev.greatexpectations.io instead of localhost:7000"

agent:
	@if [ -z "$(token)" ]; then \
  		echo "Error: token is required. Usage: make runner token=<your-token> [profile=true] [dev=true]"; \
  		exit 1; \
	fi; \
	chmod +x ./scripts/run-local.sh; \
	args="-t $(token)"; \
	if [ -n "$(org)" ]; then \
		args="$$args -o $(org)"; \
	fi; \
	if [ "$(profile)" = "true" ]; then \
		args="$$args -p"; \
	fi; \
	if [ "$(dev)" = "true" ]; then \
	  	args="$$args -d"; \
	fi; \
	./scripts/run-local.sh $$args
