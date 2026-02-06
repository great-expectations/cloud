from __future__ import annotations

from enum import StrEnum


class CreatedResourceTypes(StrEnum):
    EXPECTATION = "Expectation"
    EXPECTATION_DRAFT_CONFIG = "ExpectationDraftConfig"


# CREATED_VIA indicates the source of expectations created via actions.
# The possible options are determined by the mercury API and can be examined in our openapi docs.
# If you have mercury-v1 running, you can see the allowed values at this URL:
# http://localhost:7000/#/protected/create_expectation
# And looking at the request body schema.
CREATED_VIA_ECHOES = "echoes"
CREATED_VIA_PROMPT = "echoes.prompt"
