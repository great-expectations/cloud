import pytest
import requests
from tenacity import retry, stop_after_delay

from great_expectations_cloud.agent.config import GxAgentEnvVars


@pytest.fixture(scope="session")
def gx_agent_vars() -> GxAgentEnvVars:
    return GxAgentEnvVars()


@pytest.fixture(scope="session")
def local_gql_url(gx_agent_vars: GxAgentEnvVars) -> str:
    gql_url = (
        f"http://localhost:5000/organizations/{gx_agent_vars.gx_cloud_organization_id}/graphql"
    )
    return gql_url


# Retry for 3 minutes, if agent is not ready then assume something went wrong
@retry(stop=stop_after_delay(180))
def ensure_agent_is_ready(local_gql_url: str, token: str):
    """Throw an HTTP or ConnectionError exception if the GX Agent is not ready"""
    body = """
        query agent_status {
            agentStatus {
            active
            __typename
            }
        }
    """
    response = requests.post(
        url=local_gql_url,
        json={"query": body},
        headers={"Authorization": f"Bearer {token}"},
    )
    response.raise_for_status()

    if not response.json()["data"]["agentStatus"]["active"]:
        raise ConnectionError("Agent is not ready")


@pytest.fixture(scope="session")
def wait_for_docker_compose(local_gql_url: str, gx_agent_vars: GxAgentEnvVars):
    # pretest setup
    ensure_agent_is_ready(local_gql_url, gx_agent_vars.gx_cloud_access_token)
    yield
    print("Post pytest test_job_processing teardown")


@pytest.mark.agentjobs
def test_job_processing(wait_for_docker_compose, local_gql_url: str, gx_agent_vars: GxAgentEnvVars):
    body = """
      mutation createRunCheckpointJob($checkpointId: UUID!) {
        createRunCheckpointJob(checkpointId: $checkpointId) {
          jobId
          __typename
        }
      }
      """

    variables = """
      {"checkpointId": "5f3814d6-a2e2-40f9-ba75-87ddf485c3a8"}
      """

    response = requests.post(
        url=local_gql_url,
        json={"query": body, "variables": variables},
        headers={"Authorization": f"Bearer {gx_agent_vars.gx_cloud_access_token}"},
    )
    response.raise_for_status()
    jobId = response.json()["data"]["createRunCheckpointJob"]["jobId"]

    check_job_status(jobId, local_gql_url, gx_agent_vars)


@retry(stop=stop_after_delay(30))
def check_job_status(jobId: str, local_gql_url: str, gx_agent_vars: GxAgentEnvVars):
    get_job_by_id_body = """
        query getJobs {
        jobs {
            id
            status
            errorMessage
            __typename
        }
        }
    """

    get_job_by_id_variables = f'{{"jobId": "{jobId}"}}'

    response = requests.post(
        url=local_gql_url,
        json={"query": get_job_by_id_body, "variables": get_job_by_id_variables},
        headers={"Authorization": f"Bearer {gx_agent_vars.gx_cloud_access_token}"},
    )
    response.raise_for_status()

    for job in response.json()["data"]["jobs"]:
        if job["id"] == jobId:
            # Check to ensure job completed and was error free
            if job["status"] == "complete":
                assert job["errorMessage"] is None
                break
