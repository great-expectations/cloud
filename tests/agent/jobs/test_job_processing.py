import os
import time

import pytest
import requests

org_id = os.getenv("GX_CLOUD_ORGANIZATION_ID")
token = os.getenv("GX_CLOUD_ACCESS_TOKEN")
gql_url = url = f"http://localhost:5000/organizations/{org_id}/graphql"


@pytest.fixture(scope="session")
def wait_for_docker_compose():
    # pretest setup
    get_agent_status_body = """
        query agent_status {
            agentStatus {
            active
            __typename
            }
        }
    """
    response = requests.post(
        url=url,
        json={"query": get_agent_status_body},
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )
    response.raise_for_status()

    if not response.json()["data"]["agentStatus"]["active"]:
        return
    ## check here to see if the agent is online

    yield
    print("Post pytest test_job_processing teardown")
    # posttest teardown


def test_job_processing(wait_for_docker_compose):
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
        url=gql_url,
        json={"query": body, "variables": variables},
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
    )
    response.raise_for_status()
    jobId = response.json()["data"]["createRunCheckpointJob"]["jobId"]

    for i in range(10):
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
            url=url,
            json={"query": get_job_by_id_body, "variables": get_job_by_id_variables},
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            },
        )
        response.raise_for_status()

        for job in response.json()["data"]["jobs"]:
            if job["id"] == jobId:
                # Check to ensure job completed and was error free
                if job["status"] == "complete" and job["errorMessage"] is None:
                    return

        time.sleep(i * 2)

    raise Exception("Job failed")
