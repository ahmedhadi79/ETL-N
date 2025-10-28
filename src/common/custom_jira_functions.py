import logging
import sys
import time
import uuid
from datetime import datetime
import numpy as np
import pandas as pd
from atlassian import Jira


def initialize_log(name='') -> logging.Logger:
    """
    logging function with set level logging output
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logging.basicConfig(
        format="%(asctime)s %(levelname)s:%(message)s",
        level=logging.INFO,
        datefmt="%I:%M:%S",
    )
    return logger


logger = initialize_log()


def jira_api(user_name, url, token):
    "Initials jira api object with secret details"
    jira = Jira(url, username=user_name, password=token, cloud=True)

    return jira


def get_jql_data(user_name, url, token, jql_request, limit):
    """
    Returns iterable response object based on a Jira query
    with a defined number of pages (limit) per iteration
    set to a maximum of 100 and starting page min 0
    """
    jira = jira_api(user_name, url, token)
    jira_response = jira.jql(jql_request, limit=limit)

    return jira_response


def number_of_pages(jira_response, limit):
    """
    returns the total number of iterations
    required to process all the data for a given query
    limit parameter must match the page limit in get_jql_data function
    """
    pages = jira_response["total"] / limit
    if pages % 1 != 0:
        total_pages = round(jira_response["total"] / limit) + 1
    else:
        total_pages = round(jira_response["total"] / limit)

    return total_pages


def jira_bugs_transform(jira_response, total_pages, start_page):
    """
    Takes the bug jira response
    total pages
    starting page
    Transforms the JQL query to a dataframe
    """
    jira_list = []
    for page in range(0, total_pages):
        issues = jira_response.get("issues", [])
        start_page = start_page + 101
        logger.info("iteration %s process jira response page: %s", page, start_page)
        for issue in issues:
            unique_id = f"{str(uuid.uuid4())}_{int(time.time())}"
            key = issue.get("key")
            summary = issue.get("fields").get("summary")
            resolution = issue.get("fields").get("resolution", None)
            resolution = np.nan if resolution is None else resolution.get("name")
            role = issue.get("fields").get("customfield_10742")
            role = np.nan if role is None else role
            assignee = issue.get("fields").get("assignee", None)
            assignee = np.nan if assignee is None else assignee.get("displayName")
            creator = issue.get("fields").get("creator").get("displayName")
            issuetype = issue.get("fields").get("issuetype")
            issuetype = np.nan if issuetype is None else issuetype.get("name")
            status = issue.get("fields").get("status")
            status = np.nan if status is None else status.get("name")
            status_catergory = (
                issue.get("fields").get("status").get("statusCategory").get("name")
            )
            priority = issue.get("fields").get("priority").get("name")
            organisation = issue.get("fields").get("customfield_10273")
            organisation = np.nan if organisation is None else organisation.get("value")
            division = issue.get("fields").get("customfield_10275")
            division = np.nan if division is None else division.get("value")
            squad = issue.get("fields").get("customfield_10001")
            squad = np.nan if squad is None else squad.get("name")
            environment = (
                np.nan
                if issue.get("fields").get("environment") is None
                else issue.get("fields").get("environment")
            )
            device = issue.get("fields").get("customfield_10076")
            device = np.nan if device is None else device.get("value")
            created = issue.get("fields").get("created")
            updated = issue.get("fields").get("updated")

            data = {
                "id": unique_id,
                "key": key,
                "summary": summary,
                "resolution": resolution,
                "role": role,
                "assignee": assignee,
                "creator": creator,
                "issuetype": issuetype,
                "status": status,
                "status_catergory": status_catergory,
                "priority": priority,
                "organisation": organisation,
                "division": division,
                "squad": squad,
                "environment": environment,
                "device": device,
                "created": created,
                "updated": updated,
                "timestamp_extracted": datetime.utcnow().strftime(
                    "%Y-%m-%d %H:%M:%S.%f"
                )[:-1],
            }
            jira_list.append(data)

    df = pd.DataFrame(jira_list)

    return df


def extract_issue_details(issue):
    unique_id = f"{str(uuid.uuid4())}_{int(time.time())}"
    key = issue.get("key")
    summary = issue.get("fields").get("summary")
    resolution = issue.get("fields").get("resolution", None)
    resolution = np.nan if resolution is None else resolution.get("name")
    role = issue.get("fields").get("customfield_10742")
    role = np.nan if role is None else role
    assignee = issue.get("fields").get("assignee", None)
    assignee = np.nan if assignee is None else assignee.get("displayName")
    creator = issue.get("fields").get("creator").get("displayName")
    issuetype = issue.get("fields").get("issuetype")
    issuetype = np.nan if issuetype is None else issuetype.get("name")
    defect_severity = issue.get("fields").get("customfield_10073")
    defect_severity = '' if defect_severity is None else defect_severity.get("value")
    status = issue.get("fields").get("status")
    status = np.nan if status is None else status.get("name")
    status_catergory = (
        issue.get("fields").get("status").get("statusCategory").get("name")
    )
    priority = issue.get("fields").get("priority").get("name")
    organisation = issue.get("fields").get("customfield_10273")
    organisation = np.nan if organisation is None else organisation.get("value")
    division = issue.get("fields").get("customfield_10275")
    division = np.nan if division is None else division.get("value")
    squad = issue.get("fields").get("customfield_10001")
    squad = np.nan if squad is None else squad.get("name")
    development_bug = issue.get("fields").get("customfield_10311", '')
    environment = (
        np.nan
        if issue.get("fields").get("environment") is None
        else issue.get("fields").get("environment")
    )
    device = issue.get("fields").get("customfield_10076")
    device = np.nan if device is None else device.get("value")
    created = issue.get("fields").get("created")
    updated = issue.get("fields").get("updated")
    data = {
        "id": unique_id,
        "key": key,
        "summary": summary,
        "resolution": resolution,
        "role": role,
        "assignee": assignee,
        "creator": creator,
        "issuetype": issuetype,
        "defect_severity": defect_severity,
        "status": status,
        "status_catergory": status_catergory,
        "priority": priority,
        "organisation": organisation,
        "division": division,
        "squad": squad,
        "development_bug": development_bug,
        "environment": environment,
        "device": device,
        "created": created,
        "updated": updated,
        "timestamp_extracted": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"),
    }
    return data
