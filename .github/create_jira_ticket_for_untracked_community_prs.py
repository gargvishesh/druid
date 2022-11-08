import os
import re
import requests
from jira import JIRA
from github import Github
import os

options = {"server": os.environ["JIRA_BASE_URL"]}
jira = JIRA(options, basic_auth=(os.environ["JIRA_USER_EMAIL"], os.environ["JIRA_API_TOKEN"]))


def imply_authors():
    imply_authors = []
    g = Github(os.environ["GITHUB_TOKEN"])
    repo = g.get_repo("implydata/druid")

    for user in repo.get_collaborators():
        imply_authors.append(user.login)
    return imply_authors


def escaped_jql_str(str):
    reserved_chars = '''?&|!{}[]()^~*:\\"'+-'''
    # make trans table
    replace = ['' for l in reserved_chars]
    trans = str.maketrans(dict(zip(reserved_chars, replace)))
    return str.translate(trans)


def find_next_url(links):
    for link in links:
        link = link.strip()
        if link.find("rel=\"next\"") >= 0:
            match_result = re.match("<https.*>", link)
            if match_result is None:
                raise Exception("Next link[{}] is found but can't find url".format(link))
            else:
                url_holder = match_result.group(0)
                return url_holder[1:-1]
    return None


def filter_pr_with_no_jira(pr_jsons):
    prs = []
    for pr in pr_jsons:
        if pr["user"]["login"] not in imply_authors():
            search_string = escaped_jql_str(f'Community Ticket - (#{pr["number"]}) {pr["title"]}')
            search_result = jira.search_issues(jql_str=f'summary ~ "{search_string}"')
            if not search_result:
                prs = prs + [pr]
    return prs


def create_jira_for_prs(prs):
    issues = []
    for pr in prs:
        issue = jira.create_issue(
            project={
                "id": "10005",
                "key": "IMPLY",
                "name": "Imply"
            },
            issuetype={
                "id": "10010",
                "name": "Task",
                "subtask": False
            },
            labels=["community"],
            summary=f'Community PR Ticket - (#{pr["number"]}) {pr["title"]}',
            description=f'Apache druid Community PR: [{pr["html_url"]}] \n\n {pr["body"]}',
            customfield_10033=[{
                "value": "Druid",
                "id": "10025"
            }],
            priority={
                "name": "Medium",
                "id": "3"
            },
            customfield_10140=[{
                "value": "Druid Systems",
                "id": "10520"
            }],
            customfield_10037=f'{pr["html_url"]}'
        )
        issues.append(f'https://implydata.atlassian.net/browse/{issue.key}')
    return issues


def main():
    # Get all open PRs
    PRs = []
    next_url = "https://api.github.com/repos/apache/druid/pulls"

    while next_url is not None:
        resp = requests.get(next_url)
        PRs = PRs + filter_pr_with_no_jira(resp.json())
        links = resp.headers['Link'].split(',')
        next_url = find_next_url(links)

    return create_jira_for_prs(PRs)


if __name__ == "__main__":
    print(main())