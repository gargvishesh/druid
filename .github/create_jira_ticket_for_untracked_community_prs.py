import os
import re
from datetime import datetime, timedelta
from jira import JIRA
from github import Github

options = {"server": os.environ["JIRA_BASE_URL"]}
jira = JIRA(options, basic_auth=(os.environ["JIRA_USER_EMAIL"], os.environ["JIRA_API_TOKEN"]))


def imply_authors():
    imply_pr_authors = ["dependabot[bot]"]
    g = Github(os.environ["GITHUB_TOKEN"])
    repo = g.get_repo("implydata/druid")

    for user in repo.get_collaborators():
        imply_pr_authors.append(user.login)
    return imply_pr_authors


def filter_pr_with_no_jira(prs):
    no_jira_prs = []
    past = datetime.now() - timedelta(days=1)  # datetime object for 24 hours before
    for pr in prs:
        if pr.created_at < past:
            break  # stop parsing the sorted listed if created_at is olders than past
        if pr.user.login not in imply_authors():
            search_string = f'"\\"Community Ticket - (#{pr.number})\\""'  # Adding quotes to string for exact search.
            search_result = jira.search_issues(jql_str=f'summary ~ {search_string}')
            if not search_result:
                no_jira_prs = no_jira_prs + [pr]
        if len(no_jira_prs) == 5: # Maximum of 5 PRs will be created in one run.
            break
    return no_jira_prs


def process_desc(desc_str):
    desc_str = desc_str.rsplit('This PR has:', 1)[0]  # Removing the checkboxes at the end
    desc_str = re.sub(r'(?s)<!--.*?-->', '', desc_str)  # Removing commented text.
    desc_str = re.sub(r'#|<hr>', '', desc_str)  # Removing '#' chars to skip unnecessary indentation in jira desc.
    if len(desc_str) >= 20000:  # Keep length of description less than 20K chars
        desc_str = desc_str[:20000]
    return desc_str


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
            summary=f'Community PR Ticket - (#{pr.number}) {pr.title}',
            description=f'Apache druid Community PR: [{pr.html_url}] \n\n {process_desc(pr.body)}',
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
            customfield_10037=f'{pr.html_url}'
        )
        issues.append(f'https://implydata.atlassian.net/browse/{issue.key}')
    return issues


def main():
    g = Github()
    repo = g.get_repo("apache/druid")
    PRs = repo.get_pulls(state='open', sort='created', base='master', direction='desc')
    return create_jira_for_prs(filter_pr_with_no_jira(PRs))


if __name__ == "__main__":
    print(main())
