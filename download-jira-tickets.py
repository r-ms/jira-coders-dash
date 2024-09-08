import pandas as pd
from jira import JIRA
from diskcache import Cache
import json

print("Reading configuration...")
with open("config.json", "r") as f: cfg = json.load(f)

# Connect to JIRA
jira_options = {'server': cfg['jira_server']}
jira = JIRA(options=jira_options, basic_auth=(cfg['jira_user'], cfg['jira_token']))
jira_cache = Cache(directory=".cache")

# Get sprints
print(f"Reading sprint info from board id {cfg['board_id']} ... ", end='')
start_at = 0
max_results = 25
all_sprints = []

while True:
    sprints = jira.sprints(board_id=cfg['board_id'], startAt=start_at, maxResults=max_results)
    all_sprints.extend(sprints)   
    
    start_at += max_results

    if len(sprints) < max_results:
        break
print(f"got {len(all_sprints)} sprints")

# Convert to DataFrame and prepare for future use
sprints_data = pd.json_normalize([sprint.raw for sprint in all_sprints])
sprints_data['startDate'] = pd.to_datetime(sprints_data['startDate'])
sprints_data['endDate'] = pd.to_datetime(sprints_data['endDate'])
sprints_data.loc[sprints_data['completeDate'].isna(), 'completeDate'] = sprints_data.loc[sprints_data['completeDate'].isna(), 'endDate']
jira_cache['sprints_data'] = sprints_data

# Download all issues

print("Downloading issues ... ", end='')
jql_query = f'created >= "{cfg["task_creation_start_date"]}" AND created <= "{cfg["task_creation_end_date"]}" AND project = MTK'# AND status = Done'

# Fetch all issues from the project
start_at = 0
max_results = 100  # Number of issues to fetch per request
all_issues = []

while True:
    issues = jira.search_issues(jql_query, startAt=start_at, maxResults=max_results)
    all_issues.extend(issues)
    
    if len(issues) < max_results:
        break
    
    start_at += max_results

print(f"got {len(all_issues)} issues")
jira_cache['all_issues'] = all_issues

# Create dictionary of titles
jira_cache['issue_titles'] = {x.key: x.fields.summary for x in all_issues}

def download_jira_issue(issue_key, updated):
    cache_key = f"jira_ticket_{issue_key}"
    
    # Try to retrieve the cached object
    cached_issue = jira_cache.get(cache_key)
    
    if cached_issue:
        # Check if the cached object is outdated
        cached_updated = pd.to_datetime(cached_issue.fields.updated)
        if updated and (cached_updated >= updated):
            # Return cached value if it's up-to-date
            return cached_issue
    
    # Fetch ticket from JIRA if not in cache or outdated
    issue = jira.issue(issue_key, expand='changelog')
    
    # Store ticket in cache with the current timestamp
    jira_cache.set(cache_key, issue)
    
    return issue

def get_assignee_status_intervals(issue_key, updated):
    try:
        # print(issue_key)
        # Fetch the issue with its changelog
        issue = download_jira_issue(issue_key=issue_key, updated=updated)

       # default values used in case of missing changelogs
        issue_current_assignee = issue.fields.assignee.displayName if issue.fields.assignee else 'Unassigned'
        issue_current_status = issue.fields.status.name
        issue_created_date = issue.fields.created
        sprints_info = issue.fields.customfield_10010
        issue_current_sprint = ', '.join(sprint.name for sprint in sprints_info) if sprints_info else 'Unassigned'
    
        # Access the changelog
        changelog = issue.changelog

        intervals = []

        count_changes = {
            'assignee': 0,
            'status': 0,
            'Sprint': 0
        }

        # Iterate through the changelog histories
        for history in changelog.histories:
            for item in history.items:
                if item.field in count_changes.keys():
                    count_changes[item.field] += 1
                    intervals.append(
                        {'start_date': history.created, 'field': item.field, 'fromString': item.fromString, 'toString': item.toString })

        def make_changelog_df(intervals):
            # Create data frame
            changelog = pd.DataFrame(intervals)
            changelog['start_date'] = pd.to_datetime(changelog['start_date'])
            changelog = changelog.sort_values(by='start_date')
            changelog = changelog.reset_index().drop(columns='index')
            return changelog

        if count_changes['Sprint'] == 0:
            intervals.append({'start_date': issue_created_date, 'field': 'Sprint', 'fromString': '', 'toString': issue_current_sprint})
        else:
            # otherwise take from first change
            changelog = make_changelog_df(intervals)
            first_change_row = changelog[changelog.field == 'Sprint'].iloc[0]
            first_found_value = first_change_row['fromString'] if first_change_row['fromString'] else 'Unassigned'
            intervals.append({'start_date': issue_created_date, 'field': 'Sprint', 'fromString': '', 'toString': first_found_value})

        # Add first artificial interval
        if count_changes['assignee'] == 0:
            # if assignee never changed - use current values
            intervals.append({'start_date': issue_created_date, 'field': 'assignee', 'fromString': '', 'toString': issue_current_assignee})
        else:
            # otherwise take from first change
            changelog = make_changelog_df(intervals)
            first_change_row = changelog[changelog.field == 'assignee'].iloc[0]
            first_found_value = first_change_row['fromString'] if first_change_row['fromString'] else 'Unassigned'
            intervals.append({'start_date': issue_created_date, 'field': 'assignee', 'fromString': '', 'toString': first_found_value})

        if count_changes['status'] == 0:
            intervals.append({'start_date': issue_created_date, 'field': 'status', 'fromString': '', 'toString': issue_current_status})
        else:
            # otherwise take from first change
            changelog = make_changelog_df(intervals)
            first_change_row = changelog[changelog.field == 'status'].iloc[0]
            first_found_value = first_change_row['fromString']
            intervals.append({'start_date': issue_created_date, 'field': 'status', 'fromString': '', 'toString': first_found_value})

        changelog = make_changelog_df(intervals)

        # return changelog

        # Create current values for each interval
        for field_name in count_changes.keys():

            field_selector = changelog['field'] == field_name

            # fill values from changelog
            changelog[field_name] = changelog.loc[field_selector, 'toString']

            # ensure the very first row has values
            first_found_row = changelog[field_selector].iloc[0]
            first_found_value = first_found_row['toString']

            if not first_found_value:
                # return changelog
                raise Exception(f"No first value found")
                # first_found_value = first_status_row['fromString']

            changelog.loc[0, field_name] = first_found_value

            # calculate intervals
            changelog['end_date'] = pd.to_datetime(changelog['start_date'].shift(-1).fillna(pd.to_datetime('now', utc=True)))
            changelog[field_name] = changelog[field_name].ffill()

        # Fill Unassigned values
        changelog.loc[(changelog.field == 'assignee') & (changelog.toString.isna()), 'toString'] = 'Unassigned'
        changelog.loc[(changelog.field == 'assignee') & (changelog.fromString.isna() | (changelog.fromString == '')), 'fromString'] = 'Unassigned'

        # Keep last Sprint
        # changelog['Sprint'] = changelog['Sprint'].apply(lambda x: x.split(',')[-1].strip() if pd.notna(x) else x)
        return changelog
    
    except Exception as e:
        print(f"An error occurred while fetching the changelog: {e}, skipping {issue_key}")
        print(changelog)

class Person:
    def __init__(self, name):
        # Initialize an empty list to store states
        self.states = []
        self.name = name

    def get_name(self):
        return self.name
    
    def record_state(self, issue_key, start_date, end_date, status, issue_type, sprint):
       
        end_date = self.round_to_next_working_day(end_date)
        
        # Add the state as a dictionary to the list
        new_state = {
                'issue_key': issue_key,
                'start_date': start_date,
                'end_date': end_date,
                'status': status,
                'issue_type': issue_type,
                'sprint': sprint
            }
        if new_state not in self.states:
            self.states.append(new_state)
    
    def round_to_next_working_day(self, date):
        if date.weekday() == 5:  # Saturday
            date += pd.Timedelta(days=2)
            date = date.replace(hour=cfg['start_hour'], minute=0, second=0, microsecond=0)
        elif date.weekday() == 6:  # Sunday
            date += pd.Timedelta(days=1)
            date = date.replace(hour=cfg['start_hour'], minute=0, second=0, microsecond=0)

        return date
    
    def get_states(self):
        return pd.DataFrame(self.states)
    
# Process data related to assignees and statuses

persons = {}

for i, issue in enumerate(all_issues):
    
    # print(issue.key)
    
    # provide update field from JIRA QUERY to figure out if we need to download issue again
    changelog = get_assignee_status_intervals(issue_key=issue.key, updated=pd.to_datetime(issue.fields.updated))

    # clear_output(wait=True)
    print(f"Downloaded changelogs for {issue.key}, {i}/{len(all_issues)} issues")

    if changelog is not None:
        for _, change in changelog.iterrows():
            # Create a person if its not existed
            if change.assignee not in persons.keys():
                persons[change.assignee] = Person(name=change.assignee)
            
            persons[change.assignee].record_state(
                issue_key = issue.key,
                start_date = change.start_date,
                end_date = change.end_date,
                status = change.status,
                issue_type = issue.raw.get('fields',{}).get('issuetype',{}).get('name', None),
                sprint = change.Sprint
            )
            
jira_cache["persons"] = persons
jira_cache['updated'] = pd.to_datetime('now')

print(f"All data has been updated {jira_cache['updated']}")