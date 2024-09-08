import pandas as pd
from jira import JIRA
from diskcache import Cache
from functools import lru_cache
import json

import dash
from dash import dcc, html, dash_table
from dash.dash_table import FormatTemplate
from dash.dependencies import Input, Output, State


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
            date = date.replace(hour=START_HOUR, minute=0, second=0, microsecond=0)
        elif date.weekday() == 6:  # Sunday
            date += pd.Timedelta(days=1)
            date = date.replace(hour=START_HOUR, minute=0, second=0, microsecond=0)

        return date
    
    def get_states(self):
        return pd.DataFrame(self.states)

# Function to calculate working seconds between two days
def calculate_working_seconds(start, end):
    
    def next_day(dt):
        days = [1, 1, 1, 1, 3, 2, 1][dt.weekday()]
        return (dt + pd.Timedelta(days=days)).replace(hour=cfg['start_hour'], minute=0, second=0, microsecond=0)

    if start >= end:
        return 0
    
    # If work started on holidays - ignore it and start from the next working day
    if start.weekday()>4:
        start = next_day(start)

    # Move start to the beginning of the day
    if start.hour < cfg['start_hour']:
        start = start.replace(hour=cfg['start_hour'], minute=0, second=0, microsecond=0)
    # If work started after the end of the day - move to the next day
    if start.hour >= cfg['end_hour']:
        start = next_day(start)
    # If work finsished early in the morning - move to the begging of the day
    if end.hour < cfg['start_hour']:
        end = end.replace(hour=cfg['start_hour'], minute=0, second=0, microsecond=0)
    # If work finished after the end - move to the next day
    if end.hour >= cfg['end_hour']:
        end = next_day(end)

    # print((start, end))

    # Initialize total working hours
    total_seconds = 0

    # Iterate by days, skipping weekends
    while start.date() < end.date():
        seconds_today = start.replace(hour=cfg['end_hour'], minute=0, second=0, microsecond=0).timestamp() - start.timestamp()
        total_seconds += seconds_today
        # print(f"start = {start}, adding {seconds_today} or {seconds_today/3600} hours")
        start = next_day(start)
    
    # add last day time, if its not ended on holidays
    if start.date() == end.date():
        last_day_seconds = end.timestamp() - start.timestamp()
        # print(f"{start}, {end} adding last day seconds {last_day_seconds}")
        total_seconds += last_day_seconds

    return total_seconds

# Normalizes time intervals and calculates their duration evenly spreading time to overlapping intervals
def get_unique_intervals(time_intervals):
    
    issue_meta_columns = ['issue_key', 'issue_type', 'status', 'sprint']
    # sort intervals from most distant to most recent
    unique_intervals = []
    time_intervals = time_intervals.sort_values(by='start_date', ascending=True)

    # 1. Build sub intervals
    edges = list()
    for _, interval in time_intervals.iterrows():
        edges.append(interval.start_date)
        edges.append(interval.end_date)

    edges = sorted(edges)

    for i in range(len(edges)-1):
        sub_interval_start = edges[i]
        sub_interval_end = edges[i+1]

        # 2. For each calculate the middle 
        sub_interval_center = sub_interval_start + (sub_interval_end - sub_interval_start)/2

        # 3. For each subinterval create unique time intervals and their normalized duration
        affected_time_intervals = time_intervals[(time_intervals.start_date < sub_interval_center) & (time_intervals.end_date > sub_interval_center)]
        sub_interval_num_overlapses = affected_time_intervals.shape[0]

        for _, interval in affected_time_intervals.iterrows():
            unique_interval = {
                "start_date": sub_interval_start,
                "end_date": sub_interval_end,
                "raw_duration": calculate_working_seconds(sub_interval_start, sub_interval_end),
            }
            unique_interval['normalized_duration'] = unique_interval['raw_duration']/sub_interval_num_overlapses

            for column in issue_meta_columns:
                unique_interval[column] = interval.get(column)

            unique_intervals.append(unique_interval)
    
    return pd.DataFrame(unique_intervals)

@lru_cache(maxsize=50)
def get_active_intervals(name, period_start_date, period_end_date):
    df = persons[name].get_states()

    # keep only tasks that fall into period of analysis
    df = df[(df.end_date > period_start_date) & (df.start_date < period_end_date)]

    # set edges
    df.loc[df.start_date<period_start_date, 'start_date'] = period_start_date
    df.loc[df.end_date>period_end_date, 'end_date'] = period_end_date

    active_intervals = df[df.status.str.upper().isin(cfg['statuses_and_stages']['active_statuses'])].sort_values(by='start_date').copy()
    # active_intervals = active_intervals[active_intervals.columns]

    return active_intervals[active_intervals.columns]

def get_sprint_state_at(name, date):
    sprints_data = jira_cache['sprints_data']
    sprint_data_row = sprints_data[sprints_data.name == name]
    if sprint_data_row.shape[0]==0:
        sprint_state = 'Not found'
    else:
        sprint_data_row = sprint_data_row.iloc[0]
        if date < sprint_data_row.startDate:
            sprint_state = 'Future'
        else:
            if date > sprint_data_row.endDate:
                sprint_state = 'Finished'
            else:
                sprint_state = 'Active'

    return sprint_state

def get_sprint_prioritized(sprint_str, ts):
    sprints = [s.strip() for s in sprint_str.split(',')]
    current_state = 'Not found'
    current_sprint = 'Not found'
    
    for sprint in sprints:
        # In August 2024 was renaming of sprints Sprint 17 became TMK 2024 Sprint 17
        if not 'MTK' in sprint:
            sprint = f"MTK 2024 {sprint}"

        state = get_sprint_state_at(sprint, ts)
        # print(f'State of the sprint {sprint} at {ts} is {state} ')
        
        if state == 'Active':
            return (sprint, state)
        
        # Update current state if conditions are met
        if current_state == 'Finished' and state == 'Future':
            current_state = state
            current_sprint = sprint
        elif state != 'Not found':
            current_state = state
            current_sprint = sprint
    
    return (current_sprint, current_state)

def get_middle_of_interval(t0, t1):
    return t0 + (t1 - t0)/2

def get_team_by_status(status):
    for team in cfg['statuses_and_stages']['stages']:
        if status in cfg['statuses_and_stages']['stages'][team]:
            return team
    return "Undefined"

@lru_cache(maxsize=100)
def get_active_hours_report(period_start_date, period_end_date):
    print(f"Called get_active_hours_report({period_start_date}, {period_end_date})")
    period_start_date = pd.to_datetime(period_start_date).replace(hour = cfg['start_hour']).tz_localize('Europe/Moscow')
    period_end_date   = pd.to_datetime(period_end_date).replace(hour = cfg['end_hour']).tz_localize('Europe/Moscow')

    records = []

    for name in jira_cache['persons'].keys():
        # print(name)
        record = {"name": name}
        active_intervals = get_active_intervals(name, period_start_date, period_end_date)
        
        unique_intervals = get_unique_intervals(active_intervals)
        
        if len(unique_intervals)>0:
            record['total, hours'] = unique_intervals.normalized_duration.sum()/3600

            # Calculate the state of the sprint for each interval
            unique_intervals['middle_date'] = unique_intervals.apply(lambda row: get_middle_of_interval(row['start_date'], row['end_date']), axis=1)
            unique_intervals['priority_sprint_status'] = unique_intervals.apply(lambda row: get_sprint_prioritized(row['sprint'], row['middle_date'])[1], axis=1)
            # unique_intervals['priority_sprint'] = unique_intervals.apply(lambda row: get_sprint_prioritized(row['sprint'], row['middle_date'])[0], axis=1)

            # identify for which team the person spends most of the time
            teams = unique_intervals.status.str.upper().apply(lambda x: get_team_by_status(x))
            record['team'] = unique_intervals.groupby(teams).normalized_duration.sum().idxmax()

            record['active_sprint'] = unique_intervals.loc[unique_intervals['priority_sprint_status']=='Active', 'normalized_duration'].sum()/3600
            record['future_sprint'] = unique_intervals.loc[unique_intervals['priority_sprint_status']=='Future', 'normalized_duration'].sum()/3600
            record['finished_sprint'] = unique_intervals.loc[unique_intervals['priority_sprint_status']=='Finished', 'normalized_duration'].sum()/3600
            record['notfound_sprint'] = unique_intervals.loc[unique_intervals['priority_sprint_status']=='Not found', 'normalized_duration'].sum()/3600

        records.append(record)

    df = pd.DataFrame(records).sort_values('total, hours', ascending = False).dropna().round(2)
    df['active'] = df['active_sprint'] / calculate_working_seconds(period_start_date, period_end_date) * 3600
    # df['active'] = df['active'].apply(lambda x: f"{x:0.2%}")

    return df

def format_timedelta(td):
    days = td.days
    hours, remainder = divmod(td.seconds, 3600)
    minutes, _ = divmod(remainder, 60)
    return f"{days}d {hours}h {minutes}m"

@lru_cache(maxsize=100)
def get_active_tasks_report(name, period_start_date = None, period_end_date = None):
    print(f"Called get_active_tasks_report({name}, {period_start_date}, {period_end_date})")

    period_start_date = pd.to_datetime(period_start_date).replace(hour = cfg['start_hour']).tz_localize('Europe/Moscow')
    period_end_date   = pd.to_datetime(period_end_date).replace(hour = cfg['end_hour']).tz_localize('Europe/Moscow')

    df = get_active_intervals(name, period_start_date, period_end_date)

    df['duration'] = (df.end_date - df.start_date)
    df = df.sort_values(by = 'duration', ascending = False)
    df.duration = df.duration.apply(format_timedelta)

    df = df.rename(columns={'issue_key':'key'})
    issue_summaries = jira_cache['issue_titles']
    df.insert(1, 'summary', df['key'].apply(lambda x: issue_summaries.get(x, 'Not found')))
    # df = pd.merge(jira_cache['issue_titles'], df, right_on='issue_key', left_on = 'key', how = 'right', suffixes=('x_', 'y_')).drop(columns='issue_key').fillna('')

    # df['middle_date'] = df.apply(lambda row: get_middle_of_interval(row['start_date'], row['end_date']), axis=1)
    df['priority_sprint_status'] = df.apply(lambda row: get_sprint_prioritized(row['sprint'], row['end_date'])[1], axis=1)
    df['priority_sprint'] = df.apply(lambda row: get_sprint_prioritized(row['sprint'], row['end_date'])[0], axis=1)

    df.loc[:, 'start_date'] = df.loc[:, 'start_date'].dt.tz_convert('Europe/Moscow').dt.strftime('%Y-%m-%d %H:%M:%S')
    df.loc[:, 'end_date'] = df.loc[:, 'end_date'].dt.tz_convert('Europe/Moscow').dt.strftime('%Y-%m-%d %H:%M:%S')

    return df#.drop(columns = ['middle_date'])

# Initialize the Dash app
with open("config.json", "r") as f: cfg = json.load(f)
jira_cache = Cache(directory=".cache")
persons = jira_cache['persons']

app = dash.Dash(__name__)

default_start_date = '2024-08-01T00:00:00'
default_end_date = '2024-09-01T00:00:00'
active_hours = get_active_hours_report(default_start_date, default_end_date)
active_hours_format = {'active': FormatTemplate.percentage(2) }
active_tasks = get_active_tasks_report(active_hours.name.iloc[0], default_start_date, default_end_date)

def create_task_tables():
    list_of_tables = []
    for sprint_status in ['Finished', 'Not found', 'Active', 'Future']:
        list_of_tables.append(
            html.Div([
                html.H3(f"{sprint_status} sprints"),
                dash_table.DataTable(
                    id=f'table-active-tasks-{sprint_status}',
                    columns=[{"name": col, "id": col, "presentation": "markdown"} for col in active_tasks.columns],
                    data=active_tasks[active_tasks.priority_sprint_status == sprint_status].to_dict('records'),
                    style_table={'margin-top': '20px', 'width': '100%'},  # Adjust table width
                    style_cell={
                        'textAlign': 'left',
                        'padding': '10px',
                        'fontFamily': 'Arial',
                        'fontSize': '14px',
                        'whiteSpace': 'normal',
                    },
                    style_data={
                        'width': '150px',
                        'maxWidth': '150px',
                        'minWidth': '50px',
                    },
                    style_header={
                        'backgroundColor': 'lightgrey',
                        'fontWeight': 'bold',
                        'textAlign': 'left',
                    },
                    style_cell_conditional=[
                        {'if': {'column_id': 'Details'}, 'width': '200px', 'textAlign': 'right'},
                    ]
                )
            ])
        )
    return html.Div(list_of_tables, style={'flex': '2'})
# Define the layout of the app
app.layout = html.Div([
    html.H1("Active Work Time Distribution"),
    
    html.Div([
        html.Label('Start Date:', style={'margin-right': '10px'}),
        dcc.DatePickerSingle(
            id='start-date',
            date=pd.to_datetime(default_start_date),  # Default start date
            display_format='YYYY-MM-DD',
            style={'margin-right': '20px'}
        ),
        html.Label('End Date:', style={'margin-right': '10px'}),
        dcc.DatePickerSingle(
            id='end-date',
            date=pd.to_datetime(default_end_date),  # Default end date
            display_format='YYYY-MM-DD'
        ),
    ], style={'margin-bottom': '20px'}),
    
    html.Div([
        html.Div([
            html.H3("Active Working Hours per Person"),
            dash_table.DataTable(
                id='table-active-hours',
                columns=[{"name": col, "id": col, "type":"numeric", "format": active_hours_format.get(col, None)} for col in active_hours.columns],
                row_selectable='single',
                selected_rows=[0],
                cell_selectable=False,
                data=active_hours.to_dict('records'),
                style_table={'margin-top': '20px', 'width': '100%'},  # Adjust table width
                style_cell={
                    'textAlign': 'left',
                    'padding': '10px',
                    'fontFamily': 'Arial',
                    'fontSize': '14px',
                    'whiteSpace': 'normal',
                },
                style_data={
                    'width': '150px',
                    'maxWidth': '150px',
                    'minWidth': '50px',
                },
                style_header={
                    'backgroundColor': 'lightgrey',
                    'fontWeight': 'bold',
                    'textAlign': 'left',
                },
                style_data_conditional=[
                    #{'if': {'column_id': 'Hours'}, 'width': '100px', 'textAlign': 'right'},
                    {
                        'if': {
                            'filter_query': '{active} > 0.5',  # Values greater than 80%
                            'column_id': 'active'
                        },
                        'backgroundColor': '#28a745',
                        'color': 'white'
                    },
                    {
                        'if': {
                            'filter_query': '{active} >= 0.25 && {active} <= 0.5',  # Values between 30% and 80%
                            'column_id': 'active'
                        },
                        'backgroundColor': '#ffc107',
                        'color': 'black'
                    },
                    {
                        'if': {
                            'filter_query': '{active} < 0.25',  # Values less than 30%
                            'column_id': 'active'
                        },
                        'backgroundColor': '#dc3545',
                        'color': 'white'
                    }
                ]
            )
        ], style={'flex': '1', 'padding-right': '20px'}),
        create_task_tables()
    ], style={'display': 'flex', 'justify-content': 'space-between'})
])

# Callback to react to date pickers
@app.callback(
    Output('table-active-hours', 'data'),
    Output('table-active-hours', 'selected_rows'),
    Input('start-date', 'date'),
    Input('end-date', 'date'),
    # State('table-active-hours', 'selected_row_ids')
)
def update_output(start_date, end_date):
    # Get the report data
    active_hours = get_active_hours_report(start_date, end_date)
    # Return data for the DataTable and reset selected row
    return active_hours.to_dict('records'), [0]

# Selecting rows in table
def define_callbacks(status):
    @app.callback(
        Output(f'table-active-tasks-{status}', 'data'),
        # Output('table-active-tasks', 'style_data_conditional'),
        Input('table-active-hours', 'selected_rows'),
        Input('start-date', 'date'),
        Input('end-date', 'date')
    )
    def display_selected_row(selected_rows, start_date, end_date):

        if selected_rows is None or len(selected_rows) == 0:
            return pd.DataFrame().to_dict('records'), []
        else:
            
            selected_index = selected_rows[0]
            active_hours = get_active_hours_report(start_date, end_date)
            selected_row_data = active_hours.iloc[selected_index]
            selected_person_name = selected_row_data['name']
            print(f"Selected row is {selected_rows} -> {selected_person_name}")
            # print(status, ', ', selected_person_name,' ,', start_date,', ', end_date)
            active_tasks = get_active_tasks_report(selected_person_name, start_date, end_date)
            active_tasks = active_tasks[active_tasks.priority_sprint_status == status]

            active_tasks.loc[:, 'key'] = active_tasks.loc[:, 'key'].apply(lambda x: f"[{x}]({cfg['jira_server']}/browse/{x})")

            return active_tasks.to_dict('records')

for sprint_status in ['Finished', 'Not found', 'Active', 'Future']:
    define_callbacks(sprint_status)

# Run the app
if __name__ == '__main__':
    print("Reading configuration...")
    app.run_server(debug=True, port=9090)