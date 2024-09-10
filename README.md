# Dash App for Active Work Time Distribution

This project is a Dash application designed to analyze and visualize active work time distribution from Jira issues. It provides insights into individual and team activities over specified periods, highlighting task durations, sprint statuses, and time allocations.

## Features

- **Active Working Hours Analysis**: Displays total active hours per person within a specified time frame.
- **Task Details**: Provides detailed tables for tasks in various sprint states (`Finished`, `Not found`, `Active`, `Future`).
- **Configurable Date Range**: Allows the selection of start and end dates to adjust the analysis period.
- **Data Integration**: Pulls data from Jira issues, using caching to improve performance.

## Prerequisites

- Python 3.8 or higher
- Dependencies listed in `requirements.txt`

## Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/r-ms/jira-coders-dash
   cd jira-coders-dash/
   ```

2. **Create config.json** editing config.json.example: change `jira_user` and `jira_token`

3. **Download JIRA issues**  
```bash
python download-jira-tickets.py
```

## Configuration

The app uses a config.json file with the following keys:

- start_hour: The starting hour of the workday.
- end_hour: The ending hour of the workday.
- statuses_and_stages: Defines active statuses and their respective stages.

## Running the Application

To start the Dash app, run:

```bash
python app.py
```
Access the app in your web browser at http://127.0.0.1:9090.

## Usage

- Select Start and End Dates: Use the date pickers to define the period for analysis.
- View Active Hours: The main table shows the active working hours per person, including the percentage of active time.
- Explore Task Details: Click on a row in the main table to see detailed task information, categorized by sprint status.

## Key Functions

Person Class: Manages individual records and states, handling time rounding to the next working day.
calculate_working_seconds: Calculates the working seconds between two dates, considering weekends and work hours.
get_unique_intervals: Normalizes time intervals to evenly distribute time among overlapping tasks.
get_active_intervals: Fetches active task intervals for a person within a specified date range.
get_active_hours_report: Generates a report of active hours per person for the given period.
get_active_tasks_report: Provides detailed task information for a person, categorized by sprint status.


## License

This project is licensed under the MIT License.

## Contact

For questions or support, please contact m.rakutko@gmail.com
