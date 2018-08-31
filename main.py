#==============================================================================
from datetime import datetime, timedelta
import pytz
import json
import requests
import pandas as pd
import time 
import numpy as np
from pandas.io.json import json_normalize
from keboola import docker
#==============================================================================

#Authorization settings
# with open('data.txt', 'r') as myfile:
#     API_token=myfile.read().replace('\n', '')


# cfg = docker.Config('/data/')
# refresh_token = cfg.get_parameters()['#refresh_token']
cfg = docker.Config('/data/')
API_token = cfg.get_parameters()['#api_token']
# load_mode = cfg.get_parameters()['processing_mode']
parameters = {'authtoken': API_token,
              'flag': 'internal'}


#==============================================================================
#Functions for extracting data from specific endpoints
#==============================================================================
def get_data_projects(target_df, endpoint, to_append,  url=''):

    req = requests.get(url=url, params=parameters)

    if req.status_code != 200:
        print('Warning: The returned status code is: ' + str(req.status_code))
        return(target_df)

    else:
        df_tmp = json_normalize(req.json()[endpoint][0])
        df_tmp['parent'] = to_append

    if target_df.empty:
        target_df = df_tmp
    else:
        target_df = target_df.append(df_tmp)

    return(target_df)

def get_data_tasklists(target_df, endpoint, to_append, url = ''):
    
    
    req = requests.get(url = url, params = parameters)

    if req.status_code != 200:
        print('Warning: The returned status code is: ' + str(req.status_code))
        return(target_df)

    else:
        df_tmp = json_normalize(req.json()[endpoint])
        df_tmp['parent'] = to_append
    
    if target_df.empty:
        target_df = df_tmp
    else:
        target_df = target_df.append(df_tmp)
  
    return(target_df)


def get_data_milestones(target_df, endpoint, to_append, target_id='', url=''):

    req = requests.get(url=url, params=parameters)

    if req.status_code != 200:
        print('Warning: The returned status code is: ' + str(req.status_code))
        return(target_df)

    else:
        df_tmp = json_normalize(req.json()[endpoint])
        df_tmp['parent'] = to_append

    if target_df.empty:
        target_df = df_tmp
    else:
        target_df = target_df.append(df_tmp)

    return(target_df)
        
def get_data_tasks(target_df, endpoint, to_append, url = ''):
    
    
    req = requests.get(url = url, params = parameters)
    
    if req.status_code != 200:
        print('Warning: The returned status code is: ' + str(req.status_code))
        return(target_df)
    
    else:
        df_tmp = json_normalize(req.json()[endpoint])
        df_tmp['parent'] = to_append
    
    
    if target_df.empty:
        target_df = df_tmp
    else:
        target_df = target_df.append(df_tmp)
  
    return(target_df)

def get_data_timelogs(target_df, endpoint, to_append, url = ''):
    
    req = requests.get(url = url, params = parameters)
    
    if req.status_code != 200:
        print('Warning: The returned status code is: ' + str(req.status_code))
        return(target_df)
    
    else:
        df_tmp = json_normalize(req.json()[endpoint]['tasklogs'])
        df_tmp['parent'] = to_append
    
    
    if target_df.empty:
        target_df = df_tmp
    else:
        target_df = target_df.append(df_tmp)
  
    return(target_df)
        

#Extract data for projects
url = 'https://projectsapi.zoho.eu/restapi/portal/20061034739/projects/'
req = requests.get(url=url, params=parameters)
base_df = json_normalize(req.json()['projects'])


projects_info = pd.DataFrame(np.zeros((0, 0)))
for i in base_df['link.self.url']:
    projects_info = get_data_projects(target_df=projects_info,
                                      url=i, endpoint='projects',
                                      to_append='Keboola')
    time.sleep(1)

print('projects extracted')
#Extract data for tasklists
tasklists_info = pd.DataFrame(np.zeros((0,0)))
for counter, i in enumerate(projects_info['link.tasklist.url']):
    tasklists_info = get_data_tasklists(target_df = tasklists_info, url = i,
                              endpoint = 'tasklists', to_append = projects_info['id'].iloc[counter])
    time.sleep(1)
print('tasklists extracted')

#Extract data for milestones
milestones_info = pd.DataFrame(np.zeros((0, 0)))
for counter, i in enumerate(projects_info['link.milestone.url']):
    milestones_info = get_data_milestones(target_df=milestones_info, url=i,
                                         endpoint='milestones', to_append=projects_info['id'].iloc[counter])
    time.sleep(1)
print('milestones extracted')

 #Extract data for tasks
tasks_info = pd.DataFrame(np.zeros((0,0)))
for counter, i in enumerate(tasklists_info['link.task.url']):
    tasks_info = get_data_tasks(target_df = tasks_info, url = i,
                                endpoint = 'tasks',
                                to_append = tasklists_info['id'].iloc[counter])
    time.sleep(1)
print('tasks extracted')
#Extract data for timelogs
timelogs_info = pd.DataFrame(np.zeros((0,0)))
for counter, i in enumerate(tasks_info['link.timesheet.url']):
    timelogs_info = get_data_timelogs(target_df = timelogs_info, url = i,
                                endpoint = 'timelogs',
                                to_append = tasks_info['id'].iloc[counter])
    time.sleep(1)
print('timelogs extracted')

userstasks_info = pd.DataFrame(np.zeros((0,0)))                           
for i in range(tasks_info.shape[0]):
    df_tmp = json_normalize(tasks_info['details.owners'].iloc[i])
    df_tmp['task_id'] = tasks_info['id'].iloc[i]
                           
    if userstasks_info.empty:
        userstasks_info = df_tmp
    else:
        userstasks_info = userstasks.append(df_tmp)
print('userstasks extracted')


## Write extracted data
projects_info.to_csv('/data/out/tables/projects.csv', index=False)
timelogs_info.to_csv('/data/out/tables/timelogs.csv', index=False)
tasks_info.to_csv('/data/out/tables/tasks.csv', index=False)
tasklists_info.to_csv('/data/out/tables/tasklists.csv', index=False)
milestones_info.to_csv('/data/out/tables/milestones.csv', index=False)
userstasks_info.to_csv('/data/out/tables/users_tasks.csv', index=False)

