import pandas as pd
import numpy as np

import warnings
warnings.filterwarnings('ignore')

import os

def get_file_names(path_to_files):
    """
    Returns a list of tickers in each folder.
    
    Parameters
    ----------
    path_to_files : dict
        Dict values are directory path to folders 
        of certain feature group.
    """
    result = []
    for _, path in path_to_files.items():
        try:
            files = os.listdir('data/{}'.format(path))
            tickers = [file.strip('.csv') for file in files]
            result.append(tickers)
        except:
            continue
    return result

def list_intersection(file_names):
    """
    Returns a list of common tickers which are contained 
    in every data folders.
    
    Parameters
    ----------
    file_names : list
        List of lists. Each inner list represents tickers 
        in each data folder.
    """
    max_length = 0
    max_idx = 0
    for idx in range(len(file_names)):
        cur_len = len(file_names[idx])
        if cur_len > max_length:
            max_idx = idx
            max_length = cur_len
    
    s = set(file_names[max_idx])
    return list(s.intersection(*file_names))

def get_feature_names(path_to_files, ticker):
    """
    Returns a list of feature names we want to add to a dataframe.
    
    Parameters
    ----------
    path_to_files : dict
        Dict values are directory path to folders 
        of certain feature group.
    common_element : str
        Ticker contained in each data folder.
    """
    features = []
    for _, path in path_to_files.items():
        cols = pd.read_csv('data/{0}/{1}.csv'.format(path, ticker))
        features.append(cols.columns.values)
    return features

def create_base_df(path_to_files, tickers):
    """
    Returns a DataFrame with basic features related to earning releases 
    for each company: release date, period end, estimates.
    
    Parameters
    ----------
    path_to_files : str
        Path to basic files, stored in folder 'estimates'.
    """
    df = pd.read_csv('{0}{1}.csv'.format(path_to_files, tickers[0]))
    df['ticker'] = tickers[0]
    if len(tickers) == 1:
        return df
    
    idx = len(tickers)
    for ticker in tickers[1:]:
        try:
            current_df = pd.read_csv('{0}{1}.csv'.format(path_to_files,
                                                         ticker))
            current_df['ticker'] = ticker
            df = pd.concat([df, current_df], ignore_index=True)
        except:
            print(ticker + ' not found!') 
            
    df.reset_index(drop=True)
    df['Release Date'] = pd.to_datetime(df['Release Date'], 
                                        format='%b %d, %Y')
    df['Period End'] = pd.to_datetime(df['Period End'])
    return df

def add_fundamentals(df, tickers, path_to_files, groups):
    """
    Returns a DataFrame with added fundamental features.
    
    Parameters
    ----------
    df : DataFrame
        DataFrame created in function 'create base df'.
    tickers : numpy array
        Array with ticker names.
    path_to_files : dict
        Dict values are directory path to folders 
        of certain feature group (key).  
    groups : list
        List of feature groups, e.g. 'Income statement' etc.
    """
    # add columns we want to use
    new_groups = []
    for group_num in range(len(groups)):
        new_groups.append(groups[group_num][1:-1])
        all_zeros = np.zeros(len(new_groups[-1]))
        match_group_val = dict(zip(new_groups[-1], all_zeros))
        df = df.assign(**match_group_val)

    group_num = 0
    for group_name, path in path_to_files.items():
        features = new_groups[group_num]
        for ticker in tickers:
            indexes = df[df['ticker']==ticker].index.values
            calendar = df[df['ticker']==ticker]['Period End']
            data = pd.read_csv('data/{0}/{1}.csv'.format(path, ticker))
            data['date'] = pd.to_datetime(data['date'], format='%Y-%m-%d')
            for idx in indexes:
                # find data for appropriate release day 
                cond_df = data[(data['date'].dt.year==calendar[idx].year)\
                             & (data['date'].dt.month==calendar[idx].month)]
                if not cond_df.empty:
                    df.loc[idx,features] = cond_df.loc[:,features].values[0]
            print(group_name + ' data for ' + ticker + ' added!')
        print('\n' + group_name + ' features have been added!\n')
        group_num += 1
    return df

def assign_values(df, n_before, k_after, idx = None, vals = None, 
                  total_inds = None):
    """
    Assign values of stock quotes to certain rows of DataFrame. 
    If idx to insert in isn't passed the function adds new zero-valued 
    columns with names: 
    ['<i>_open','<i>_high','<i>_low','<i>_close','<i>_volume'], 
    where i equals to number of days before or after release day.
    
    Parameters
    ----------
    df : DataFrame
        DataFrame we want to modify. 
    n_before : int
        Number of days before release day.
    k_after : int
        Number of days after release day.
    idx : int
        Index of row we want to insert quotes in.
    vals : numpy array
        1d array of shape (n_before + k_after + 1) contained values
        of stock price (open, high, etc.) in certain days in a form:
        [<n> before open, <n> before high, ..., <n> after volume]
    """   
    if idx is None:
        # create a list of all stock price related columns
        prev_days = [str(n) + 'd_before' for n in range(n_before, 0, -1)]
        next_days = [str(k) + 'd_after' for k in range(1, k_after + 1)]
        all_days = prev_days + ['0d_release'] + next_days
        
        quote_inds = ['open', 'high', 'low', 'close', 'volume']
        total_inds = []
        for day in all_days:
            for ind in quote_inds:
                total_inds.append(day + '_' + ind)
        # when using first time all new columns assigned with zeros.
        init_vals = np.zeros(len(total_inds))
        match_ind_val = dict(zip(total_inds, init_vals))
        df = df.assign(**match_ind_val)        
        return (df, total_inds)
    
    df.loc[idx, total_inds] = vals
    return df,_

def find_quotes(ticker, quotes, release, n_before, k_after):
    """
    Returns a 2d numpy array of stock prices of shape 
    (n_before + k_after + release, 5), where rows and cols represent 
    day and information about stock price (open, high, low, close, volume),
    respectively. If certain days are outside of the table a corresponding 
    row is filled with zeros.
    
    Parameters
    ----------
    ticker : str
        Ticker name.
    quotes : DataFrame
        Open file in form of DataFrame, where prices would be searched.
    release : DateTime
        Day of earnings release, which is base for search.
    n_before : int
        Number of days we want to extract quotes before release day.
    n_after : int
        Number of days we want to extract quotes after release day.
    """
    # store first and last day of entire df to keep track days lying 
    # outside of df to fill them with zeros.
    first_date = quotes['date'][0]
    last_date = quotes['date'][len(quotes) - 1]
    
    # if date wasn't found return zeros array
    idx_release_df = quotes[quotes['date'] == release]
    if idx_release_df.empty:
        out_shape = (n_before + k_after + 1, quotes.shape[1] - 1)
        return np.zeros(out_shape)

    idx_release = idx_release_df.index[0]
    start_date = max(idx_release - n_before, 0)
    end_date = min(idx_release + k_after, len(quotes) - 1)
    
    # fill rows outside the table with zeros
    quotes_needed = quotes.iloc[start_date:end_date + 1, 1:]
    if len(quotes_needed) != n_before + k_after + 1:
        dif_start = idx_release - n_before
        dif_end = idx_release + k_after - len(quotes) + 1
        if dif_start < 0:
            additional_rows = np.zeros((-dif_start, quotes_needed.shape[1]))
            additional_df = pd.DataFrame(additional_rows, 
                                         columns=quotes_needed.columns.values)
            quotes_needed = pd.concat([additional_df, quotes_needed])
        if dif_end > 0:
            additional_rows = np.zeros((dif_end, quotes_needed.shape[1]))
            additional_df = pd.DataFrame(additional_rows, 
                                         columns=quotes_needed.columns.values)
            quotes_needed = pd.concat([quotes_needed, additional_df])
    return quotes_needed.values

def add_stock_quotes(df, tickers, n_before, k_after):
    """
    Returns a DataFrame with quotes for each row for multiple days
    in range from release - n_before to release + k_after.
    
    Parameters are the same as before.
    """
    df, total_inds = assign_values(df, n_before, k_after)

    idx = 0
    for ticker in tickers:
        path = 'data/historical_daily_quotes/{}.csv'.format(ticker)
        quotes = pd.read_csv(path)      
        quotes['date'] = pd.to_datetime(quotes['date'], 
                                          format='%Y-%m-%d')
        
        for _, row in df[idx:].iterrows():
            if row['ticker'] != ticker:
                continue   
                
            release_date = row['Release Date']
            vals = find_quotes(ticker, quotes, release_date, 
                               n_before, k_after)
            df,_ = assign_values(df, n_before, k_after, 
                                 idx, vals.flatten(), total_inds)
            idx += 1
        print(ticker + ' stock quotes have been added!')
    return df