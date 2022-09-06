from typing import Dict
import pandas as pd
import numpy as np
import time as t
import math

# V1: variant 1 of the results DataFrame structure: rows = MP, columns = (variable + "_" + projection year)
# E.g. math_res_closing_0, math_res_closing_1, ...

def calculate_year_sim_v1(portfolio: pd.DataFrame, year: int, scenario: pd.Series, results: pd.DataFrame):
    
    s_year = str(year)
    if year == 0:
        results['math_res_opening_' + s_year] = portfolio['math_res']
        results['math_res_before_ps_' + s_year] = portfolio['math_res']
        results['math_res_closing_' + s_year] = portfolio['math_res']
    else:
        results['math_res_opening_' + s_year] = results['math_res_closing_' + str(year-1)]
        results['math_res_before_ps_' + s_year] = results['math_res_opening_' + s_year]     
    
        funds = results[['id_pool', 'math_res_opening_' + s_year]].groupby('id_pool').agg('sum')
        results['ret_rate'] = scenario[1][s_year]/scenario[1][str(year-1)] + np.where(funds['math_res_opening_' + s_year][results['id_pool']] > 1000000, portfolio['spread'], 0)
        results['math_res_closing_' + s_year] = results['math_res_before_ps_' + s_year] * results['ret_rate']

    return results

def calculate_v1(portfolio : pd.DataFrame, scenarios: pd.DataFrame, projectionTerm: int):
    for scen in scenarios.iloc[0:25].iterrows():
        print('-> Calculating scenario {}: '.format(scen[0]), end='')
        
        t_start = t.time()
        for year in range(projectionTerm+1):
            # create the results dataframe  
            if year == 0:
                # create a list with all variable names (var x projection years)
                vars = ['math_res_opening', 'math_res_before_ps', 'math_res_closing']
                cols = [var +'_' + str(x) for var in vars for x in range(projectionTerm+1)]
                
                # create a full results DataFrame filled with float32 zeroes (MP x vars)
                results = pd.DataFrame(data=np.zeros(shape=(numPolicies, len(cols)), dtype=np.float32), columns=cols)
                results['id_pool'] = portfolio['id_pool']
            results = calculate_year_sim_v1(portfolio, year, scen, results)
        t_end = t.time()
        print('done in {:.03f} s'.format((t_end-t_start)))
         
if __name__ == "__main__":
    # read in policy data from a CSV file
    portfolio = pd.read_csv('data/mp_policies.csv')
    numPolicies = portfolio.shape[0]

    # read in scenario data from a CSV file
    scenarios = pd.read_csv('data/scen_eco.csv')
    numScenarios = scenarios.shape[0]
    
    # projection parameters
    projectionTerm = scenarios.shape[1]-4
    targetRows = 10000
    portfolio['spread'] = np.where(portfolio['id_pool'] == 1, 0.01, 0.025)
    
    # print some info
    print('Initializing projection model')
    print('+ read in {} policies'.format(numPolicies))
    print('+ read in {} scenarios'.format(numScenarios))
    print('+ setting projection term to {} years'.format(projectionTerm))

    # adjust portfolio size if necessary (clone existing rows to be >= targetRows)
    if numPolicies < targetRows:
        print('+ cloning policies to match {} portfolio size'.format(targetRows))
        portfolioFactor = math.ceil(targetRows/numPolicies)
        portfolio = pd.concat([portfolio]*portfolioFactor, ignore_index=True)
        numPolicies = portfolio.shape[0]
        print('+ portfolio size is now {} policies'.format(numPolicies))

    # run calculations
    print('\nStarting projection calculations')
    calculate_v1(portfolio, scenarios, projectionTerm)

    