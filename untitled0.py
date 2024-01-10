# -*- coding: utf-8 -*-
"""
Created on Sun Jan  7 18:14:34 2024

@author: 91959
"""





import numpy as np 
import os 
import pandas as pd




#importing all the files

casa_org = pd.read_csv('C:\\Users\\91959\\Desktop\\project1\\casa_constant_rates.csv', na_values=['-'])
curve_repo_org = pd.read_csv('C:\\Users\\91959\\Desktop\\project1\\curve_repository_mul.csv',  na_values=['-'])
ftp_synthetic_org = pd.read_csv('C:\\Users\\91959\\Desktop\\project1\\ftp_synthetic_curve.csv', na_values=['-'])
ftp_components_org = pd.read_csv('C:\\Users\\91959\\Desktop\\project1\\ftp_curve_components.csv' , na_values=['-'])
#pool2spread_org = pd.read_csv('C:\\Users\\91959\\Desktop\\project1\\pool_wise_spread_mapping.csv', na_values=['-'])
pool2spread_org = pd.read_csv('C:\\Users\\91959\\Desktop\\project1\\pool_wise_spread_mapping.csv', na_values=['-'])
pool2curve_org = pd.read_csv('C:\\Users\\91959\\Desktop\\project1\\pool_to_curve_mapping.csv', na_values=['-'])



#base FTP calculation 

pool2curve = pool2curve_org[['pool_name', 'product_group','base_ftp_curve','constant_parameter_value','pool_id']]
ftp_synthetic =  ftp_synthetic_org[['curve_components','curve_name','rate']]


#merge -- right (loss of constant parameter but gain of numbe of  rows  //  left loss of rows gain of constaint paremeter)
# on right we get rows that doesnt have pool id defined 
pool2curve.rename(columns = {'base_ftp_curve' : 'curve_name'},inplace = True )
curve2synthetic =  pool2curve.merge(ftp_synthetic,on='curve_name',how = 'right')

ftp_components =  ftp_components_org[['tenor_value','tenor_unit','curve_components']]


curve2synthetic2components = curve2synthetic.merge(ftp_components,on = 'curve_components', how = 'left')
curve2synthetic2components.rename(columns = {'rate' : 'base_ftp_rate'},inplace = True)

base_ftp_check  =  curve2synthetic2components.copy()
base_ftp_check  = base_ftp_check[['pool_id','pool_name','product_group','tenor_value','tenor_unit','base_ftp_rate']]




# credit spread and credit premium calculation 


c2s2c = curve2synthetic2components.copy()


pool2spread = pool2spread_org[['pool_id','pool_name','product_group','spread_type','spread_source','spread_curve','spread_name','constant_parameter_value']]
pool2spread = pool2spread.dropna(subset =['spread_source'])

c2s2c = c2s2c.merge(pool2spread,on = ['pool_id','pool_name','product_group'], how = 'right')

c2s2c.drop(['curve_name','constant_parameter_value_x'], axis=1, inplace=True)
c2s2c.rename(columns = {'constant_parameter_value_y':'constant_parameter_value'},inplace = True )

liability = c2s2c[c2s2c['spread_type'].notna()]
unique_values = liability['spread_type'].unique()

for value in unique_values:
    liability[str(value)] = np.nan

casa = casa_org[['constant_rate_parameter', 'custom_rate']]
casa.rename(columns={'custom_rate': 'rate', 'constant_rate_parameter': 'Constant Parameter'}, inplace=True)

ftp_synthetic_casa = ftp_synthetic.copy()
ftp_synthetic_casa = ftp_synthetic.merge(ftp_components, on = 'curve_components',how ='right')
ftp_synthetic_casa.rename(columns={'curve_name': 'Curve Based'}, inplace=True)


liability.rename(columns={'constant_parameter_value': 'Constant Parameter',
                           'spread_curve': 'Curve Based', 'spread_name': 'Masked Based'}, inplace=True)

source_to_df = {
    'Curve Based': ftp_synthetic_casa,
    'Constant Parameter': casa
}

liability2 = liability.copy()

# Reset index for relevant DataFrames
liability = liability.reset_index(drop=True)
ftp_synthetic_casa = ftp_synthetic_casa.reset_index(drop=True)
casa = casa.reset_index(drop=True)


# Iterate through each spread_source and spread_type




# if foramt is like casa
for spread_source in source_to_df.keys():
    for spread_type in unique_values:
        condition = (liability['spread_source'] == spread_source) & (liability['spread_type'] == spread_type)
        # Merge with the source DataFrame based on 'spread_source'
        merged_df = pd.merge(liability, source_to_df[spread_source], left_on= spread_source, right_on= spread_source, how='left')

# Assign the 'rate' column to the 'spread_type' column in 'liability2'
        liability2[spread_type] = merged_df['rate']
        
        
        
        
# if the format is like tenor & tenor unit 
for spread_source in source_to_df.keys():
    for spread_type in unique_values:
        condition = (liability['spread_source'] == spread_source) & (liability['spread_type'] == spread_type)
        
        # Filter the DataFrame based on the condition
        filtered_liability = liability[condition]
        
        # Merge with the source DataFrame based on 'spread_source'
        key = spread_source 
        if(key == 'Curve Based') :
            key  = ['Curve Based','tenor_value','tenor_unit']   #code to give speacil preference to ftp_synthetic casa
        merged_df = pd.merge(filtered_liability, source_to_df[spread_source], left_on=key, right_on=key , how='left')
        
        # Assign the 'rate' column to the 'spread_type' column in 'liability2'
        liability2.loc[condition, spread_type] = merged_df['rate']




        #liability2.loc[condition, spread_type] = merged_df['rate'].where(liability2[spread_type].isna())



      ## merging type of ftp_sythetic is different and it includes tenor and  tenor_unit as well  
        # Check for duplicate values for 'credit spread'
#print(liability2[liability2['spread_type'] == 'Credit spread']['spread_source'].value_counts())

# Check for duplicate values for 'liquidity premium'
#print(liability2[liability2['spread_type'] == 'Liquidity premium']['spread_source'].value_counts())

        
        
        
        #liability2[spread_type] = np.where(condition, liability['spread_source'].map(source_to_df[spread_source].set_index(spread_source)['rate']), liability[spread_type])
        #rate_values = np.where(condition,liability['spread_source'].map(source_to_df[spread_source].set_index(spread_source)['rate']),liability[spread_type])
            # Assign the extracted rate values to the 'spread_type' column in 'liability2'
        #liability2[spread_type] = rate_values




#compresssion fo all the rows after drop
liability2 = liability2.groupby(['pool_id', 'pool_name','product_group','tenor_value','tenor_unit','base_ftp_rate']).max().reset_index()
#format_ans =  pd.concat([liability2, base_ftp_check], axis=0)

format_ans = liability2[['pool_id','pool_name','product_group','tenor_value','tenor_unit','base_ftp_rate','Credit spread','Liquidity premium']]



format_ans = format_ans[~((format_ans['Credit spread'].isna()) & (format_ans['Liquidity premium'].isna()))]

format_ans.dropna(subset=['tenor_value', 'tenor_unit'], how='all', inplace=True)


format_ans ['tenor_unit_num'] = format_ans['tenor_unit'].map({'M': 1, 'Y': 12})
format_ans['tenor_in_months'] = format_ans['tenor_value'] * format_ans['tenor_unit_num']
format_ans = format_ans.sort_values('tenor_in_months')

# If you want to drop the 'tenor_unit_num' and 'tenor_in_months' columns after sorting
format_ans = format_ans.drop(['tenor_unit_num', 'tenor_in_months'], axis=1)



format_ans['Credit spread'].fillna(0, inplace=True)
format_ans['Liquidity premium'].fillna(0, inplace=True)

format_ans['final_ftp'] = format_ans['base_ftp_rate'] + format_ans['Credit spread'] + format_ans['Liquidity premium']














































