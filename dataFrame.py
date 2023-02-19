import pandas as pd
import os
import json

# dataframe for aggregate transaction
transaction_path1="/content/pulse/data/aggregated/transaction/country/india/state/"
agg_state_list1=os.listdir(transaction_path1)
transaction_clm={'State':[], 'Year':[],'Quater':[],'Transacion_type':[], 'Transacion_count':[], 'Transacion_amount':[]}
for i in agg_state_list1:
    p1_i=transaction_path1+i+"/"
    Agg_yr1=os.listdir(p1_i)    
    for j in Agg_yr1:
        p1_j=p1_i+j+"/"
        Agg_yr_list=os.listdir(p1_j)        
        for k in Agg_yr_list:
            p1_k=p1_j+k
            Data=open(p1_k,'r')
            D=json.load(Data)
            for z in D['data']['transactionData']:
              Name=z['name']
              count=z['paymentInstruments'][0]['count']
              amount=z['paymentInstruments'][0]['amount']
              transaction_clm['Transacion_type'].append(Name)
              transaction_clm['Transacion_count'].append(count)
              transaction_clm['Transacion_amount'].append(amount)
              transaction_clm['State'].append(i)
              transaction_clm['Year'].append(j)
              transaction_clm['Quater'].append(int(k.strip('.json')))
Agg_Trans=pd.DataFrame(transaction_clm)

