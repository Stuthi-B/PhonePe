import pandas as pd
import os
import json
import _mysql_connector as sql

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

# dataframe for map transaction
transaction_path3="/content/pulse/data/map/transaction/hover/country/india/state/"
map_state_list1=os.listdir(transaction_path3)
transaction_clm2={'State':[], 'Year':[],'Quater':[],'Transaction_location':[], 'Transaction_count':[], 'Transaction_amount':[]}
for i in map_state_list1:
    p3_i=transaction_path3+i+"/"
    Map_yr3=os.listdir(p3_i)    
    for j in Map_yr3:
        p3_j=p3_i+j+"/"
        Map_yr_list=os.listdir(p3_j)        
        for k in Map_yr_list:
            p3_k=p3_j+k
            Data=open(p3_k,'r')
            D=json.load(Data)
            for z in D['data']['hoverDataList']:
              Name=z['name']
              count=z['metric'][0]['count']
              amount=z['metric'][0]['amount']
              transaction_clm2['Transaction_location'].append(Name)
              transaction_clm2['Transaction_count'].append(count)
              transaction_clm2['Transaction_amount'].append(amount)
              transaction_clm2['State'].append(i)
              transaction_clm2['Year'].append(j)
              transaction_clm2['Quater'].append(int(k.strip('.json')))
Map_Trans=pd.DataFrame(transaction_clm2)

 
# dataframe for top transaction
transaction_path5="/content/pulse/data/top/transaction/country/india/state/"
top_state_list1=os.listdir(transaction_path5)
transaction_clm3={'State':[], 'Year':[],'Quater':[],'Transaction_state':[], 'Transaction_count':[], 'Transaction_amount':[]}
for i in top_state_list1:
    p5_i=transaction_path5+i+"/"
    Top_yr5=os.listdir(p5_i)    
    for j in Top_yr5:
        p5_j=p5_i+j+"/"
        Top_yr_list=os.listdir(p5_j)        
        for k in Top_yr_list:
            p5_k=p5_j+k
            Data=open(p5_k,'r')
            D=json.load(Data)
            try:
              for z in D['data']['districts']:
                print(z)
            except TypeError:
              pass
            finally:
                Name=z['entityName']
                Count=z['metric']['count']
                Amount=z['metric']['amount']
                transaction_clm3['Transaction_state'].append(Name)
                transaction_clm3['Transaction_count'].append(count)
                transaction_clm3['Transaction_amount'].append(amount)
                transaction_clm3['State'].append(i)
                transaction_clm3['Year'].append(j)
                transaction_clm3['Quater'].append(int(k.strip('.json')))

            
Top_Trans=pd.DataFrame(transaction_clm3)


# dataframe for top users
user_path6="/content/pulse/data/top/user/country/india/state/"
top_state_list2=os.listdir(user_path6)
user_clm3={'State':[], 'Year':[],'Quater':[],'User_location':[], 'Registered_users':[] }
for i in top_state_list2:
    p6_i=user_path6+i+"/"
    Top_yr6=os.listdir(p6_i)    
    for j in Top_yr6:
        p6_j=p6_i+j+"/"
        Top_yr_list=os.listdir(p6_j)        
        for k in Top_yr_list:
            p6_k=p6_j+k
            Data=open(p6_k,'r')
            D=json.load(Data)
            try:
              for z in D['data']['districts']:
                print(z)
            except TypeError:
              pass
            finally:    
              Name=z['name']
              Count=z['registeredUsers']
              user_clm3['User_location'].append(Name)
              user_clm3['Registered_users'].append(Count)
              user_clm3['State'].append(i)
              user_clm3['Year'].append(j)
              user_clm3['Quater'].append(int(k.strip('.json')))
Top_users=pd.DataFrame(user_clm3)

mydb=mysql.connect(host='localhost', database='phonepe', user='root', password='Kewal@123')
