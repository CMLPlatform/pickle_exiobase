# -*- coding: utf-8 -*-
"""
Created on Tue Jan 31 07:55:33 2017

Description: 

Scope: Modelling the Circular Economy in EEIO

@author:Franco Donati
@institution:Leiden University CML
"""
import pickle as pk
import pandas as pd
import numpy as np

#==============================================================================
# General mrSUTs Characteristics
#==============================================================================
countries_no = 49 # total number of countries and SUTs
EU_no = 31 # total number of EU countries
ROW_no = 18
reg_no = 2 # number of regions we want at the end EU & RoW
ind_no = 163 # number of industries in the SUTs
prod_no = 200 # number of products in the SUTs
Y_no = 7 # number of final demand categories in the SUTs
Be_no = 170 # number of environmental extensions

#==============================================================================
# Load serialised data
#==============================================================================

data = pd.read_pickle("SUTs/V3.3/parsed_SUT_V3.3.pkl") # load pickled data

#==============================================================================
#  Separated by region
#==============================================================================
def sep_b_reg(data):
    """separates SUTs by regions"""
    
    #load data
    V_ = data["V"] # Supply
    U_ = data["U"] # Use
    Y_ = data["Y"] # Final Demand
    E_ = data["E"] # Factor inputs
    
    Bm_ = data["Bm"] # Materials
    Br_ = data["Br"] # Resources
    Be_ = data["Be"] # Emissions
    
    YBm_ = data["YBm"] # Final Demand Materials
    YBr_ = data["YBr"] # Final Demand Resources
    YBm_ = data["YBm"] # Final Demand Materials
    YBe_ = data["YBe"] # Final Demand Emissions
    
    # separation section
    
    # Supply
    V1 = V_.loc[["EU"],["EU"]] # EU
    V2 = V_.loc[["EU"],["ROW"]] # export EU to ROW (import ROW from EU)
    V3 = V_.loc[["ROW"],["EU"]] # export ROW to EU (import EU from ROW)
    V4 = V_.loc[["ROW"],["ROW"]] # ROW
        
    # Use
    U1 = U_.loc[["EU"],["EU"]] # EU
    U2 = U_.loc[["EU"],["ROW"]] # export EU to ROW (import ROW from EU)
    U3 = U_.loc[["ROW"],["EU"]] # export ROW to EU (import EU from ROW)
    U4 = U_.loc[["ROW"],["ROW"]] # ROW
    
    # Final Demand
    Y1 = Y_.loc[["EU"],["EU"]] # EU
    Y2 = Y_.loc[["EU"],["ROW"]] # export EU to ROW (import ROW from EU)
    Y3 = Y_.loc[["ROW"],["EU"]] # export ROW to EU (import EU from ROW)
    Y4 = Y_.loc[["ROW"],["ROW"]] # ROW
    
    # Factor inputs
    E1 = E_.loc[:,["EU"]] # EU
    E2 = E_.loc[:,["ROW"]] # ROW
    
    # Materials
    Bm1 = Bm_.loc[:,["EU"]] # EU
    Bm2 = Bm_.loc[:,["ROW"]] # ROW
    
    # Resources
    Br1 = Br_.loc[:,["EU"]] # EU
    Br2 = Br_.loc[:,["ROW"]] # ROW
    
    # Final Demand Emissions
    Be1 = Be_.loc[:,["EU"]] # EU
    Be2 = Be_.loc[:,["ROW"]] # ROW
    
    # Final Demand Materials
    YBm1 = YBm_.loc[:,["EU"]] # EU
    YBm2 = YBm_.loc[:,["ROW"]] # ROW
    
    # Finald Demand Resources
    YBr1 = YBr_.loc[:,["EU"]] # EU
    YBr2 = YBr_.loc[:,["ROW"]] # ROW
    
    # Final Demand Emissions
    YBe1 = YBe_.loc[:,["EU"]] # EU
    YBe2 = YBe_.loc[:,["ROW"]] # ROW
      
    # preparing to output
    
    SUT =  {"V1":V1, # Supply EUtoEU
            "V2":V2, # Supply EUtoROW
            "V3":V3, # Supply ROWtoEU
            "V4":V4, # Supply ROWtoROW

            "U1":U1, # Supply EUtoEU
            "U2":U2, # Supply EUtoROW
            "U3":U3, # Supply ROWtoEU
            "U4":U4, # Supply ROWtoROW

            "Y1":Y1, # Final demand EU
            "Y2":Y2, # Final demand EU from exp to ROW
            "Y3":Y3, # Final demand ROW from exp to EU
            "Y4":Y4, # Final demand EU

            "E1":E1, # Factor inputs or primary inputs EU
            "E2":E2, # Factor inputs or primary inputs ROW

            "Be1":Be1, # Emissions EU
            "Be2":Be2, # Emissions ROW

            "YBe1":YBe1, # Final demand emissions EU
            "YBe2":YBe2, # Final demand emissions ROW

            "Br1":Br1, # Resources EU
            "Br2":Br2, # Resources ROW

            "YBr1":YBr1, # Final demand resources EU
            "YBr2":YBr2, # Final demand resources ROW

            "Bm1":Bm1, # Materials EU
            "Bm2":Bm2, # Materials ROW
            
            "YBm1":YBm1, # Final demand materials EU
            "YBm2":YBm2 # Final demand materials ROW
            }
            
    return(SUT)
  
#==============================================================================
# Separating by country and collecting
#==============================================================================

def split_n_agg(item, no_Cgroup1, no_Cgroup2="", axis = 0):
    """ takes dataframes in a list and splits them by country"""  
        
    def aggregate(the_list, no_Cgr):
        """ sums split dfs"""
        base = the_list[0]
        n = 1
        while n < no_Cgr:
            base = np.add(base, the_list[n])
            n += 1
        return (base)
    
    split1 = np.split(item, no_Cgroup1, axis)
    first = aggregate(split1, no_Cgroup1)
    
    try:
        split2 = np.split(first, no_Cgroup2, axis = 1)
        second = aggregate(split2, no_Cgroup2)
    except (ValueError, TypeError):
        return(first)
    else:
        return(second)
        
#==============================================================================
# Aggregation
#==============================================================================

def aggregate(data):
    """aggregates SUTs by regions EU, ROW"""
    
    #load data
    V1 = data["V1"] # Supply
    V2 = data["V2"]
    V3 = data["V3"]
    V4 = data["V4"]
    
    U1 = data["U1"] # Use
    U2 = data["U2"] 
    U3 = data["U3"] 
    U4 = data["U4"]
    
    Y1 = data["Y1"] # Final Demand
    Y2 = data["Y2"]
    Y3 = data["Y3"]
    Y4 = data["Y4"]
    
    E1 = data["E1"] # Factor inputs
    E2 = data["E2"]
        
    Bm1 = data["Bm1"] # Materials
    Bm2 = data["Bm2"]

    Br1 = data["Br1"] # Resources
    Br2 = data["Br2"]
    
    Be1 = data["Be1"] # Emissions
    Be2 = data["Be2"]
    
    YBm1 = data["YBm1"] # Final Demand Materials
    YBm2 = data["YBm2"]
    
    YBr1 = data["YBr1"] # Final Demand Resources
    YBr2 = data["YBr2"]    
    
    YBm1 = data["YBm1"] # Final Demand Materials
    YBm2 = data["YBm2"]    
    
    YBe1 = data["YBe1"] # Final Demand Emissions
    YBe2 = data["YBe2"]
    
    # Aggregation section
    
    # Supply
    V1a = split_n_agg(V1, EU_no, EU_no) # EU
    V2a = split_n_agg(V2, EU_no, ROW_no) # export EU to ROW (import ROW from EU)
    V3a = split_n_agg(V3, ROW_no, EU_no) # export ROW to EU (import EU from ROW)
    V4a = split_n_agg(V4, ROW_no, ROW_no) # ROW
    
    # Use
    U1a = split_n_agg(U1, EU_no, EU_no) # EU
    U2a = split_n_agg(U2, EU_no, ROW_no) # export EU to ROW (import ROW from EU)
    U3a = split_n_agg(U3, ROW_no, EU_no) # export ROW to EU (import EU from ROW)
    U4a = split_n_agg(U4, ROW_no, ROW_no) # ROW
      
    # Final Demand
    Y1a = split_n_agg(Y1, EU_no, EU_no) # EU
    Y2a = split_n_agg(Y2, EU_no, ROW_no) # EU from ROW
    Y3a = split_n_agg(Y3, ROW_no, EU_no) # EU
    Y4a = split_n_agg(Y4, ROW_no, ROW_no) # EU from ROW
    
    # Factor inputs
    E1a = split_n_agg(E1, EU_no, axis=1) # EU
    E2a = split_n_agg(E2, ROW_no, axis=1)  # ROW
    
    # Materials
    Bm1a = split_n_agg(Bm1, EU_no, axis=1) # EU
    Bm2a = split_n_agg(Bm2, ROW_no, axis=1)  # ROW
    
    # Resources
    Br1a = split_n_agg(Br1, EU_no, axis=1) # EU
    Br2a = split_n_agg(Br2, ROW_no, axis=1)  # ROW

    # Emissions
    Be1a = split_n_agg(Be1, EU_no, axis=1) # EU
    Be2a = split_n_agg(Be2, ROW_no, axis=1)  # ROW
    
    # Final Demand Materials
    YBm1a = split_n_agg(YBm1, EU_no, countries_no, axis = 1) # EU
    YBm2a = split_n_agg(YBm2, ROW_no, countries_no, axis = 1) # ROW

    # Finald Demand Resources
    YBr1a = split_n_agg(YBr1, EU_no, countries_no, axis = 1) # EU
    YBr2a = split_n_agg(YBr2, ROW_no, countries_no, axis = 1) # ROW

    # Final Demand Emissions
    YBe1a = split_n_agg(YBe1, EU_no, countries_no, axis = 1) # EU
    YBe2a = split_n_agg(YBe2, ROW_no, countries_no, axis = 1) # ROW
    
    # Reassemble aggregated SUT
   
    V12 = [V1a, V2a] # top quadrants
    V34 = [V3a,V4a] # bottom qudrants
    Vquad = [pd.concat(V12, axis = 1), pd.concat(V34, axis = 1)]
    V = pd.concat(Vquad, axis = 0) # supply
    V.columns = V.columns.droplevel([1,2,3,4])
    V.index = V.index.droplevel([1,2,3,4])
        
    U12 = [U1a,U2a]
    U34 = [U3a,U4a]
    Uquad = [pd.concat(U12, axis = 1), pd.concat(U34, axis = 1)]
    U = pd.concat(Uquad, axis = 0) # Use
    U.columns = U.columns.droplevel([1,2,3,4])
    U.index = U.index.droplevel([1,2,3,4])
    
    Y13 = [Y1a,Y3a]
    Y24 = [Y2a,Y4a]
    Yquad = [pd.concat(Y13, axis = 0), pd.concat(Y24, axis = 0)]
    Y = pd.concat(Yquad, axis = 1) # supply
    Y.columns = Y.columns.droplevel([1,2,3,4])
    Y.index = Y.index.droplevel([1,2,3,4])
    
    E = pd.concat([E1a,E2a], axis = 1) # Factor inputs
    E.columns = E.columns.droplevel([1,2,3,4])
    
    Bm = pd.concat([Bm1a,Bm2a], axis = 1) # Materials
    Bm.columns = Bm.columns.droplevel([1,2,3,4])
    
    Br = pd.concat([Br1a,Br2a], axis = 1) # Resources
    Br.columns = Br.columns.droplevel([1,2,3,4])
    
    Be = pd.concat([Be1a,Be2a], axis = 1) # Emissions
    Be.columns = Be.columns.droplevel([1,2,3,4])
  
    YBm = pd.concat([YBm1a,YBm2a], axis = 1) # Materials
    YBm.columns = YBm.columns.droplevel([1,2,3,4])
    
    YBr = pd.concat([YBr1a,YBr2a], axis = 1) # Resources
    YBr.columns = YBr.columns.droplevel([1,2,3,4])
    
    
    YBe = pd.concat([YBe1a,YBe2a], axis = 1) # Emissions
    YBe.columns = YBe.columns.droplevel([1,2,3,4])
    

    
    SUT =  {"V":V, # Supply
            "U":U, # Use
            "Y":Y, # Final demand
            "E":E, # Factor inputs or primary inputs
            "Be":Be, # Emissions
            "YBe":YBe, # Final demand emissions
            "Br":Br, # Resources
            "YBr":YBr, # Final demand resources
            "Bm":Bm, # Materials
            "YBm":YBm # Final demand materials
            }
            
    return (SUT)
          
          
def save_pkl(SUT, pickle_name): # recommended resources/mrSUT_EU_ROW.pkl
    """ Saves SUTs, IOT balance and SUT balance """
  
    w=open(pickle_name, "wb") #pickles SUT
    pk.dump(SUT, w, 2) #pickling

    w.close()


#==============================================================================
# Uncomment To execute 
#==============================================================================
data_s = sep_b_reg(data)
data_a = aggregate(data_s)
 
save = save_pkl(data_a, "SUTs/V3.3/mrSUT_EU_ROW_V3.3.pkl")




