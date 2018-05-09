# -*- coding: utf-8 -*-
"""
Created on Tue Jan 31 07:56:46 2017

Description: Populating and parsing from SUTs for later aggreggation

Scope: Modelling the Circular Economy in EEIO

@author:Franco Donati
@institution:Leiden University CML, TU Delft TPM
"""
from multiprocessing import Pool
import pickle as pk 
import pandas as pd
from pandas import DataFrame as df
from pandas import MultiIndex as mi

#==============================================================================
# Load mrSU tables and their extensions
#==============================================================================

V_ = pd.read_csv("SUTs/V3.3/mrSupply_3.3_2011.txt", sep="\t") #Supply
U_ = pd.read_csv("SUTs/V3.3/mrUse_3.3_2011.txt", sep="\t") #Use
Y_ = pd.read_csv("SUTs/V3.3/mrFinalDemand_3.3_2011.txt", sep="\t") # Final demand
E_ = pd.read_csv("SUTs/V3.3/mrFactorInputs_3.3_2011.txt", sep="\t") # Factor inputs

Be_ = pd.read_csv("SUTs/V3.3/mrEmissions_3.3_2011.txt", sep="\t") # Emissions
YBe_ = pd.read_csv("SUTs/V3.3/mrFDEmissions_3.3_2011.txt", sep="\t") # Final demand emissions
 
Br_ = pd.read_csv("SUTs/V3.3/mrResources_3.3_2011.txt", sep="\t") #Final demand resources
YBr_ = pd.read_csv("SUTs/V3.3/mrFDResources_3.3_2011.txt", sep="\t") #Final demand resources

Bm_ = pd.read_csv("SUTs/V3.3/mrMaterials_3.3_2011.txt", sep="\t") # Materials
YBm_ = pd.read_csv("SUTs/V3.3/mrFDMaterials_3.3_2011.txt", sep="\t") #Final demand materials

#==============================================================================
# Clean mrSUT
#==============================================================================

V = V_.iloc[1:,3:].apply(pd.to_numeric) #Supply
U = U_.iloc[1:,3:].apply(pd.to_numeric) #Use
Y = Y_.iloc[1:,3:].apply(pd.to_numeric) # Final demand
E = E_.iloc[1:,2:].apply(pd.to_numeric) # Factor inputs
Be = Be_.iloc[1:,3:].apply(pd.to_numeric) # Emissions
YBe = YBe_.iloc[1:,3:].apply(pd.to_numeric) # Final demand emissions
Br = Br_.iloc[1:,3:].apply(pd.to_numeric) #Final demand resources
YBr = YBr_.iloc[1:,3:].apply(pd.to_numeric) #Final demand resources
Bm = Bm_.iloc[1:,2:].apply(pd.to_numeric) # Materials
YBm = YBm_.iloc[1:,2:].apply(pd.to_numeric) #Final demand materials


# =============================================================================
# Load classifications
# =============================================================================

regions = pd.read_excel("resources/classifications3.0.13_3_dec_2016.xlsx", sheet_name= "countries")
substances = pd.read_excel("resources/classifications3.0.13_3_dec_2016.xlsx", sheet_name= "substances")
fact_inputs = pd.read_excel("resources/classifications3.0.13_3_dec_2016.xlsx", sheet_name= "factorinputtypes")
final_demand = pd.read_excel("resources/classifications3.0.13_3_dec_2016.xlsx", sheet_name= "finaldemandtypes")
materials = pd.read_excel("resources/classifications3.0.13_3_dec_2016.xlsx", sheet_name= "physicaltypes")
resources = pd.read_excel("resources/classifications3.0.13_3_dec_2016.xlsx", sheet_name= "extractions")
industries = pd.read_excel("resources/classifications3.0.13_3_dec_2016.xlsx", sheet_name= "industrytypes")
products = pd.read_excel("resources/classifications3.0.13_3_dec_2016.xlsx", sheet_name= "producttypes")


#==============================================================================
# reindexing 
#==============================================================================
def reg_labels(index):
    """ 
    ouputs df by region to use in indexing        
    """
    
    C = [] 
      
    for itr, val in index.iterrows():
        for name, values in regions.iterrows():
            cc = values.loc["CountryCode"] # country code
            cn = values.loc["CountryName"] # country name
            gc = values.loc["CountryGroup"] # country group code
            gn = values.loc["CountryGroupName"] # country group name
            if val.iloc[0].startswith(cc):
                if gc == "WE":
                    C.append(["EU",cc,cn,gn,gc])
                else:
                    C.append(["ROW",cc,cn,gn,gc])
                    
    output = df(C)
    
    output.columns = ["region", "country_code","country_name", "group_name", "group_code"]
            
    return(output)

def ind_labels(index):
    """
    Outputs all the labels according to industry categories
    """
    
    C = [] 
      
    for itr, val in index.iterrows():
        for name, values in industries.iterrows():
            icode = values.loc["IndustryTypeCode"] # industry code
            iname = values.loc["IndustryTypeName"] # industry name
            isyno = values.loc["IndustryTypeSynonym"]
            if val.iloc[1] == iname:
                C.append([icode,iname,isyno])
                    
    output = df(C)
    
    output.columns = ["code", "name", "synonym"]
            
    return(output)

def prod_labels(index):
    """
    Outputs all the labels according to product categories
    """
    
    C = [] 
      
    for itr, val in index.iterrows():
        for name, values in products.iterrows():
            icode = values.loc["ProductTypeCode"] # industry code
            iname = values.loc["ProductTypeName"] # industry name
            isyno = values.loc["ProductTypeSynonym"]
            if val.iloc[1] == iname:
                C.append([icode,iname,isyno])
                    
    output = df(C)
    
    output.columns = ["code", "name", "synonym"]
            
    return(output)

def Y_labels(index):
    """
    Outputs all the labels according to final demand categories
    """
    
    C = [] 
      
    for itr, val in index.iterrows():
        for name, values in final_demand.iterrows():
            fcode = values.loc["FinalDemandTypeCode"] # final demand code
            fname = values.loc["FinalDemandTypeName"] # name
            fsyno = values.loc["FinalDemandTypeSynonym"] # synonym
            if val.iloc[1] == fname:
                C.append([fcode,fname,fsyno])
                    
    output = df(C)
    
    output.columns = ["code", "name", "synonym"]
            
    return(output)
    
def Bmind_labels(index):
    """
    Outputs all the labels according to materials categories
    """
    
    C = [] 
      
    for itr, val in index.iterrows():
        for name, values in materials.iterrows():
            mname = values.loc["PhysicalTypeName"] # materials name
            msyno = values.loc["PhysicalTypeSynonym"] # materials synonym
            if val.iloc[0] == mname:
                C.append([mname,msyno])
                    
    output = df(C)
    
    output.columns = ["name", "synonym"]
            
    return(output)

def Eind_labels(index):
    """
    Outputs all the labels according to factor inputs categories
    """
    
    C = [] 
      
    for itr, val in index.iterrows():
        for name, values in fact_inputs.iterrows():
            ename = values.loc["FactorInputTypeName"] # name
            esyno = values.loc["FactorInputTypeSynonym"] # synonym
            ecode = values.loc["FactorInputTypeCode"] # code
            if val.iloc[0] == ename:
                C.append([ecode, ename, esyno])
                    
    output = df(C)
    
    output.columns = ["code","name", "synonym"]
            
    return(output)

def Brind_labels(index):
    """
    Outputs all the labels according to resources categories
    """
    
    C = [] 
      
    for itr, val in index.iterrows():
        for name, values in resources.iterrows():
            rname = values.loc["ExtractionTypeName"] # name
            rsyno = values.loc["ExtractionTypeSynonym"] # synonym
            if val.iloc[0] == rname:
                C.append([rname, rsyno])
                    
    output = df(C)
    
    output.columns = ["name", "synonym"]
            
    return(output)

def Beind_labels(index):
    """
    Outputs all the labels according to emissions categories
    """
    
    C = [] 
      
    for itr, val in index.iterrows():
        for name, values in substances.iterrows():
            scode = values.loc["SubstanceCode"] # name
            sname = values.loc["SubstanceName"] # name
            ssyno = values.loc["SubstanceSynonym"] # synonym
            if val.iloc[0] == sname:
                C.append([scode, sname, ssyno])
                    
    output = df(C)
    
    output.columns = ["code","name", "synonym"]
            
    return(output)

# Main Column names
Vcol_ = V_.iloc[0,3:].reset_index(drop = False) # column names of the Supply table from the txt files
Vcol_reg = reg_labels(Vcol_) # adding missing country and regional labels
Vcol_indu = ind_labels(Vcol_) # adding missing industry labels
V_columns = pd.concat([Vcol_reg, Vcol_indu, df(["M.EUR"] * len(Vcol_reg), columns = ["unit"])], axis = 1, ignore_index = False)

# Main index names
Vind_ = V_.iloc[1:,:3].reset_index(drop = True) # index labels of te supply table from the txt file
Vind_reg = reg_labels(Vind_) # adding missing country and regional labels
Vind_prod = prod_labels(Vind_) # adding missing products labels
V_index = pd.concat([Vind_reg, Vind_prod, df(["M.EUR"] * len(Vind_reg), columns = ["unit"])], axis = 1, ignore_index = False)

# Y Column names
Ycol_ = Y_.iloc[0,3:].reset_index(drop = False)
Ycol_reg = reg_labels(Ycol_) 
Ycol_lab = Y_labels(Ycol_)
Y_columns = pd.concat([Ycol_reg, Ycol_lab, df(["M.EUR"] * len(Ycol_reg), columns = ["unit"])], axis = 1, ignore_index = False)

# E index names
Eind_ = E_.iloc[1:,:2].reset_index(drop = True).fillna("")
Eind_lab = Eind_labels(Eind_)
Eind_unit = df(Eind_.iloc[:,1])
Eind_unit.columns = ["unit"]
E_index = pd.concat([Eind_lab, Eind_unit], axis = 1, ignore_index = False)

# Bm index names
Bmind_ = Bm_.iloc[1:,:2].reset_index(drop =True).fillna("") # Labels from materials extension table
Bmind_lab = Bmind_labels(Bmind_) # checking names and adding synonyms
Bmind_unit = df(Bmind_.iloc[:,1])
Bmind_unit.columns = ["unit"]
Bm_index = pd.concat([Bmind_lab, Bmind_unit], axis = 1, ignore_index = False)

# Br index names
Brind_ = Br_.iloc[1:,:3].reset_index(drop =True).fillna("")
Brind_lab = Brind_labels(Brind_)
Brind_unit = df(Brind_.iloc[:,2])
Brind_unit.columns = ["unit"]
Br_index = pd.concat([Brind_lab, Brind_unit], axis = 1, ignore_index = False)

# Be index names
Beind_ = Be_.iloc[1:,:3].reset_index(drop =True).fillna("")
Beind_lab = Beind_labels(Beind_)
Beind_unit = df(Beind_.iloc[:,2])
Beind_unit.columns = ["unit"]
Be_index = pd.concat([Beind_lab, Beind_unit], axis = 1, ignore_index = False)

#==============================================================================
# Assembly
#==============================================================================

# V assemble indeces
V.index = mi.from_arrays(V_index.values.T)
V.index.names = V_index.columns 
V.columns = mi.from_arrays(V_columns.values.T)
V.columns.names = V_columns.columns 

# U assemble indeces
U.index = mi.from_arrays(V_index.values.T)
U.index.names = V_index.columns 
U.columns = mi.from_arrays(V_columns.values.T)
U.columns.names = V_columns.columns 

# Y assemble indeces
Y.index = mi.from_arrays(V_index.values.T)
Y.index.names = V_index.columns
Y.columns = mi.from_arrays(Y_columns.values.T)
Y.columns.names = Y_columns.columns

# E assemble indeces
E.columns = mi.from_arrays(V_columns.values.T)
E.columns.names = V_columns.columns
E.columns = E.columns.droplevel(8) 
E.index = mi.from_arrays(E_index.values.T)
E.index.names =  E_index.columns

# YBe assemble indeces
YBe.columns = mi.from_arrays(Y_columns.values.T)
YBe.columns.names = Y_columns.columns
YBe.columns = YBe.columns.droplevel(8) # eliminating unit level
YBe.index = mi.from_arrays(Be_index.values.T)
YBe.index.names = Be_index.columns

# YBm assemble indeces
YBm.columns = mi.from_arrays(Y_columns.values.T)
YBm.columns.names = Y_columns.columns
YBm.columns = YBm.columns.droplevel(8) # eliminating unit level
YBm.index = mi.from_arrays(Bm_index.values.T)
YBm.index.names = Bm_index.columns

# YBr assemble indeces
YBr.columns = mi.from_arrays(Y_columns.values.T)
YBr.columns.names = Y_columns.columns
YBr.columns = YBr.columns.droplevel(8) # eliminating unit level
YBr.index = mi.from_arrays(Br_index.values.T)
YBr.index.names = Br_index.columns

# Be assemble indeces
Be.columns = mi.from_arrays(V_columns.values.T)
Be.columns.names = V_columns.columns
Be.columns = Be.columns.droplevel(8)
Be.index = mi.from_arrays(Be_index.values.T)
Be.index.names = Be_index.columns

# Br assemble indeces
Br.columns = mi.from_arrays(V_columns.values.T)
Br.columns.names = V_columns.columns
Br.columns = Br.columns.droplevel(8) # eliminating unit level
Br.index = mi.from_arrays(Br_index.values.T)
Br.index.names = Br_index.columns

# Bm assemble indeces
Bm.columns = mi.from_arrays(V_columns.values.T)
Bm.columns.names = V_columns.columns
Bm.columns = Bm.columns.droplevel(8) # eliminating unit level
Bm.index = mi.from_arrays(Bm_index.values.T)
Bm.index.names = Bm_index.columns

del(V_,Y_,E_, U_, Be_, YBe_, Br_, YBr_, Bm_, YBm_)
#==============================================================================
# Pickle data
#==============================================================================

def serialise(file_name): # "outputs/SUT.pkl reccomended 

     if __name__=="__main__":
        
        pool=Pool(processes=4)
        
        dic = {"V":V,
               "U":U,
               "Y":Y,
               "E":E,
               "Be":Be,
               "YBe":YBe,
               "Br":Br,
               "YBr":YBr,
               "Bm":Bm,
               "YBm":YBm
               }
                 
        w=open(file_name, "wb") #pickles SUT
        pk.dump(dic, w, 2) #pickling
        w.close()
        
        pool.close()
        pool.join()
        
        





