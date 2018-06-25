# -*- coding: utf-8 -*-
"""
Created on Tue Jan 31 07:56:46 2017

Description: Parses EXIOBASE mrSUT V3.3 from CSV files, reconstructs all the
labels and pickles everything

@author:Franco Donati
@institution:Leiden University CML, TU Delft TPM
"""
import numpy
import pickle as pk
import pandas as pd
from multiprocessing import Pool
from pandas import DataFrame as df
from pandas import MultiIndex as mi
import energy_aggregate as enag


def load_mrSUT():
    """
    Load mrSUT, their extensions and characterisation factors

    """
    V = pd.read_csv("mrSupply_3.3_2011.txt", sep="\t")  # Supply
    U = pd.read_csv("mrUse_3.3_2011.txt", sep="\t")  # Use
    Y = pd.read_csv("mrFinalDemand_3.3_2011.txt", sep="\t")  # Final demand
    E = pd.read_csv("mrFactorInputs_3.3_2011.txt", sep="\t")  # Factor inputs

    Be = pd.read_csv("mrEmissions_3.3_2011.txt", sep="\t")  # Emissions
    YBe = pd.read_csv("mrFDEmissions_3.3_2011.txt", sep="\t")  # Y emissions

    Br = pd.read_csv("mrResources_3.3_2011.txt", sep="\t")  # Resources
    YBr = pd.read_csv("mrFDResources_3.3_2011.txt", sep="\t")  # Y resources

    Bm = pd.read_csv("mrMaterials_3.3_2011.txt", sep="\t")  # Materials
    YBm = pd.read_csv("mrFDMaterials_3.3_2011.txt", sep="\t")  # Y materials

    # Characterization tables
    charact = "characterisation_DESIRE_version3.3.xlsx"
    CrBe = pd.read_excel(charact, sheet_name="Q_emissions")  # Emissions
    CrBr = pd.read_excel(charact, sheet_name="Q_resources")  # Resources
    CrBm = pd.read_excel(charact, sheet_name="Q_materials")  # Materials
    CrE = pd.read_excel(charact, sheet_name="Q_factorinputs")  # Factor inputs

    output = {"V": V,
              "U": U,
              "Y": Y,
              "E": E,
              "Be": Be,
              "YBe": YBe,
              "Br": Br,
              "YBr": YBr,
              "Bm": Bm,
              "YBm": YBm,
              "CrBe": CrBe,
              "CrBr": CrBr,
              "CrBm": CrBm,
              "CrE": CrE,
              }

    return(output)


def get_mrSUT_values(data):
    """
    outputs only the values contained in the dataframes
    """

    V = data["V"].iloc[1:, 3:].apply(pd.to_numeric)  # Supply
    U = data["U"].iloc[1:, 3:].apply(pd.to_numeric)  # Use
    Y = data["Y"].iloc[1:, 3:].apply(pd.to_numeric)  # Final demand
    E = data["E"].iloc[1:, 2:].apply(pd.to_numeric)  # Factor inputs
    Be = data["Be"].iloc[1:, 3:].apply(pd.to_numeric)  # Emissions
    YBe = data["YBe"].iloc[1:, 3:].apply(pd.to_numeric)  # Y emissions
    Br = data["Br"].iloc[1:, 3:].apply(pd.to_numeric)  # Resources
    YBr = data["YBr"].iloc[1:, 3:].apply(pd.to_numeric)  # Y resources
    Bm = data["Bm"].iloc[1:, 2:].apply(pd.to_numeric)  # Materials
    YBm = data["YBm"].iloc[1:, 2:].apply(pd.to_numeric)  # Y materials

    # Characterisation factors
    CrBe = data["CrBe"].iloc[2:, 4:].apply(pd.to_numeric)
    CrBr = data["CrBr"].iloc[2:, :].apply(pd.to_numeric)
    CrBm = data["CrBm"].iloc[1:, :].apply(pd.to_numeric)
    CrE = data["CrE"].iloc[1:, 4:].apply(pd.to_numeric)

    output = {"V": V,
              "U": U,
              "Y": Y,
              "E": E,
              "Be": Be,
              "YBe": YBe,
              "Br": Br,
              "YBr": YBr,
              "Bm": Bm,
              "YBm": YBm,
              "CrBe": CrBe,
              "CrBr": CrBr,
              "CrBm": CrBm,
              "CrE": CrE,
              }

    return(output)


def labels(file):
    """
    Loads label classifications
    """
    reg = pd.read_excel(file, sheet_name="countries")
    subs = pd.read_excel(file, sheet_name="substances")
    fc_inp = pd.read_excel(file, sheet_name="factorinputtypes")
    fin_dem = pd.read_excel(file, sheet_name="finaldemandtypes")
    mat = pd.read_excel(file, sheet_name="physicaltypes")
    res = pd.read_excel(file, sheet_name="extractions")
    ind = pd.read_excel(file, sheet_name="industrytypes")
    prod = pd.read_excel(file, sheet_name="producttypes")

    output = {"reg": reg,
              "subs": subs,
              "fc_inp": fc_inp,
              "fin_dem": fin_dem,
              "mat": mat,
              "res": res,
              "ind": ind,
              "prod": prod
              }

    return(output)


# reindexing


def reg_labels(index, reg_classi):
    """
    ouputs df by region to use in indexing
    """

    C = []

    for itr, val in index.iterrows():
        for name, values in reg_classi.iterrows():
            cc = values.loc["CountryCode"]  # country code
            cn = values.loc["CountryName"]  # country name
            gc = values.loc["CountryGroup"]  # country group code
            gn = values.loc["CountryGroupName"]  # country group name
            if val.iloc[0].startswith(cc):
                if gc == "WE":
                    C.append(["EU", cc, cn, gn, gc])
                else:
                    C.append(["ROW", cc, cn, gn, gc])

    output = df(C)

    output.columns = ["region", "country_code", "country_name",
                      "CountryGroup", "CountryGroupName"]

    return(output)


def ind_labels(index, ind_classi):
    """
    Outputs all the labels according to industry categories
    """

    C = []

    for itr, val in index.iterrows():
        for name, values in ind_classi.iterrows():
            icode = values.loc["IndustryTypeCode"]  # industry code
            iname = values.loc["IndustryTypeName"]  # industry name
            isyno = values.loc["IndustryTypeSynonym"]  # ind synonym
            if val.iloc[1] == iname:
                C.append([icode, iname, isyno])

    output = df(C)

    output.columns = ["code", "name", "synonym"]

    return(output)


def prod_labels(index, prod_classi):
    """
    Outputs all the labels according to product categories
    """

    C = []

    for itr, val in index.iterrows():
        for name, values in prod_classi.iterrows():
            icode = values.loc["ProductTypeCode"]  # industry code
            iname = values.loc["ProductTypeName"]  # industry name
            isyno = values.loc["ProductTypeSynonym"]
            if val.iloc[1] == iname:
                C.append([icode, iname, isyno])

    output = df(C)

    output.columns = ["code", "name", "synonym"]

    return(output)


def Y_labels(index, Y_classi):
    """
    Outputs all the labels according to final demand categories
    """

    C = []

    for itr, val in index.iterrows():
        for name, values in Y_classi.iterrows():
            fcode = values.loc["FinalDemandTypeCode"]  # final demand code
            fname = values.loc["FinalDemandTypeName"]  # name
            fsyno = values.loc["FinalDemandTypeSynonym"]  # synonym
            if val.iloc[1] == fname:
                C.append([fcode, fname, fsyno])

    output = df(C)

    output.columns = ["code", "name", "synonym"]

    return(output)


def Bmind_labels(index, Bm_classi):
    """
    Outputs all the labels according to materials categories
    """

    C = []

    for itr, val in index.iterrows():
        for name, values in Bm_classi.iterrows():
            mname = values.loc["PhysicalTypeName"]  # materials name
            msyno = values.loc["PhysicalTypeSynonym"]  # materials synonym
            if val.iloc[0] == mname:
                C.append([mname, msyno])

    output = df(C)

    output.columns = ["name", "synonym"]

    return(output)


def Eind_labels(index, E_classi):
    """
    Outputs all the labels according to factor inputs categories
    """

    C = []

    for itr, val in index.iterrows():
        for name, values in E_classi.iterrows():
            ename = values.loc["FactorInputTypeName"]  # name
            esyno = values.loc["FactorInputTypeSynonym"]  # synonym
            ecode = values.loc["FactorInputTypeCode"]  # code
            if val.iloc[0] == ename:
                C.append([ecode, ename, esyno])

    output = df(C)

    output.columns = ["code", "name", "synonym"]

    return(output)


def Brind_labels(index, Br_classi):
    """
    Outputs all the labels according to resources categories
    """

    C = []

    for itr, val in index.iterrows():
        for name, values in Br_classi.iterrows():
            rname = values.loc["ExtractionTypeName"]  # name
            rsyno = values.loc["ExtractionTypeSynonym"]  # synonym
            if val.iloc[0] == rname:
                C.append([rname, rsyno])

    output = df(C)

    output.columns = ["name", "synonym"]

    return(output)


def Beind_labels(index, Be_classi):
    """
    Outputs all the labels according to emissions categories
    """

    C = []

    for itr, val in index.iterrows():
        for name, values in Be_classi.iterrows():
            scode = values.loc["SubstanceCode"]  # name
            sname = values.loc["SubstanceName"]  # name
            ssyno = values.loc["SubstanceSynonym"]  # synonym
            if val.iloc[0] == sname:
                C.append([scode, sname, ssyno])

    output = df(C)

    output.columns = ["code", "name", "synonym"]

    return(output)


# begin
def load_in(aggregate_energy=False):
    """
    Loads in all data
    """
    file = "classifications3.0.13_3_dec_2016.xlsx"

    if __name__ == "__main__":
        pool = Pool(processes=4)
        data = load_mrSUT()
        vals = get_mrSUT_values(data)
        lab = labels(file)

        # Industries' labels
        Vcol_ = data["V"].iloc[0, 3:].reset_index(drop=False)  # index labels
        Vcol_reg = reg_labels(Vcol_, lab["reg"])  # add country and reg. labels
        Vcol_indu = ind_labels(Vcol_, lab["ind"])  # adding industry labels
        industries = pd.concat([Vcol_reg, Vcol_indu,
                                df(["M.EUR"] * len(Vcol_reg),
                                   columns=["unit"])], axis=1,
                               ignore_index=False)

        # Products' labels
        Vind_ = data["V"].iloc[1:, :3].reset_index(drop=True)
        Vind_reg = reg_labels(Vind_, lab["reg"])
        Vind_prod = prod_labels(Vind_, lab["prod"])
        products = pd.concat([Vind_reg, Vind_prod,
                              df(["M.EUR"] * len(Vind_reg), columns=["unit"])],
                             axis=1, ignore_index=False)

        # Final consumption' labels
        Ycol_ = data["Y"].iloc[0, 3:].reset_index(drop=False)
        Ycol_reg = reg_labels(Ycol_, lab["reg"])
        Ycol_lab = Y_labels(Ycol_, lab["fin_dem"])
        final_cons = pd.concat([Ycol_reg, Ycol_lab,
                                df(["M.EUR"] * len(Ycol_reg),
                                   columns=["unit"])], axis=1,
                               ignore_index=False)

        # E index names
        Eind_ = data["E"].iloc[1:, :2].reset_index(drop=True).fillna("")
        Eind_lab = Eind_labels(Eind_, lab["fc_inp"])
        Eind_unit = df(Eind_.iloc[:, 1])
        Eind_unit.columns = ["unit"]
        fac_inp = pd.concat([Eind_lab, Eind_unit], axis=1, ignore_index=False)

        # Bm index names
        Bmind_ = data["Bm"].iloc[1:, :2].reset_index(drop=True).fillna("")
        Bmind_lab = Bmind_labels(Bmind_, lab["mat"])
        Bmind_unit = df(Bmind_.iloc[:, 1])
        Bmind_unit.columns = ["unit"]
        mater = pd.concat([Bmind_lab, Bmind_unit], axis=1, ignore_index=False)

        # Br index names
        Brind_ = data["Br"].iloc[1:, :3].reset_index(drop=True).fillna("")
        Brind_lab = Brind_labels(Brind_, lab["res"])
        Brind_unit = df(Brind_.iloc[:, 2])
        Brind_unit.columns = ["unit"]
        resour = pd.concat([Brind_lab, Brind_unit], axis=1, ignore_index=False)

        # Be index names
        Beind_ = data["Be"].iloc[1:, :3].reset_index(drop=True).fillna("")
        Beind_lab = Beind_labels(Beind_, lab["subs"])
        Beind_unit = df(Beind_.iloc[:, 2])
        Beind_unit.columns = ["unit"]
        emis = pd.concat([Beind_lab, Beind_unit], axis=1, ignore_index=False)

        # Characterisation factors
        CrBe_l = data["CrBe"].iloc[2:, :4]
        CrBe_l.columns = ["impact_method", "characterization",
                          "reference", "unit"]

        CrBr_l = data["CrBr"].index.to_frame(False).iloc[2]
        CrBr_l.index = ["characterization", "unit"]

        CrBm_l = data["CrBm"].index.to_frame(False).iloc[1:, :]
        CrBm_l.columns = ["characterization", "unit"]

        CrE_l = data["CrE"].index.to_frame(False).iloc[1:, :2]
        CrE_l.columns = ["characterization", "unit"]

        del(data)

        # Assembly

        # V assemble indeces
        V = vals["V"]
        V.index = mi.from_arrays(products.values.T)
        V.index.names = products.columns
        V.columns = mi.from_arrays(industries.values.T)
        V.columns.names = industries.columns

        # U assemble indeces
        U = vals["U"]
        U.index = mi.from_arrays(products.values.T)
        U.index.names = products.columns
        U.columns = mi.from_arrays(industries.values.T)
        U.columns.names = industries.columns

        # Y assemble indeces
        Y = vals["Y"]
        Y.index = mi.from_arrays(products.values.T)
        Y.index.names = products.columns
        Y.columns = mi.from_arrays(final_cons.values.T)
        Y.columns.names = final_cons.columns

        # E assemble indeces
        E = vals["E"]
        E.columns = mi.from_arrays(industries.values.T)
        E.columns.names = industries.columns
        E.columns = E.columns.droplevel(8)
        E.index = mi.from_arrays(fac_inp.values.T)
        E.index.names = fac_inp.columns

        # YBe assemble indeces
        YBe = vals["YBe"]
        YBe.columns = mi.from_arrays(final_cons.values.T)
        YBe.columns.names = final_cons.columns
        YBe.columns = YBe.columns.droplevel(8)  # eliminating unit level
        YBe.index = mi.from_arrays(emis.values.T)
        YBe.index.names = emis.columns

        # YBm assemble indeces
        YBm = vals["YBm"]
        YBm.columns = mi.from_arrays(final_cons.values.T)
        YBm.columns.names = final_cons.columns
        YBm.columns = YBm.columns.droplevel(8)  # eliminating unit level
        YBm.index = mi.from_arrays(mater.values.T)
        YBm.index.names = mater.columns

        # YBr assemble indeces
        YBr = vals["YBr"]
        YBr.columns = mi.from_arrays(final_cons.values.T)
        YBr.columns.names = final_cons.columns
        YBr.columns = YBr.columns.droplevel(8)  # eliminating unit level
        YBr.index = mi.from_arrays(resour.values.T)
        YBr.index.names = resour.columns

        # Be assemble indeces
        Be = vals["Be"]
        Be.columns = mi.from_arrays(industries.values.T)
        Be.columns.names = industries.columns
        Be.columns = Be.columns.droplevel(8)
        Be.index = mi.from_arrays(emis.values.T)
        Be.index.names = emis.columns

        # Br assemble indeces
        Br = vals["Br"]
        Br.columns = mi.from_arrays(industries.values.T)
        Br.columns.names = industries.columns
        Br.columns = Br.columns.droplevel(8)  # eliminating unit level
        Br.index = mi.from_arrays(resour.values.T)
        Br.index.names = resour.columns

        # Bm assemble indeces
        Bm = vals["Bm"]
        Bm.columns = mi.from_arrays(industries.values.T)
        Bm.columns.names = industries.columns
        Bm.columns = Bm.columns.droplevel(8)  # eliminating unit level
        Bm.index = mi.from_arrays(mater.values.T)
        Bm.index.names = mater.columns

        # Characterisation factors
        # Emissions
        CrBe = vals["CrBe"]
        CrBe.index = mi.from_arrays(CrBe_l.values.T)
        CrBe.index.names = CrBe_l.columns

        # Resources
        CrBr = vals["CrBr"]
        CrBr.index.names = CrBr_l.index

        # Materials
        CrBm = vals["CrBm"]
        CrBm.index = mi.from_arrays(CrBm_l.values.T)
        CrBm.index.names = CrBm_l.columns

        # Factor inputs
        CrE = vals["CrE"]
        CrE.index = mi.from_arrays(CrE_l.values.T)
        CrE.index.names = CrE_l.columns

        del(vals)

        if aggregate_energy is True:

            ni_lab = df(["Nature Inputs", "NI.tot", "TJ"])
            erec_lab = df(["Emission Relevant Energy Carrier", "EnER.tot",
                           "TJ"])
            ecs_lab = df(["Energy Carrier Supply", "EnS.tot", "TJ"])
            ecu_lab = df(["Energy Carrier Use", "EnU.tot", "TJ"])

            en_agg_lab = {"Nature Inputs": ni_lab,
                          "Emission Relevant Energy Carrier": erec_lab,
                          "Energy Carrier Supply": ecs_lab,
                          "Energy Carrier Use": ecu_lab
                          }

            keys = enag.names.keys()
            for l in keys:
                values = enag.names[l]
                CrBm = aggregate(CrBm, values, l, True).T
                labl = en_agg_lab[l]
                labl.index = ["name", "synonym", "unit"]
                Bm = aggregate(Bm, values, labl)  # it is
                YBm = aggregate(YBm, values, labl)  # it is

        pool.close()
        pool.join()

        output = {"V": V,
                  "U": U,
                  "Y": Y,
                  "E": E,
                  "Be": Be,
                  "YBe": YBe,
                  "Br": Br,
                  "YBr": YBr,
                  "Bm": Bm,
                  "YBm": YBm,
                  "CrBe": CrBe,
                  "CrBr": CrBr,
                  "CrBm": CrBm,
                  "CrE": CrE,
                  }

        return(output)


def aggregate(matrix, enag_group, new_label, characterisation=False):

    if characterisation is True:
        matrix = matrix.T
    else:
        pass

    aggregate = df(numpy.sum(matrix.loc[enag_group, :], axis=0))

    if characterisation is True:
        aggregate = aggregate/len(enag_group)
        aggregate.columns = [new_label]
    else:
        aggregate.columns = mi.from_arrays(new_label.values)

    dropped = matrix.drop(enag_group)

    output = dropped.append(aggregate.T)

    return(output)


def serialise(data, file_name):  # "outputs/SUT.pkl reccomended
    if __name__ == "__main__":

        pool = Pool(processes=4)

        w = open(file_name, "wb")  # pickles SUT
        pk.dump(data, w, 2)  # pickling
        w.close()

        pool.close()
        pool.join()
