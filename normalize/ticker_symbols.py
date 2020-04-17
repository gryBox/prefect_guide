from prefect import Task

import pandas as pd

import humps

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class MapTickerSymbols(Task):
    def __init__(
        self,
        symbol_clmn_nm="Symbol",
        yhoo_sym_clmn_nm="yahoo_symbol",
        tsx_sym_clmn_nm="tsx_symbol",
        prfrd_pattern=".PR."
    ):
        super().__init__()

        self.symbol_clmn_nm = symbol_clmn_nm
        self.yhoo_sym_clmn_nm = yhoo_sym_clmn_nm
        self.tsx_sym_clmn_nm = tsx_sym_clmn_nm
        self.prfrd_pattern = prfrd_pattern

    def map_tsx_to_yhoo_sym(self, tsx_sym):
        #print(tsx_sym)
        # Check for prefereds 
        if tsx_sym.find(self.prfrd_pattern)!=-1:
            #print(tsx_sym)
            pr_parts = tsx_sym.partition(self.prfrd_pattern)
            yhoo_sym = f"{pr_parts[0]}-{pr_parts[1][1]}{pr_parts[2]}.TO"
            #print(yhoo_sym)
        else:
            # Replace equity extensions (i.e. UN, PR)
            yhoo_sym = tsx_sym.replace(".", "-")

            # Add yahoo TSX key
            yhoo_sym = f"{yhoo_sym}.TO"

        return yhoo_sym
    
    def run(self, df):
         # 1. Map TSX symbols to yhoo
        df[self.yhoo_sym_clmn_nm] = df[self.symbol_clmn_nm].apply(self.map_tsx_to_yhoo_sym)
        df.rename(columns={self.symbol_clmn_nm: self.tsx_sym_clmn_nm}, inplace=True)

        # 2. Normalize columns
        df.rename(
            columns=lambda col_nm: humps.decamelize(col_nm).replace(" ",""), 
            inplace=True
        )


        return df