from prefect import Task



class MOCKeys(Task):
    def yahoo_synbol(df, symbol_clmn):
        return df.assign(yahoo_tsx_symbol=df[symbol_clmn].apply(lambda x: f"{x}.TO"))

    def normalize_clmns(df, clmn_map)


    def run(self, )