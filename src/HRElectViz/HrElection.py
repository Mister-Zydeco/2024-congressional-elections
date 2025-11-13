import polars as pl

import HRElectViz.ushelper as ush

# The District field from the scrape of Hose Clerk data is a two-digit
# string (district number) for states with more than one district; the
# string 'AT LARGE' for states with one district; and the string
# 'DELEGATE' for all non-voting territories except Puerto Rico, for
# which the string 'RESIDENT COMMISSIONER' is used.

SD_COLS = ['State\nCode', 'District\nNumber']

# The Republican entry in the dictionary below is a hack to accommodate
# a comma-naive preprocessing that transformed "Republican, Libertarian"
# into "Republican  Libertarian"(sic).
#


def x_is_affiliate_of(major_party: str) -> pl.Expr:
    party_affiliate_names = {
        'Democrat': ['Democrat', 'Democratic-Farmer-Labor',
                     'Democratic-Nonpartisan League'],
        'Republican': ['Republican', 'Republican  Libertarian'],
    }
    return pl.col('Party').is_in(party_affiliate_names[major_party])


major_party_selector: pl.Expr = (
    pl.when(x_is_affiliate_of('Democrat'))
    .then(pl.lit('Democrat'))
    .otherwise(
        pl.when(x_is_affiliate_of('Republican'))
        .then(pl.lit('Republican'))
        .otherwise(pl.col('Party'))
    )
)
lower48_selector: pl.Expr = pl.col('State\nCode').is_in(ush.lower48_abbrs)


def std_polars_config() -> pl.Config:
    return pl.Config(
        tbl_rows=-1,
        tbl_cols=-1,
        thousands_separator=',',
        tbl_width_chars=150,
        tbl_cell_alignment='LEFT',
        tbl_cell_numeric_alignment='RIGHT',
    )


class HrElection:
    def __init__(self):
        is_at_large_x = pl.col('State\nCode').is_in(ush.at_large_states_abbrs)
        is_territory_x = pl.col('State\nCode').is_in(ush.territory_abbrs)

        house_clerk_csv_path = '../../election-data/elections2024.csv'
        self.dfs = {}
        df = (
            pl.read_csv(house_clerk_csv_path)
            .with_columns(
                pl.col('StateTerritory')
                .replace(ush.ucname_to_abbr)
                .alias('State\nCode'),
                pl.col('StateTerritory')
                .replace(ush.ucname_to_fips)
                .alias('state_fips'),
            )
            .with_columns(
                pl.when(is_at_large_x | is_territory_x)
                .then(pl.lit('1'))
                .otherwise(pl.col('District'))
                .alias('d_number_'),
                pl.when(is_at_large_x)
                .then(pl.lit('00'))
                .otherwise(
                    pl.when(is_territory_x)
                    .then(pl.lit('98'))
                    .otherwise(pl.col('District').str.zfill(2))
                )
                .alias('district_fips'),
            )
        )
        df = df.with_columns(
            pl.col('d_number_').cast(pl.Int8).alias('District\nNumber'),
        )
        self.dfs['states_and_territories'] = df
        self.dfs['states'] = df.filter(
            pl.col('State\nCode').is_in(ush.state_abbrs)
        )

    def get_ndistricts_per_state(self) -> pl.DataFrame:
        df: pl.DataFrame = (
            self.dfs['states'].select(SD_COLS).unique()
            .group_by('State\nCode')
            .agg(pl.len().alias('Number of\nDistricts'))
        )
        self.dfs['districts_per_state'] = df
        return df

    def get_districts_ranked_by_vote(self) -> pl.DataFrame:
        df: pl.DataFrame = (
            self.dfs['states']
            .sort(SD_COLS + ['Vote'], descending=[False, False, True])
            .select(
                SD_COLS
                + ['state_fips', 'district_fips', 'Party', 'Name', 'Vote']
            )
        )
        self.dfs['districts_ranked_by_vote'] = df
        return df

    def get_district_winners(self) -> pl.DataFrame:
        if 'districts_ranked_by_vote' not in self.dfs:
            self.get_districts_ranked_by_vote()
        df: pl.DataFrame = (
            self.dfs['districts_ranked_by_vote']
            .group_by(SD_COLS, maintain_order=True)
            .first()
            .select(SD_COLS + ['state_fips', 'district_fips', 'Party', 'Name'])
        )
        print('district_winners columns', df.columns)
        print(df)
        self.dfs['district_winners'] = df
        return df

    def get_district_major_party_vote(self) -> pl.DataFrame:
        if 'districts_ranked_by_vote' not in self.dfs:
            self.get_districts_ranked_by_vote()
        df: pl.DataFrame = (
            self.dfs['districts_ranked_by_vote']
            .select(['State\nCode', 'District\nNumber', 'Party', 'Vote'])
            .with_columns(major_party_selector.alias('Party'))
            .group_by(
                ['State\nCode', 'District\nNumber', 'Party'],
                maintain_order=True,
            )
            .sum()
            .filter(pl.col('Party').is_in(['Republican', 'Democrat']))
            .pivot('Party', index=SD_COLS)
            .fill_null(0)
        )
        self.dfs['district_major_party_vote'] = df
        return df

    def get_district_winners_with_major_party(self) -> pl.DataFrame:
        if 'district_winners' not in self.dfs:
            self.get_district_winners()
        df: pl.DataFrame = self.dfs['district_winners'].with_columns(
            major_party_selector.alias('Party')
        )
        self.dfs['district_winners_with_major_party'] = df
        return df

    def get_state_nwinners_by_party(self) -> pl.DataFrame:
        x_total_delegates = pl.col('Republican') + pl.col('Democrat')
        if 'district_winners_with_major_party' not in self.dfs:
            self.get_district_winners_with_major_party()
        df: pl.DataFrame = (
            self.dfs['district_winners_with_major_party']
            .group_by(['State\nCode', 'Party'], maintain_order=True)
            .len()
            .pivot('Party', index=['State\nCode'], values='len')
            .fill_null(0)
        )
        df = df.with_columns(
            pl.col('State\nCode').replace(ush.abbr_to_fips).alias('state_fips'),
            ((pl.col('Republican') * 100) / x_total_delegates)
            .round(1)
            .alias('Republican\ndelegate %'),
            ((pl.col('Democrat') * 100) / x_total_delegates)
            .round(1)
            .alias('Democrat\ndelegate %'),
        ).select(
            pl.col('State\nCode'),
            pl.col('state_fips'),
            pl.col('Republican').alias('Republican\ndelegate\ncount'),
            pl.col('Democrat').alias('Democrat\ndelegate\ncount'),
            pl.col('Republican\ndelegate %'),
            pl.col('Democrat\ndelegate %'),
        )
        print('state_nwinners_by_party', df.columns)
        self.dfs['state_nwinners_by_party'] = df
        return df

    def get_aggregate_vote_by_state(self) -> pl.DataFrame:
        df: pl.DataFrame = (
            self.dfs['states']
            .group_by('State\nCode', maintain_order=True)
            .agg(
                [
                    pl.col('Vote')
                    .filter(x_is_affiliate_of('Democrat'))
                    .sum()
                    .alias('Democrat\nVote'),
                    pl.col('Vote')
                    .filter(x_is_affiliate_of('Republican'))
                    .sum()
                    .alias('Republican\nVote'),
                    pl.col('Vote').sum().alias('Vote\nAll\nParties'),
                ]
            )
            .with_columns(
                (pl.col('Democrat\nVote') + pl.col('Republican\nVote'))
                .sum()
                .alias('Major\nParty\nVote'),
                (
                    (pl.col('Democrat\nVote') / pl.col('Vote\nAll\nParties'))
                    * 100
                )
                .round(1)
                .alias('Democrat\nVote %'),
                (
                    (pl.col('Republican\nVote') / pl.col('Vote\nAll\nParties'))
                    * 100
                )
                .round(1)
                .alias('Republican\nVote %'),
            )
        )
        self.dfs['aggregate_vote_by_state'] = df
        return df

    def get_aggregate_vote_by_district(self) -> pl.DataFrame:
        df: pl.DataFrame = (
            self.dfs['states']
            .group_by(SD_COLS, maintain_order=True)
            .agg(
                [
                    pl.col('Vote')
                    .filter(x_is_affiliate_of('Democrat'))
                    .sum()
                    .alias('Democrat\nVote'),
                    pl.col('Vote')
                    .filter(x_is_affiliate_of('Republican'))
                    .sum()
                    .alias('Republican\nVote'),
                ]
            )
            .with_columns(
                (pl.col('Democrat\nVote') + pl.col('Republican\nVote')).alias(
                    'Vote\nBoth\nParties'
                )
            )
            .with_columns(
                (
                    (
                        pl.col('Democrat\nVote')
                        / pl.col('Vote\nBoth\nParties')
                        * 100
                    )
                    .round(1)
                    .alias('Democrat\nVote %')
                ),
                (
                    (
                        pl.col('Republican\nVote')
                        / pl.col('Vote\nBoth\nParties')
                        * 100
                    )
                    .round(1)
                    .alias('Republican\nVote %')
                ),
            )
        )
        self.dfs['aggregate_vote_by_district'] = df
        return df
