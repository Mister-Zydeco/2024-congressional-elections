from datetime import datetime
import polars as pl
import hrelectviz.ushelper as ush


def get_most_recent_house_election_year() -> int:
    year: int = datetime.now().year
    return year - 2 if year % 2 == 0 else year - 1

# The District field from the scrape of Hose Clerk data is a two-digit
# string (district number) for states with more than one district; the
# string 'AT LARGE' for states with one district; and the string
# 'DELEGATE' for all non-voting territories except Puerto Rico, for
# which the string 'RESIDENT COMMISSIONER' is used.

SD_COLS = ['State\nAbbr', 'District\nNumber']

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
lower48_selector: pl.Expr = pl.col('State\nAbbr').is_in(ush.lower48_abbrs)


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
    def __init__(self, year=2024):
        is_at_large_x = pl.col('State\nAbbr').is_in(ush.at_large_states_abbrs)
        is_territory_x = pl.col('State\nAbbr').is_in(ush.territory_abbrs)

        house_clerk_csv_path = f'./election-data/elections{year}.csv'
        self.dfs = {}
        df = (
            pl.read_csv(house_clerk_csv_path)
            .with_columns(
                pl.col('StateTerritory')
                .replace(ush.ucname_to_abbr)
                .alias('State\nAbbr'),
                pl.col('StateTerritory')
                .replace(ush.ucname_to_fips)
                .alias('State\nFIPS'),
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
                .alias('District\nFIPS'),
            )
        )
        df = df.with_columns(
            pl.col('d_number_').cast(pl.Int8).alias('District\nNumber'),
        )
        self.dfs['states_and_territories'] = df
        self.dfs['states'] = df.filter(
            pl.col('State\nAbbr').is_in(ush.state_abbrs)
        )

    def get_ndistricts_per_state(self) -> pl.DataFrame:
        df: pl.DataFrame = (
            self.dfs['states_and_territories'].select(SD_COLS).unique()
            .group_by('State\nAbbr')
            .agg(pl.len().alias('Number of\nDistricts'))
        )
        self.dfs['districts_per_state'] = df
        return df

    def get_districts_ranked_by_vote(self) -> pl.DataFrame:
        df: pl.DataFrame = (
            self.dfs['states_and_territories']
            .sort(SD_COLS + ['Vote'], descending=[False, False, True])
            .select(
                SD_COLS
                + ['State\nFIPS', 'District\nFIPS', 'Party', 'Name', 'Vote']
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
            .select(SD_COLS + ['State\nFIPS', 'District\nFIPS', 'Party', 'Name'])
        )
        self.dfs['district_winners'] = df
        return df

    def get_district_major_party_vote(self) -> pl.DataFrame:
        if 'districts_ranked_by_vote' not in self.dfs:
            self.get_districts_ranked_by_vote()
        df: pl.DataFrame = (
            self.dfs['districts_ranked_by_vote']
            .select(['State\nAbbr', 'District\nNumber', 'Party', 'Vote'])
            .with_columns(major_party_selector.alias('Party'))
            .group_by(
                ['State\nAbbr', 'District\nNumber', 'Party'],
                maintain_order=True,
            )
            .sum()
            .filter(pl.col('Party').is_in(['Republican', 'Democrat']))
            .pivot('Party', index=SD_COLS)
            .fill_null(0)
        ).rename({x: f'Total vote for\n{x} candidates'
                 for x in ['Democrat', 'Republican']})
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
            .group_by(['State\nAbbr', 'Party'], maintain_order=True)
            .len()
            .pivot('Party', index=['State\nAbbr'], values='len')
            .fill_null(0)
        )
        df = df.with_columns(
            pl.col('State\nAbbr').replace(ush.abbr_to_fips).alias('State\nFIPS'),
            ((pl.col('Republican') * 100) / x_total_delegates)
            .round(1)
            .alias('Republican\ndelegate %'),
            ((pl.col('Democrat') * 100) / x_total_delegates)
            .round(1)
            .alias('Democrat\ndelegate %'),
        ).select(
            pl.col('State\nAbbr'),
            pl.col('State\nFIPS'),
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
            self.dfs['states_and_territories']
            .group_by('State\nAbbr', maintain_order=True)
            .agg(
                [
                    pl.col('Vote')
                    .filter(x_is_affiliate_of('Democrat'))
                    .sum()
                    .alias('State Vote\nDemocrat'),
                    pl.col('Vote')
                    .filter(x_is_affiliate_of('Republican'))
                    .sum()
                    .alias('State Vote\nRepublican'),
                    pl.col('Vote').sum().alias('State Vote\nAll Parties'),
                ]
            )
            .with_columns(
                pl.col('State Vote\nDemocrat') + pl.col('State Vote\nRepublican')
                    .sum().alias('State Vote\nMajor Parties'),
                (pl.col('State Vote\nDemocrat') / pl.col('State Vote\nAll Parties') * 100)
                .round(1).alias('State Vote %\nDemocrat'),
                (pl.col('State Vote\nRepublican') / pl.col('State Vote\nAll Parties') * 100)
                .round(1).alias('State Vote %\nRepublican'),
            )
        )
        self.dfs['aggregate_vote_by_state'] = df
        return df

    def get_aggregate_vote_by_district(self) -> pl.DataFrame:
        df: pl.DataFrame = (
            self.dfs['states_and_territories']
            .group_by(SD_COLS, maintain_order=True)
            .agg(
                [
                    pl.col('Vote')
                    .filter(x_is_affiliate_of('Democrat'))
                    .sum()
                    .alias('District Vote\nDemocrat'),
                    pl.col('Vote')
                    .filter(x_is_affiliate_of('Republican'))
                    .sum()
                    .alias('District Vote\nRepublican'),
                ]
            )
            .with_columns(
                (pl.col('District Vote\nDemocrat')
                    + pl.col('District Vote\nRepublican')).alias(
                    'District Vote\nMajor Parties'
                )
            )
            .with_columns(
                ((pl.col('District Vote\nDemocrat')
                    / pl.col('District Vote\nMajor Parties'))
                  * 100).round(1).alias('District Vote %\nDemocrat'),
                ((pl.col('District Vote\nRepublican')
                  / pl.col('District Vote\nMajor Parties'))
                  * 100).round(1).alias('District Vote %\nRepublican')
            )
        )
        self.dfs['aggregate_vote_by_district'] = df
        return df