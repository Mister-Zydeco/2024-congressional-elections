import re
import polars as pl

from hrelectviz.hrelection import HrElection, std_polars_config

major_parties = ['Democrat', 'Republican']
gm_column_names = {
    'partisan_skew': [
        'State\nAbbr', 'Republican\ndelegate\ncount',
        'Republican\ndelegate %', 'State Vote %\nRepublican',
        'Skew towards\nRepublican', 'Democrat\ndelegate\ncount',
        'Democrat\ndelegate %', 'State Vote %\nDemocrat', 'Skew towards\nDemocrat'],
    'mean_median_difference': [
        'State\nAbbr', 'Mean share\nDemocrat', 'Median share\nDemocrat',
        'Mean-median difference\n(+ favors Democrats)',
        'Mean share\nRepublican', 'Median share\nRepublican',
        'Mean-median difference\n(+ favors Republicans)'],
    'efficiency_gap': [
        'State\nAbbr', 'State Vote\nMajor Parties',
        'State Vote\nRepublican', 'Wasted\nRepublican\nVotes',
        'Republican-leaning\nefficiency gap',
        'State Vote\nDemocrat', 'Wasted\nDemocrat\nVotes',
        'Democrat-leaning\nefficiency gap']
}

def shorten_column_name(col: str) -> str:
    col = re.sub(r'-leaning|\(\+ favors|towards|\)', '', col)
    col = col.replace('\n', ' ').replace('efficiency', 'eff.').replace(
        'Democrat', 'D').replace('Republican', 'R')
    return col

class GerryMeter:
    def __init__(self, hr_elect: HrElection):
        self.hr_elect = hr_elect
        self.dfs: dict[str, pl.DataFrame] = {}

    def get_partisan_skew(self) -> pl.DataFrame:
        if 'state_nwinners_by_party' not in self.hr_elect.dfs:
            self.hr_elect.get_state_nwinners_by_party()
        if 'aggregate_vote_by_state' not in self.hr_elect.dfs:
            self.hr_elect.get_aggregate_vote_by_state()
#        for name in ['state_nwinners_by_party', 'aggregate_vote_by_state']:
#            frame = self.hr_elect.dfs[name]
#            print(name, frame.columns)

        df: pl.DataFrame = (
            self.hr_elect.dfs['state_nwinners_by_party']
            .select([
                'State\nAbbr', 'State\nFIPS', 'Republican\ndelegate\ncount',
                'Republican\ndelegate %', 'Democrat\ndelegate\ncount',
                'Democrat\ndelegate %',
            ])
            .join(
                self.hr_elect.dfs['aggregate_vote_by_state'].select([
                    'State\nAbbr', 'State Vote %\nRepublican', 'State Vote %\nDemocrat',
                ]),
                on='State\nAbbr',
            )
            .with_columns(
                (
                    pl.col('Republican\ndelegate %')
                    - pl.col('State Vote %\nRepublican')
                ).alias('Skew towards\nRepublican'),
                (
                    pl.col('Democrat\ndelegate %') - pl.col('State Vote %\nDemocrat')
                ).alias('Skew towards\nDemocrat'),
            )
        )
        self.dfs['partisan_skew'] = df
        return df

    def get_mean_median_difference(self) -> pl.DataFrame:

        nl = '\n'
        def mean_median_cols(party: str) -> list[pl.Expr]:
            return [
                pl.col(f'District Vote{nl}{party}').mean(
                ).round(1).alias(f'Mean share{nl}{party}'),
                pl.col(f'District Vote{nl}{party}').median(
                ).round(1).alias(f'Median share{nl}{party}'),
            ]

        def mean_median_difference_col(party: str) -> pl.Expr:
            x: pl.Expr = (
              pl.col(f'Mean share{nl}{party}') - pl.col(f'Median share{nl}{party}')
              .round(1).alias('Mean-median\ndifference,\n{party}'))
            return x

        if 'aggregate_vote_by_district' not in self.hr_elect.dfs:
            self.hr_elect.get_aggregate_vote_by_district().filter(
                pl.col('State\nAbbr') != 'PR'
            )
        df: pl.DataFrame = (
            self.hr_elect.dfs['aggregate_vote_by_district']
            .group_by(pl.col('State\nAbbr'), maintain_order=True)
            .agg(*mean_median_cols('Democrat'), *mean_median_cols('Republican'))
        )
        df = df.with_columns(
            (pl.col('Mean share\nDemocrat') - pl.col('Median share\nDemocrat')
             ).alias('Mean-median difference\n(+ favors Republicans)'),
            (pl.col('Mean share\nRepublican') - pl.col('Median share\nRepublican')
             ).alias('Mean-median difference\n(+ favors Democrats)'),
        )
        self.dfs['mean_median_difference'] = df
        return df

    def get_efficiency_gap(self) -> pl.DataFrame:
        def x_wasted_vote(party: str) -> pl.Expr:
            nl = '\n'
            return (
                pl.when(pl.col(f'District Vote{nl}{party}') >= pl.col('Needed\nto win'))
                .then(pl.col(f'District Vote{nl}{party}') - pl.col('Needed\nto win'))
                .otherwise(pl.col(f'District Vote{nl}{party}'))
                .alias(f'Wasted{nl}{party}{nl}Votes')
            )

        def x_efficiency_gap_favoring(party: str) -> pl.Expr:
            nl = '\n'
            parties: list[str] = []
            other_party = 'Republican' if party == 'Democrat' else 'Democrat'
            return (
                ((pl.col(f'Wasted{nl}{other_party}{nl}Votes')
                    - pl.col(f'Wasted{nl}{party}{nl}Votes'))
                  / pl.col('District Vote\nMajor Parties')
                ).round(2).alias(f'{party}-leaning{nl}efficiency gap')
            )

        if 'aggregate_vote_by_district' not in self.hr_elect.dfs:
            self.hr_elect.get_aggregate_vote_by_district()
        x_needed_to_win = (
            ((pl.col('District Vote\nMajor Parties') + 1.0) / 2.0)
            .floor()
            .cast(pl.Int32)
            .alias('Needed\nto win')
        )
        df = (
            self.hr_elect.dfs['aggregate_vote_by_district']
            .with_columns(x_needed_to_win)
            .with_columns(
                x_wasted_vote('Republican'), x_wasted_vote('Democrat')
            )
            .group_by('State\nAbbr', maintain_order=True)
            .agg(
                pl.col('District Vote\nRepublican').sum(),
                pl.col('District Vote\nDemocrat').sum(),
                pl.col('District Vote\nMajor Parties').sum(),
                pl.col('Wasted\nRepublican\nVotes').sum(),
                pl.col('Wasted\nDemocrat\nVotes').sum(),
            )
            .with_columns(
                x_efficiency_gap_favoring('Democrat'),
                x_efficiency_gap_favoring('Republican'),
            ).rename(lambda col: col.replace('District', 'State'))
        )
        self.dfs['efficiency_gap'] = df
        return df

    def get_gerrymander_metrics(self) -> pl.DataFrame:
        if 'partisan_skew' not in self.dfs:
            self.get_partisan_skew()
        if 'mean_median_difference' not in self.dfs:
            self.get_mean_median_difference()
        if 'efficiency_gap' not in self.dfs:
            self.get_efficiency_gap()
        df: pl.DataFrame = (
            self.dfs['partisan_skew']
            .join(self.dfs['mean_median_difference'], on='State\nAbbr')
            .join(self.dfs['efficiency_gap'], on='State\nAbbr')
        )
        self.dfs['gerrymander_metrics'] = df
        return df

def get_color_col_name(df_name: str, party: str) -> str:
    match df_name:
        case 'partisan_skew':
            return f'Skew towards\n{party}'
        case 'mean_median_difference':
            return f'Mean-median\ndifference,\n+{party}'
        case 'efficiency_gap':
            return f'{party}-leaning\nefficiency gap'

if __name__ == '__main__':
    hrelect = HrElection()
    gm = GerryMeter(hrelect)
    gm_df = gm.get_gerrymander_metrics()
    for dfname, df in gm.dfs.items():
        print(f'{dfname} has columns {df.columns}')
