import polars as pl

from HRElectViz.HrElection import HrElection


class GerryMeter:
    def __init__(self, hr_elect_dfs: HrElection):
        self.hr_elect_dfs = hr_elect_dfs
        self.dfs: dict[str, pl.DataFrame] = {}

    def get_partisan_bias(self) -> pl.DataFrame:
        if 'state_nwinners_by_party' not in self.hr_elect_dfs.dfs:
            self.hr_elect_dfs.get_state_nwinners_by_party()
        if 'aggregate_vote_by_state' not in self.hr_elect_dfs.dfs:
            self.hr_elect_dfs.get_aggregate_vote_by_state()
        for name in ['state_nwinners_by_party', 'aggregate_vote_by_state']:
            frame = self.hr_elect_dfs.dfs[name]
            print(name, frame.columns)

        df: pl.DataFrame = (
            self.hr_elect_dfs.dfs['state_nwinners_by_party']
            .select(
                pl.col('State\nCode'),
                pl.col('state_fips'),
                pl.col('Republican\ndelegate\ncount'),
                pl.col('Republican\ndelegate %'),
                pl.col('Democrat\ndelegate\ncount'),
                pl.col('Democrat\ndelegate %'),
            )
            .join(
                self.hr_elect_dfs.dfs['aggregate_vote_by_state'].select(
                    pl.col('State\nCode'),
                    pl.col('Republican\nVote %'),
                    pl.col('Democrat\nVote %'),
                ),
                on='State\nCode',
            )
            .with_columns(
                (
                    pl.col('Republican\ndelegate %')
                    - pl.col('Republican\nVote %')
                ).alias('Partisan\nbias towards\nRepublicans'),
                (
                    pl.col('Democrat\ndelegate %') - pl.col('Democrat\nVote %')
                ).alias('Partisan\nbias towards\nDemocrats'),
            )
        )
        self.dfs['partisan_bias'] = df
        return df

    def get_mean_median_difference(self) -> pl.DataFrame:
        if 'aggregate_vote_by_district' not in self.hr_elect_dfs.dfs:
            self.hr_elect_dfs.get_aggregate_vote_by_district()
        df: pl.DataFrame = (
            self.hr_elect_dfs.dfs['aggregate_vote_by_district']
            .group_by(pl.col('State\nCode'), maintain_order=True)
            .agg(
                pl.col('Democrat\nVote %')
                .mean()
                .round(1)
                .alias('Mean share\nDemocrat'),
                pl.col('Democrat\nVote %')
                .median()
                .round(1)
                .alias('Median share\nDemocrat'),
                pl.col('Republican\nVote %')
                .mean()
                .round(1)
                .alias('Mean share\nRepublican'),
                pl.col('Republican\nVote %')
                .median()
                .round(1)
                .alias('Median share\nRepublican'),
            )
            .with_columns(
                (
                    (
                        pl.col('Mean share\nDemocrat')
                        - pl.col('Median share\nDemocrat')
                    ).round(1)
                ).alias('Mean-median\ndifference,\n+Democrats'),
                (
                    (
                        pl.col('Mean share\nRepublican')
                        - pl.col('Median share\nRepublican')
                    )
                    .round(1)
                    .alias('Mean-median\ndifference,\n+Republicans')
                ),
            )
        )
        self.dfs['mean_median_difference'] = df
        return df

    def get_efficiency_gap(self) -> pl.DataFrame:
        def x_wasted_vote(party: str) -> pl.Expr:
            nl = '\n'
            return (
                pl.when(pl.col(f'{party}{nl}Vote') >= pl.col('Needed\nto win'))
                .then(pl.col(f'{party}{nl}Vote') - pl.col('Needed\nto win'))
                .otherwise(pl.col(f'{party}{nl}Vote'))
                .alias(f'Wasted{nl}{party}{nl}Votes')
            )

        def x_efficiency_gap_favoring(party: str) -> pl.Expr:
            nl = '\n'
            parties: list[str] = []
            match party:
                case 'Democrat':
                    parties = ['Republican', 'Democrat']
                case 'Republican':
                    parties = ['Democrat', 'Republican']
                case _:
                    raise ValueError(
                        'get_efficiency_gap: party must beeither Democrat or Republican'
                    )
            return (
                (
                    (
                        pl.col(f'Wasted{nl}{parties[0]}{nl}Votes')
                        - pl.col(f'Wasted{nl}{parties[1]}{nl}Votes')
                    )
                    / pl.col('Vote\nBoth\nParties')
                )
                .round(2)
                .alias(f'{party}-{nl}leaning{nl}efficiency{nl}gap')
            )

        if 'aggregate_vote_by_district' not in self.hr_elect_dfs.dfs:
            self.hr_elect_dfs.get_aggregate_vote_by_district()
        x_needed_to_win = (
            ((pl.col('Vote\nBoth\nParties') + 1.0) / 2.0)
            .floor()
            .cast(pl.Int32)
            .alias('Needed\nto win')
        )
        df = (
            self.hr_elect_dfs.dfs['aggregate_vote_by_district']
            .with_columns(x_needed_to_win)
            .with_columns(
                x_wasted_vote('Republican'), x_wasted_vote('Democrat')
            )
            .group_by('State\nCode', maintain_order=True)
            .agg(
                pl.col('Republican\nVote').sum(),
                pl.col('Democrat\nVote').sum(),
                pl.col('Vote\nBoth\nParties').sum(),
                pl.col('Wasted\nRepublican\nVotes').sum(),
                pl.col('Wasted\nDemocrat\nVotes').sum(),
            )
            .with_columns(
                x_efficiency_gap_favoring('Democrat'),
                x_efficiency_gap_favoring('Republican'),
            )
        )
        self.dfs['efficiency_gap'] = df
        return df

    def get_gerrymander_metrics(self) -> pl.DataFrame:
        if 'partisan_bias' not in self.dfs:
            self.get_partisan_bias()
        if 'mean_median_difference' not in self.dfs:
            self.get_mean_median_difference()
        if 'efficiency_gap' not in self.dfs:
            self.get_efficiency_gap()
        df: pl.DataFrame = (
            self.dfs['partisan_bias']
            .join(self.dfs['mean_median_difference'], on='State\nCode')
            .join(self.dfs['efficiency_gap'], on='State\nCode')
        )
        self.dfs['gerrymander_metrics'] = df
        return df
