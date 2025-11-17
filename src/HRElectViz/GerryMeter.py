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
                pl.col('State\nAbbr'),
                pl.col('State\nFIPS'),
                pl.col('Republican\ndelegate\ncount'),
                pl.col('Republican\ndelegate %'),
                pl.col('Democrat\ndelegate\ncount'),
                pl.col('Democrat\ndelegate %'),
            )
            .join(
                self.hr_elect_dfs.dfs['aggregate_vote_by_state'].select(
                    pl.col('State\nAbbr'),
                    pl.col('State Vote %\nRepublican'),
                    pl.col('State Vote %\nDemocrat'),
                ),
                on='State\nAbbr',
            )
            .with_columns(
                (
                    pl.col('Republican\ndelegate %')
                    - pl.col('State Vote %\nRepublican')
                ).alias('Partisan\nbias towards\nRepublicans'),
                (
                    pl.col('Democrat\ndelegate %') - pl.col('State Vote %\nDemocrat')
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
            .group_by(pl.col('State\nAbbr'), maintain_order=True)
            .agg(
                pl.col('District Vote %\nDemocrat')
                .mean()
                .round(1)
                .alias('Mean share\nDemocrat'),
                pl.col('District Vote %\nDemocrat')
                .median()
                .round(1)
                .alias('Median share\nDemocrat'),
                pl.col('District Vote %\nRepublican')
                .mean()
                .round(1)
                .alias('Mean share\nRepublican'),
                pl.col('District Vote %\nRepublican')
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
                pl.when(pl.col(f'District Vote{nl}{party}') >= pl.col('Needed\nto win'))
                .then(pl.col(f'District Vote{nl}{party}') - pl.col('Needed\nto win'))
                .otherwise(pl.col(f'District Vote{nl}{party}'))
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
                    / pl.col('District Vote\nBoth Parties')
                )
                .round(2)
                .alias(f'{party}-{nl}leaning{nl}efficiency{nl}gap')
            )

        if 'aggregate_vote_by_district' not in self.hr_elect_dfs.dfs:
            self.hr_elect_dfs.get_aggregate_vote_by_district()
        x_needed_to_win = (
            ((pl.col('District Vote\nBoth Parties') + 1.0) / 2.0)
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
            .group_by('State\nAbbr', maintain_order=True)
            .agg(
                pl.col('District Vote\nRepublican').sum(),
                pl.col('District Vote\nDemocrat').sum(),
                pl.col('District Vote\nBoth Parties').sum(),
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
            .join(self.dfs['mean_median_difference'], on='State\nAbbr')
            .join(self.dfs['efficiency_gap'], on='State\nAbbr')
        )
        self.dfs['gerrymander_metrics'] = df
        return df

if __name__ == '__main__':
    hrelect = HrElection()
    gerry_metrics_df = GerryMeter(hrelect)
    print(gerry_metrics_df.get_gerrymander_metrics())