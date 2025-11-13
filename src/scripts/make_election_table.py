
from HRElectViz.HrElection import HrElection
import itables

if __name__ == '__main__':
    dfnames = [
        'aggregate_vote_by_district',
        'aggregate_vote_by_state',
        'district_major_party_vote',
        'district_winners',
        'district_winners_with_major_party',
        'districts_ranked_by_vote',
        'ndistricts_per_state',
        'state_nwinners_by_party',
    ]
    hr_elect = HrElection()
    prompt = '\n'.join(f'{x}: {dfname}'
        for x, dfname in enumerate(dfnames)
    ) + '\nEnter number next to desired dataframe above: '
    df_no = int(input(prompt))
    method = getattr(hr_elect, f'get_{dfnames[df_no]}')
    df = method()
    html = itables.to_html_datatable(df)
    with open(f'../../out/{dfnames[df_no]}.html', 'w') as fh:
        fh.write(html)