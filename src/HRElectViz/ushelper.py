import os

os.environ["DC_STATEHOOD"] = "1"

import us  # type: ignore

fips_to_abbr = us.states.mapping("fips", "abbr")
abbr_to_fips = us.states.mapping("abbr", "fips")
ucname_to_abbr: dict[str, str] = {
    name.upper(): abbr for name, abbr in us.states.mapping("name", "abbr").items()
}
states_sans_dc = [st for st in us.states.STATES if st != us.states.DC]
at_large_states_abbrs: list[str] = ["AK", "DE", "ND", "SD", "VT", "WY"]
state_names: list[str] = sorted([st.name.upper() for st in states_sans_dc])
state_territory_names: list[str] = sorted(
    [st.name.upper() for st in us.STATES_AND_TERRITORIES]
)
state_abbrs: list[str] = sorted([st.abbr for st in states_sans_dc])
territory_abbrs: list[str] = sorted([st.abbr for st in us.states.TERRITORIES] + ["DC"])


lower48_abbrs: list[str] = sorted(
    [st.abbr for st in states_sans_dc if st.is_contiguous]
)
lower48_fips: list[str] = sorted([st.fips for st in states_sans_dc if st.is_contiguous])

if __name__ == "__main__":
    print(f"{lower48_abbrs=}")
    print(f"{territory_abbrs=}")
    print(f"{us.states.STATES_AND_TERRITORIES=}")
