# Demography constructs

From `omop_constructs.alchemy.demography`.

- `PersonDemography` — demographic attributes (gender, year of birth, death, MRN, postcode, country of birth, language spoken) attached to condition episodes. A person with several recorded postcodes, countries of birth or languages has one row per combination for each episode.

---

<!-- BEGIN GENERATED: construct-contracts -->

<!-- Generated from construct-contracts.toml. Do not edit by hand; run `python -m omop_constructs.core.catalogue docs/construct-catalog.md`. -->

## Contracts

What one row of each construct represents and how rows are identified.

- **Rows**: what one row represents.
- **Unique on**: the columns that together identify one row.
- **Row IDs**: whether the construct's own ID column can be stored and reused.

| Construct | One row per |
|---|---|
| [`PersonDemography`](#contract-person_demography_mv) | person + condition episode + recorded postcode + country of birth + language spoken |

### `PersonDemography` {#contract-person_demography_mv}

View: `person_demography_mv`

**Rows:** One row per (person, condition episode, recorded postcode, country of birth, language spoken), carrying gender, year of birth, death, and MRN. A person with two postcodes and two languages has four rows per episode. People whose gender concept is missing or zero are not included.

**Unique on:** No published column combination is guaranteed unique: `person_id` and `episode_id` repeat for each postcode, country of birth, and language combination.

**Row IDs:** Numbered afresh on every refresh. Do not store them or join on them across refreshes.

<!-- END GENERATED: construct-contracts -->
