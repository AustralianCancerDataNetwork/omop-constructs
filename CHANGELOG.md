## 0.1.1
- created services layer for functions that execute queries and return data / session-bound

## 0.1.2
- condition modifier queries and factories

## 0.1.3
- refactored condition modifier queries and factories

## 0.1.4
- added in runtime lookups and support for concept resolver registry

## 0.1.5 
- typing

## 0.1.6
- changed modifier field to correct one

## 0.1.7
- modifier field

## 0.1.8
- bugfix

## 0.2.0
- bumped to beta with matview registry proper usage
- the depth of nested matview under condition mods really does need everything under it to be a matview too

## 0.2.1
- bumped constrained versions of omop-alchemy and semantics

## 0.2.2
- created initial episode views for disease eps

## 0.2.3
- added eps as module

## 0.2.4 
- upversion semantics

## 0.2.5
- treatment eps

## 0.2.6
- update col for treatment eps

## 0.2.7
- episode cols dynamic name correction

## 0.2.8
- condition episode object mapper

## 0.2.9
- measurements mat view

## 0.2.10
- missed checkin

## 0.2.11
- upversion semantics

## 0.2.12
- demography

## 0.2.13
- added cols for condition ep

## 0.2.14
- simplified ep naming

## 0.2.15
- fix label name

## 0.2.16
- mets col adapt

## 0.2.17
- mets cols thru to condition mods

## 0.3.0
- all stage modifier mapper

## 0.3.1
- fixed all stage cols

## 0.3.2
- some treatment mappers

## 0.3.3
- general case measurement mapper

## 0.3.4
- nulltype on staging pk col

## 0.3.5
- generic procedure mapper

## 0.3.6
- generic observation mapper

## 0.3.7
- fullly specified condition-treatment-summarisation mapper

## 0.3.8
- added in condition code for substring matchers

## 0.3.9
- condition code for mod condition and stage condition

## 0.3.10
- sqlalchemy v2 mappings for demography columns

## 0.3.11
- modify mv creation api

## 0.3.12
- bugfix

## 0.3.13
- bump

## 0.3.14
- fix mod proc cols

## 0.3.15
- treatment envelope

## 0.3.16
- correcting fraction filter

## 0.3.17
- modality treatment join

## 0.3.18
- modailty treatment now linked to condition

## 0.3.19
- correcting duplicated mv name

## 0.3.20
- bugfix

## 0.3.21
- bump

## 0.3.22
- clashing mv names

## 0.3.23
- country of birth not EAV

## 0.3.24
- country of birth lookup fix

## 0.4.0
- changed functionality for handling of date windows
- where no explicit event link exists, we now give either explicit date window of -90 < t < 365 days, or use episode window
- if episode window has no end date, we use episode start + 365 days
- date windowing can now be handled flexibly by downstream consumers

## 0.4.1
- episode-linked specialist visit support

## 0.4.2
- set version window for orm-loader

## 0.4.3
- fixing the inclusion of downstream domain-specific columns in episode_relevant_window factory

## 0.4.4
- added bootstrap-based complete registry loading and lazy package exports to reduce import fanout

## 0.4.5
- bump version number

## 0.4.6
- added primary diagnosis condition mv, registry compile/drift checks, postgres CI coverage, and schema snapshot artifact export

## 0.4.7
- fixed treatment envelope window issues

## 0.4.8 
- treatment envelope should left join to death (bug fix)

## 0.4.9
- surgery absence rule requires outer join 

## 0.4.10
- cast SACT and RT exposure dates to Date in sact_window and rt_window to prevent timestamp upcast contaminating LEAST/GREATEST results; fixes treatment_days_before_death and days_from_dx_to_treatment storing as interval instead of integer

## 0.5.0
- integrated oa-configurator package configuration and adapted omop-constructs to current orm-loader / omop-alchemy compatibility changes