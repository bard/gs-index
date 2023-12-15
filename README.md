# gs-index

Command-line indexer for [Allo (v1)](https://github.com/gitcoinco/grants-stack-allo-contracts-v1) events. Built for Gitcoin's December 2023 hackathon ([presentation](https://github.com/bard/gitcoin-hackathon-2023-presentation)). Companion to [gs-log](https://github.com/bard/gs-log).

## Features

- accepts events as JSON on stdin for easy composition: file, network, direct pipe from [gs-log](https://github.com/bard/gs-log), etc
- outputs SQL (Postgres dialect)

## Examples

Index historical data into a local database:

```sh
$ cat event_log.ndjson | gs-index | psql mydb
```

Index historical plus live data into a local database:

```sh
$ gs-log --chains 58008:origin..ongoing | gs-index | psql mydb
```

Index historical plus live data and keep a log to easily resume in case of interruption:

```sh
$ gs-log --chains 58008:origin..ongoing | \
  tee -a event_ndjson.log | \
  gs-index | \
  psql mydb
# ... later ...
$ cat event_ndjson | \
  gs-log --resume | \
  tee -a event_ndjson.log | \
  gs-index | \
  psql mydb
```

## Status

Work in progress. Event support:

- [x] ProjectCreated
- [x] MetadataUpdated
- [x] OwnerAdded
- [x] OwnerRemoved
- [x] RoundCreated
- [x] NewProjectApplication
- [ ] ProjectCreated
- [ ] MetadataUpdated
- [ ] OwnerAdded
- [ ] OwnerRemoved
- [ ] RoundCreated
- [ ] RoundCreated
- [ ] MatchAmountUpdated
- [ ] RoundMetaPtrUpdated
- [ ] ApplicationMetaPtrUpdated
- [ ] NewProjectApplication
- [ ] ProjectsMetaPtrUpdated
- [ ] ApplicationStatusesUpdated
- [ ] VotingContractCreatedV1
- [ ] VotingContractCreated
- [ ] Voted
- [ ] PayoutContractCreated
- [ ] ApplicationInReviewUpdated
