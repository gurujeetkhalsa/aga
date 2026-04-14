# Architecture Overview

## Why a monorepo

The current codebase mixes unrelated responsibilities in a way that makes branching, deployment, and collaboration risky.

A monorepo gives us:

- one GitHub home for the AGA functions work
- clear boundaries between apps
- easier branching and review
- room for shared helpers without forcing premature repo splits

## App responsibilities

### Ratings Explorer

User-facing search and detail experience for players and tournaments, plus snapshot maintenance and SGF features.

### ClubExpress Mail

Operational ingestion pipeline that polls Gmail, processes ClubExpress messages, archives artifacts, and routes parsed data into downstream storage/import flows.

### Membership Data

Membership/chapter/category data import and lookup endpoints.

### TD Lists

Public or semi-public TD list publishing endpoints with their own rendering/query logic.

## Repo principle

Different products can live in one repo, but each app should have a narrow responsibility and its own deployment story.

