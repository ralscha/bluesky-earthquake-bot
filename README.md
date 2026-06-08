# Bluesky Earthquake Bot

Reporting earthquakes from the USGS Earthquake feed to Bluesky.

https://bsky.app/profile/earthquake.rasc.ch

## Commands

- `post`: posts reviewed USGS earthquakes with magnitude 5.5 or higher.
- `stat`: posts the latest complete weekly earthquake summary.
- `migrate`: migrates the Pebble database to the current stored magnitude format.

## Configuration

Set `BLUESKY_IDENTIFIER` and `BLUESKY_PASSWORD` in the environment or in a local `.env` file. `BLUESKY_HOST` is optional and defaults to `https://me.rasc.ch`.
