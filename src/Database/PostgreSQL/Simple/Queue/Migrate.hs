{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

module Database.PostgreSQL.Simple.Queue.Migrate where

import Control.Monad
import Database.PostgreSQL.Simple
import Database.PostgreSQL.Simple.SqlQQ
import Data.Monoid
import Data.String

{-| This function creates a table and enumeration type that is
    appropriate for the queue.
-}
migrate :: String -> Connection -> IO ()
migrate schemaName conn = void $ execute_ conn $
  "CREATE SCHEMA IF NOT EXISTS " <> fromString schemaName <> ";" <>
  "SET search_path TO " <> fromString schemaName <> ";" <> [sql|
  DO $$
  BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'state_t') THEN
      CREATE TYPE state_t AS ENUM ('enqueued', 'dequeued', 'failed');
    END IF;
  END$$;

  CREATE TABLE IF NOT EXISTS payloads
  ( id BIGSERIAL PRIMARY KEY
  , value jsonb NOT NULL
  , attempts int NOT NULL DEFAULT 0
  , state state_t NOT NULL DEFAULT 'enqueued'
  , created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT clock_timestamp()
  );

  CREATE INDEX IF NOT EXISTS payloads_idx_state ON payloads (state);
  CREATE INDEX IF NOT EXISTS payloads_idx_created_at ON payloads (created_at);
|]
