{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

module Database.PostgreSQL.Simple.Queue.Migrate where

import Control.Monad
import Database.PostgreSQL.Simple
import Database.PostgreSQL.Simple.SqlQQ
import Data.Monoid
import Data.String

{-| This function creates a table and enumeration type that is
    appriopiate for the queue. The following sql is used.

 @
 CREATE TYPE state_t AS ENUM ('enqueued', 'locked', 'dequeued');

 CREATE TABLE payloads
 ( id uuid PRIMARY KEY
 , value jsonb NOT NULL
 , state state_t NOT NULL DEFAULT 'enqueued'
 , created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT clock_timestamp()
 , modified_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT clock_timestamp()
 );

 CREATE INDEX state_idx ON payloads (state);

 CREATE OR REPLACE FUNCTION update_row_modified_function_()
 RETURNS TRIGGER
 AS
 $$
 BEGIN
     -- ASSUMES the table has a column named exactly "modified_at".
     -- Fetch date-time of actual current moment from clock,
     -- rather than start of statement or start of transaction.
     NEW.modified_at = clock_timestamp();
     RETURN NEW;
 END;
 $$
 language 'plpgsql';
 @

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
