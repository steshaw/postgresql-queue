{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

module Database.PostgreSQL.Simple.Queue.Migrate where

import Control.Monad
import Database.PostgreSQL.Simple
import Database.PostgreSQL.Simple.SqlQQ

{-| This function creates a table and enumeration type that is
    appropriate for the queue.
-}
migrate :: Connection -> IO ()
migrate conn = void $ execute_ conn [sql|
  DO $$
  BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'status') THEN
      CREATE TYPE status AS ENUM ('enqueued', 'dequeued', 'failed');
    END IF;
  END$$;

  CREATE TABLE IF NOT EXISTS queued_jobs
  ( id BIGSERIAL PRIMARY KEY
  , queue TEXT NOT NULL
  , args jsonb NOT NULL
  , run_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT clock_timestamp()
  , status status NOT NULL DEFAULT 'enqueued'
  , attempts int NOT NULL DEFAULT 0
  );

  CREATE INDEX IF NOT EXISTS queued_jobs_idx
  ON queued_jobs (queue, status, run_at);
|]
