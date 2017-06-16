{-# LANGUAGE QuasiQuotes #-}
module Database.Queue.Migrate where
import           Control.Monad
import           Database.PostgreSQL.Simple
import           Database.PostgreSQL.Simple.SqlQQ

migrate :: Connection -> IO ()
migrate conn = void $ execute_ conn [sql|
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
  |]
