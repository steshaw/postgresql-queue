{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}

{-| This module utilize PostgreSQL to implement a durable queue for efficently processing
    arbitrary jobs which can be represented as JSON.

    Typically a producer would enqueue a new job as part of larger database
    transaction

 @
   createAccount userRecord = do
      'runDBTSerializable' $ do
         createUserDB userRecord
         'enqueueDB' $ makeVerificationEmail userRecord
 @

In another thread or process, the consumer would drain the queue.

 @
    forever $ do
      -- Attempt get a job or block until one is available
      job <- 'lock' conn

      -- Perform application specific parsing of the job arguments.
      case fromJSON $ 'qjArgs' job of
        Success x -> sendEmail x -- Perform application specific processing
        Error err -> logErr err

      -- Remove the job from future processing
      'dequeue' conn $ 'qjId' job
 @

This modules provides two flavors of functions, a DB API and an IO API.
Most operations are provided in both flavors, with the exception of 'lock'.
'lock' blocks and would not be that useful as part of a larger transaction
since it would keep the transaction open for a potentially long time. Although
both flavors are provided, in general one versions is more useful for typical
use cases.
-}
module Database.PostgreSQL.Simple.Queue
  ( -- * Types
    JobId (..)
  , State (..)
  , Job (..)
  -- * DB API
  , dequeueDB
  , deleteDB
  , enqueueDB
  , withJobDB
  , getEnqueuedCountDB
  , getFailedCountDB
  -- * IO API
  , enqueue
  , withJob
  , getEnqueuedCount
  , getFailedCount
  ) where

import Control.Monad
import Control.Monad.Catch
import Data.Aeson
import Data.Function
import Data.Int
import Data.Text (Text)
import Data.Time
import Database.PostgreSQL.Simple (Connection, Only(..))
import Database.PostgreSQL.Simple.FromField
import Database.PostgreSQL.Simple.FromRow
import Database.PostgreSQL.Simple.Notification
import Database.PostgreSQL.Simple.SqlQQ
import Database.PostgreSQL.Simple.ToField
import Database.PostgreSQL.Simple.Transaction
import Database.PostgreSQL.Transact
import Data.Monoid
import Data.String
import Control.Monad.IO.Class

import qualified Database.PostgreSQL.Simple as Simple

-------------------------------------------------------------------------------
---  Types
-------------------------------------------------------------------------------
newtype JobId = JobId
  { unJobId :: Int64
  } deriving (Eq, Show, FromField, ToField)

instance FromRow JobId where
  fromRow = fromOnly <$> fromRow

-- The fundamental record stored in the queue. The queue is a single table
-- and each row consists of a 'Job'.
data Job = Job
  { qjId :: JobId
  , qjArgs :: Value -- ^ The arguments of a job encoded as JSON.
  , qjRunAt :: UTCTime
  , qjState :: State
  , qjAttempts :: Int
  } deriving (Show, Eq)

instance FromRow Job where
  fromRow = Job <$> field <*> field <*> field <*> field <*> field

-- | A 'Job' can exist in three states in the queue, 'Enqueued',
--   and 'Dequeued'. A 'Job' starts in the 'Enqueued' state and is locked
--   so some sort of process can occur with it, usually something in 'IO'.
--   Once the processing is complete, the `Job' is moved the 'Dequeued'
--   state, which is the terminal state.
data State = Enqueued | Dequeued | Failed
  deriving (Show, Eq, Ord, Enum, Bounded)

instance ToField State where
  toField = toField . \case
    Enqueued -> "enqueued" :: Text
    Dequeued -> "dequeued"
    Failed -> "failed"

-- Converting from enumerations is annoying :(
instance FromField State where
  fromField f y = do
     n <- typename f
     if n == "state_t" then case y of
       Nothing -> returnError UnexpectedNull f "state can't be NULL"
       Just y' -> case y' of
         "enqueued" -> pure Enqueued
         "dequeued" -> pure Dequeued
         "failed"   -> pure Failed
         x          -> returnError ConversionFailed f (show x)
     else
       returnError Incompatible f $
         "Expect type name to be state but it was " ++ show n

-------------------------------------------------------------------------------
---  DB API
-------------------------------------------------------------------------------
notifyName :: (Monoid s, IsString s) => String -> s
notifyName queueName = "queue_" <> fromString queueName

{-| Enqueue a new JSON value into the queue. This particularly function
    can be composed as part of a larger database transaction. For instance,
    a single transaction could create a user and enqueue a email message.

 @
   createAccount userRecord = do
      'runDBTSerializable' $ do
         createUserDB userRecord
         'enqueueDB' $ makeVerificationEmail userRecord
 @
-}
enqueueDB :: String -> Value -> DB JobId
enqueueDB queueName value = enqueueWithDB queueName value

enqueueWithDB :: String -> Value -> DB JobId
enqueueWithDB queueName args =
  fmap head $ query ([sql| NOTIFY |] <> " " <> notifyName queueName <> ";" <> [sql|
      INSERT INTO queued_jobs (queue, args)
      VALUES (?, ?)
      RETURNING id;
    |]) (queueName, args)

retryDB :: Job -> DB Int64
retryDB Job {..} =
  execute [sql|
    UPDATE queued_jobs
    SET attempts = attempts + 1
    WHERE id = ?
  |] (Only qjId)

dequeueDB :: Job -> DB Int64
dequeueDB Job {..} =
  execute [sql|
    UPDATE queued_jobs
    SET state = 'dequeued'
    WHERE id = ?
  |] (Only qjId)

failedDB :: Job -> DB Int64
failedDB Job {..} =
  execute [sql|
    UPDATE queued_jobs
    SET state = 'failed', attempts = attempts + 1
    WHERE id = ?
  |] (Only qjId)

deleteDB :: Job -> DB Int64
deleteDB Job {..} =
  execute [sql|
    DELETE FROM queued_jobs
    WHERE id = ?
  |] (Only qjId)

{-|

Attempt to get a job and process it. If the function passed in throws an exception
return it on the left side of the `Either`. Re-add the job up to some passed in
maximum. Return `Nothing` is the `jobs` table is empty otherwise the result is an `a`
from the job enqueue function.

-}
withJobDB :: String
              -- ^ queue name
              -> Int
              -- ^ retry count
              -> (Job -> IO a)
              -- ^ job processing function
              -> DB (Either SomeException (Maybe a))
withJobDB queueName retryCount f
  = query [sql|
      SELECT id, args, run_at, state, attempts
      FROM queued_jobs
      WHERE queue = ?
      AND state = 'enqueued'
      AND run_at <= clock_timestamp()
      ORDER BY run_at ASC
      FOR UPDATE SKIP LOCKED
      LIMIT 1
    |] (Only $ queueName)
 >>= \case
    [] -> return $ return Nothing
    [job@Job {..}] ->
      handle (onError job) $ do
        result <- liftIO (f job)
        deleteDB job
        pure (Right . Just $ result)
    xs -> return
        $ Left
        $ toException
        $ userError
        $ "LIMIT is 1 but got more than one row: "
        ++ show xs
  where
    onError :: Job -> SomeException -> DBT IO (Either SomeException a)
    onError job@Job {..} e = do
      -- Retry on failure up to retryCount. Set state to failed when retryCount reached.
      if (qjAttempts < retryCount)
      then void $ retryDB job
      else void $ failedDB job
      pure $ Left e

-- | Get the number of rows in a particular state.
getCountDB :: String -> State -> DB Int64
getCountDB queueName state = fmap (fromOnly . head) $ query [sql|
    SELECT count(*)
    FROM queued_jobs
    WHERE queue = ?
    AND state = ?
  |] (queueName, state)

-- | Get the number of rows in the 'Enqueued' state.
getEnqueuedCountDB :: String -> DB Int64
getEnqueuedCountDB queueName = getCountDB queueName Enqueued

-- | Get the number of rows in the 'Failed' state.
getFailedCountDB :: String -> DB Int64
getFailedCountDB queueName = getCountDB queueName Failed

-------------------------------------------------------------------------------
---  IO API
-------------------------------------------------------------------------------
{-| Enqueue a new JSON value into the queue. See 'enqueueDB' for a version
    which can be composed with other queries in a single transaction.
-}
enqueue :: String -> Connection -> Value -> IO JobId
enqueue queueName conn args = runDBT (enqueueDB queueName args) ReadCommitted conn

-- Block until a job notification is fired. Fired during insertion.
notifyJob :: String -> Connection -> IO ()
notifyJob queueName conn = do
  Notification {..} <- getNotification conn
  unless (notificationChannel == notifyName queueName) $ notifyJob queueName conn

{-| Return the oldest 'Job' in the 'Enqueued' state or block until a
    job arrives. This function utilizes PostgreSQL's LISTEN and NOTIFY
    functionality to avoid excessively polling of the DB while
    waiting for new jobs, without sacrificing promptness.
-}
withJob :: String
            -> Connection
            -> Int
            -- ^ retry count
            -> (Job -> IO a)
            -> IO (Either SomeException a)
withJob queueName conn retryCount f = bracket_
  (Simple.execute_ conn $ "LISTEN " <> notifyName queueName)
  (Simple.execute_ conn $ "UNLISTEN " <> notifyName queueName)
  $ fix
  $ \continue -> runDBT (withJobDB queueName retryCount f) ReadCommitted conn
  >>= \case
    Left x -> return $ Left x
    Right Nothing -> do
      notifyJob queueName conn
      continue
    Right (Just x) -> return $ Right x

{-| Get the number of rows in the 'Enqueued' state. This function runs
    'getEnqueuedCountDB' in a 'ReadCommitted' transaction.
-}
getEnqueuedCount :: String -> Connection -> IO Int64
getEnqueuedCount queueName = runDBT (getEnqueuedCountDB queueName) ReadCommitted

{-| Get the number of rows in the 'Failed' state. This function runs
    'getFailedCountDB' in a 'ReadCommitted' transaction.
-}
getFailedCount :: String -> Connection -> IO Int64
getFailedCount queueName = runDBT (getFailedCountDB queueName) ReadCommitted
