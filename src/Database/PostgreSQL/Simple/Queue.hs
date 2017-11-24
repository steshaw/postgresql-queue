{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RecordWildCards #-}

{-|
This module utilize PostgreSQL to implement a durable queue for efficiently
processing arbitrary jobs which can be represented as JSON.

Typically a producer would enqueue a new job as part of larger database
transaction:

@
createAccount userRecord = do
  'runDBTSerializable' $ do
    createUserDB userRecord
    'enqueueDB' $ makeVerificationEmail userRecord
@

In another thread or process, the consumer would drain the queue:

@
forever $ do
  -- Block until a job is available.
  'withJob' sendEmailQueue connection maxRetries $ \Job {..} -> do
    case fromJSON qjArgs of
      Success email -> sendEmail email -- Perform application specific processing
      Error err -> logErr err
@

This modules provides two flavors of functions, a DB API and an IO API.
Most operations are provided in both flavors, with the exception of 'lock'.
'lock' blocks and would not be that useful as part of a larger transaction
since it would keep the transaction open for a potentially long time. Although
both flavors are provided, in general one versions is more useful for typical
use cases.
-}
module Database.PostgreSQL.Simple.Queue
  (
  -- * Types
    JobId(..)
  , Status(..)
  , Job(..)

  -- * DB API
  , dequeueDB
  , deleteDB
  , enqueueDB
  , enqueueAtDB
  , retryDB
  , retryAtDB
  , notifyName
  , notifyDB
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
import Control.Monad.IO.Class
import Data.Aeson
import Data.Char (toLower)
import Data.Function
import Data.Int
import Data.Monoid
import Data.String
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
  , qjStatus :: Status
  , qjAttempts :: Int
  } deriving (Show, Eq)

instance FromRow Job where
  fromRow = Job <$> field <*> field <*> field <*> field <*> field

{- |
A 'Job' can have one of 3 statuses. A 'Job' starts life 'Enqueued'.
If a job fails too many times, it will be set to 'Failed'.
A job can be set to 'Dequeued' if successfully processed (but it
can be deleted instead).
-}
data Status
  = Enqueued
  | Dequeued
  | Failed
  deriving (Show, Eq, Ord, Enum, Bounded)

instance ToField Status where
  toField = toField . \case
    Enqueued -> "enqueued" :: Text
    Dequeued -> "dequeued"
    Failed -> "failed"

-- Converting from enumerations is annoying :(
instance FromField Status where
  fromField f y = do
     n <- typename f
     if n == "status" then case y of
       Nothing -> returnError UnexpectedNull f "status can't be NULL"
       Just y' -> case y' of
         "enqueued" -> pure Enqueued
         "dequeued" -> pure Dequeued
         "failed"   -> pure Failed
         x          -> returnError ConversionFailed f (show x)
     else
       returnError Incompatible f $
         "Expect type name to be status but it was " ++ show n

-------------------------------------------------------------------------------
---  DB API
-------------------------------------------------------------------------------
notifyName :: (Monoid s, IsString s) => String -> s
notifyName queueName = "queue_" <> fromString (map toLower queueName)

notifyDB :: String -> DB ()
notifyDB queueName =
  void $ execute_ ([sql| NOTIFY |] <> " " <> notifyName queueName <> ";")

enqueueAtDB :: String -> Value -> UTCTime -> DB JobId
enqueueAtDB queueName args runAt =
  fmap head $ query ([sql| NOTIFY |] <> " " <> notifyName queueName <> ";" <> [sql|
      INSERT INTO queued_jobs (queue, args, run_at)
      VALUES (?, ?, ?)
      RETURNING id;
    |]) (queueName, args, runAt)

{-|
Enqueue a new JSON value into the queue. This particularly function
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
enqueueDB queueName args = do
  enqueueAtDB queueName args =<< liftIO getCurrentTime

retryAtDB :: JobId -> UTCTime -> DB Int64
retryAtDB qjId runAt =
  execute [sql|
    UPDATE queued_jobs
    SET attempts = attempts + 1, run_at = ?
    WHERE id = ?
  |] (runAt, qjId)

retryDB :: JobId -> DB Int64
retryDB qjId =
  retryAtDB qjId =<< liftIO getCurrentTime

dequeueDB :: JobId -> DB Int64
dequeueDB qjId =
  execute [sql|
    UPDATE queued_jobs
    SET status = 'dequeued'
    WHERE id = ?
  |] (Only qjId)

failedDB :: JobId -> DB Int64
failedDB qjId =
  execute [sql|
    UPDATE queued_jobs
    SET status = 'failed', attempts = attempts + 1
    WHERE id = ?
  |] (Only qjId)

deleteDB :: JobId -> DB Int64
deleteDB qjId =
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
withJobDB
  :: String -- ^ queue name
  -> Int -- ^ retry count
  -> (Job -> IO a) -- ^ job processing function
  -> DB (Either SomeException (Maybe a))
withJobDB queueName retryCount f
  = query [sql|
      SELECT id, args, run_at, status, attempts
      FROM queued_jobs
      WHERE queue = ?
      AND status = 'enqueued'
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
        void $ deleteDB qjId
        pure (Right . Just $ result)
    xs -> return
        $ Left
        $ toException
        $ userError
        $ "LIMIT is 1 but got more than one row: "
        ++ show xs
  where
    onError :: Job -> SomeException -> DBT IO (Either SomeException a)
    onError Job {..} e = do
      -- Retry on failure up to retryCount.
      -- Set status to 'Failed' when retryCount reached.
      if (qjAttempts < retryCount)
      then void $ retryDB qjId
      else void $ failedDB qjId
      pure $ Left e

-- | Get the number of rows in a particular status.
getCountDB :: String -> Status -> DB Int64
getCountDB queueName status = fmap (fromOnly . head) $ query [sql|
    SELECT count(*)
    FROM queued_jobs
    WHERE queue = ?
    AND status = ?
  |] (queueName, status)

-- | Get the number of rows that are 'Enqueued'.
getEnqueuedCountDB :: String -> DB Int64
getEnqueuedCountDB queueName = getCountDB queueName Enqueued

-- | Get the number of rows that are 'Failed'.
getFailedCountDB :: String -> DB Int64
getFailedCountDB queueName = getCountDB queueName Failed

-------------------------------------------------------------------------------
---  IO API
-------------------------------------------------------------------------------
{-|
Enqueue a new JSON value into the queue. See 'enqueueDB' for a version
which can be composed with other queries in a single transaction.
-}
enqueue :: String -> Connection -> Value -> IO JobId
enqueue queueName conn args =
  runDBT (enqueueDB queueName args) ReadCommitted conn

-- Block until a job notification is fired. Fired during insertion.
notifyJob :: String -> Connection -> IO ()
notifyJob queueName conn = do
  Notification {..} <- getNotification conn
  print ("received notification" :: Text, notificationChannel)
  unless (notificationChannel == notifyName queueName) $ notifyJob queueName conn

{-|
Return the oldest 'Job' which is 'Enqueued' or block until a
job arrives. This function utilizes PostgreSQL's LISTEN and NOTIFY
functionality to avoid excessively polling of the DB while
waiting for new jobs, without sacrificing promptness.
-}
withJob
  :: String
  -> Connection
  -> Int -- ^ retry count
  -> (Job -> IO a)
  -> IO (Either SomeException a)
withJob queueName conn retryCount f = bracket_
  (do
      print ("LISTEN" :: Text, notifyName queueName)
      Simple.execute_ conn $ "LISTEN " <> notifyName queueName)
  (do
      print ("UNLISTEN" :: Text, notifyName queueName)
      Simple.execute_ conn $ "UNLISTEN " <> notifyName queueName)
  $ fix
  $ \continue -> runDBT (withJobDB queueName retryCount f) ReadCommitted conn
  >>= \case
    Left x -> do
      print ("withJob Left", x)
      return $ Left x
    Right Nothing -> do
      print ("withJob Right -- calling notifyJob" :: Text)
      notifyJob queueName conn
      print ("notifyJob returned!" :: Text)
      continue
    Right (Just x) -> return $ Right x

{-|
Get the number of rows which are 'Enqueued'. This function runs
'getEnqueuedCountDB' in a 'ReadCommitted' transaction.
-}
getEnqueuedCount :: String -> Connection -> IO Int64
getEnqueuedCount queueName = runDBT (getEnqueuedCountDB queueName) ReadCommitted

{-|
Get the number of rows which are 'Failed'. This function runs
'getFailedCountDB' in a 'ReadCommitted' transaction.
-}
getFailedCount :: String -> Connection -> IO Int64
getFailedCount queueName = runDBT (getFailedCountDB queueName) ReadCommitted
