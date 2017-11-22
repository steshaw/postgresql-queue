{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}

{-| This module utilize PostgreSQL to implement a durable queue for efficently processing
    arbitrary payloads which can be represented as JSON.

    Typically a producer would enqueue a new payload as part of larger database
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
      -- Attempt get a payload or block until one is available
      payload <- 'lock' conn

      -- Perform application specifc parsing of the payload value
      case fromJSON $ 'pValue' payload of
        Success x -> sendEmail x -- Perform application specific processing
        Error err -> logErr err

      -- Remove the payload from future processing
      'dequeue' conn $ 'pId' payload
 @

 For a more complete example or a consumer, utilizing the provided
 'Database.PostgreSQL.Simple.Queue.Main.defaultMain', see
 'Database.PostgreSQL.Simple.Queue.Examples.EmailQueue.EmailQueue'.

This modules provides two flavors of functions, a DB API and an IO API.
Most operations are provided in both flavors, with the exception of 'lock'.
'lock' blocks and would not be that useful as part of a larger transaction
since it would keep the transaction open for a potentially long time. Although
both flavors are provided, in general one versions is more useful for typical
use cases.
-}
module Database.PostgreSQL.Simple.Queue
  ( -- * Types
    PayloadId (..)
  , State (..)
  , Payload (..)
  -- * DB API
  , dequeueDB
  , deleteDB
  , enqueueDB
  , withPayloadDB
  , getEnqueuedCountDB
  , getFailedCountDB
  -- * IO API
  , enqueue
  , withPayload
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
import Database.PostgreSQL.Simple.ToRow
import Database.PostgreSQL.Simple.Transaction
import Database.PostgreSQL.Transact
import Data.Monoid
import Data.String
import Control.Monad.IO.Class

import qualified Database.PostgreSQL.Simple as Simple

-------------------------------------------------------------------------------
---  Types
-------------------------------------------------------------------------------
newtype PayloadId = PayloadId { unPayloadId :: Int64 }
  deriving (Eq, Show, FromField, ToField)

instance FromRow PayloadId where
  fromRow = fromOnly <$> fromRow

instance ToRow PayloadId where
  toRow = toRow . Only

-- The fundamental record stored in the queue. The queue is a single table
-- and each row consists of a 'Payload'
data Payload = Payload
  { pId         :: PayloadId
  , pValue      :: Value
  -- ^ The JSON value of a payload
  , pState      :: State
  , pAttempts   :: Int
  , pCreatedAt  :: UTCTime
  } deriving (Show, Eq)

instance FromRow Payload where
  fromRow = Payload <$> field <*> field <*> field <*> field <*> field

-- | A 'Payload' can exist in three states in the queue, 'Enqueued',
--   and 'Dequeued'. A 'Payload' starts in the 'Enqueued' state and is locked
--   so some sort of process can occur with it, usually something in 'IO'.
--   Once the processing is complete, the `Payload' is moved the 'Dequeued'
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
enqueueDB :: String -> Value -> DB PayloadId
enqueueDB queueName value = enqueueWithDB queueName value 0

enqueueWithDB :: String -> Value -> Int -> DB PayloadId
enqueueWithDB queueName value attempts =
  fmap head $ query ([sql| NOTIFY |] <> " " <> notifyName queueName <> ";" <> [sql|
      INSERT INTO payloads (attempts, value)
      VALUES (?, ?)
      RETURNING id;
    |]) (attempts, value)

retryDB :: Payload -> DB Int64
retryDB Payload {..} =
  execute [sql|
    UPDATE payloads
    SET attempts = ?
    WHERE id = ?
  |] (pAttempts + 1, pId)

dequeueDB :: PayloadId -> DB Int64
dequeueDB pId =
  execute [sql| UPDATE payloads SET state = 'dequeued' WHERE id = ? |] pId

failedDB :: PayloadId -> DB Int64
failedDB pId =
  execute [sql| UPDATE payloads SET state = 'failed' WHERE id = ? |] pId

deleteDB :: PayloadId -> DB Int64
deleteDB pId =
  execute [sql| DELETE FROM payloads WHERE id = ? |] pId

{-|

Attempt to get a payload and process it. If the function passed in throws an exception
return it on the left side of the `Either`. Re-add the payload up to some passed in
maximum. Return `Nothing` is the `payloads` table is empty otherwise the result is an `a`
from the payload enqueue function.

-}
withPayloadDB :: String
              -- ^ queue name
              -> Int
              -- ^ retry count
              -> (Payload -> IO a)
              -- ^ payload processing function
              -> DB (Either SomeException (Maybe a))
withPayloadDB queueName retryCount f
  = query_ [sql|
      SELECT id, value, state, attempts, created_at
      FROM payloads
      WHERE state = 'enqueued'
      ORDER BY created_at ASC
      FOR UPDATE SKIP LOCKED
      LIMIT 1
    |]
 >>= \case
    [] -> return $ return Nothing
    [payload@Payload {..}] ->
      handle (onError payload) $ do
        result <- liftIO (f payload)
        deleteDB pId
        pure (Right . Just $ result)
    xs -> return
        $ Left
        $ toException
        $ userError
        $ "LIMIT is 1 but got more than one row: "
        ++ show xs
  where
    onError :: Payload -> SomeException -> DBT IO (Either SomeException a)
    onError payload@Payload {..} e = do
      -- Retry on failure up to retryCount. Set state to failed when retryCount reached.
      if (pAttempts < retryCount)
      then void $ retryDB payload
      else void $ failedDB pId
      pure $ Left e

-- | Get the number of rows in the 'Enqueued' state.
getEnqueuedCountDB :: String -> DB Int64
getEnqueuedCountDB queueName = fmap (fromOnly . head) $ query_ [sql|
    SELECT count(*)
    FROM payloads
    WHERE state = 'enqueued'
  |]

-- | Get the number of rows in the 'Failed' state.
getFailedCountDB :: String -> DB Int64
getFailedCountDB queueName = fmap (fromOnly . head) $ query_ [sql|
    SELECT count(*)
    FROM payloads
    WHERE state = 'failed'
  |]

-------------------------------------------------------------------------------
---  IO API
-------------------------------------------------------------------------------
{-| Enqueue a new JSON value into the queue. See 'enqueueDB' for a version
    which can be composed with other queries in a single transaction.
-}
enqueue :: String -> Connection -> Value -> IO PayloadId
enqueue queueName conn value = runDBT (enqueueDB queueName value) ReadCommitted conn

-- Block until a payload notification is fired. Fired during insertion.
notifyPayload :: String -> Connection -> IO ()
notifyPayload queueName conn = do
  Notification {..} <- getNotification conn
  unless (notificationChannel == notifyName queueName) $ notifyPayload queueName conn

{-| Return the oldest 'Payload' in the 'Enqueued' state or block until a
    payload arrives. This function utilizes PostgreSQL's LISTEN and NOTIFY
    functionality to avoid excessively polling of the DB while
    waiting for new payloads, without sacrificing promptness.
-}
withPayload :: String
            -> Connection
            -> Int
            -- ^ retry count
            -> (Payload -> IO a)
            -> IO (Either SomeException a)
withPayload queueName conn retryCount f = bracket_
  (Simple.execute_ conn $ "LISTEN " <> notifyName queueName)
  (Simple.execute_ conn $ "UNLISTEN " <> notifyName queueName)
  $ fix
  $ \continue -> runDBT (withPayloadDB queueName retryCount f) ReadCommitted conn
  >>= \case
    Left x -> return $ Left x
    Right Nothing -> do
      notifyPayload queueName conn
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
