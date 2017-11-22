{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Database.PostgreSQL.Simple.QueueSpec
  ( spec
  ) where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Concurrent.Async
import Control.Monad
import Data.Aeson
import Data.Function
import Data.List
import Database.PostgreSQL.Simple.Queue
import Database.PostgreSQL.Simple.Queue.Migrate
import Test.Hspec (Spec, it)
import Test.Hspec.Expectations.Lifted
import Test.Hspec.DB
import Control.Monad.Catch
import Control.Monad.IO.Class
import Data.List.Split
import Data.Either

schemaName :: String
schemaName = "complicated_name"

spec :: Spec
spec = describeDB migrate "Database.Queue" $ do
  itDB "empty locks nothing" $
    (either throwM return =<< (withPayloadDB schemaName 8 return))
      `shouldReturn` Nothing

  itDB "empty gives count 0" $ do
    getEnqueuedCountDB schemaName `shouldReturn` 0
    getFailedCountDB schemaName `shouldReturn` 0

  it "enqueuesDB/withPayloadDB" $ \conn -> do
    runDB conn $ do
      payloadId <- enqueueDB schemaName $ String "Hello"
      getEnqueuedCountDB schemaName `shouldReturn` 1
      getFailedCountDB schemaName `shouldReturn` 0

      either throwM return =<< withPayloadDB schemaName 8 (\(Payload {..}) -> do
        pId `shouldBe` payloadId
        pValue `shouldBe` String "Hello"
        )

      -- read committed but still 0. I don't depend on this but I want to see if it
      -- stays like this.
      getEnqueuedCountDB schemaName `shouldReturn` 0
      getFailedCountDB schemaName `shouldReturn` 0

    runDB conn $ do
      getEnqueuedCountDB schemaName `shouldReturn` 0
      getFailedCountDB schemaName `shouldReturn` 0

  it "enqueuesDB/withPayloadDB/retries" $ \conn -> do
    runDB conn $ do
      void $ enqueueDB schemaName $ String "Hello"
      getEnqueuedCountDB schemaName `shouldReturn` 1
      getFailedCountDB schemaName `shouldReturn` 0

      xs <- replicateM 7 $ withPayloadDB schemaName 8 (\(Payload {..}) ->
          throwM $ userError "not enough tries"
        )

      all isLeft xs `shouldBe` True

      either throwM return =<< withPayloadDB schemaName 8 (\(Payload {..}) -> do
        pAttempts `shouldBe` 7
        pValue `shouldBe` String "Hello"
        )

    runDB conn $ do
      getEnqueuedCountDB schemaName `shouldReturn` 0
      getFailedCountDB schemaName `shouldReturn` 0

  it "enqueuesDB/withPayloadDB/fails" $ \conn -> do
    runDB conn $ do
      void $ enqueueDB schemaName $ String "Hello"
      getEnqueuedCountDB schemaName `shouldReturn` 1
      getFailedCountDB schemaName `shouldReturn` 0

      xs <- replicateM 2 $ withPayloadDB schemaName 1 (\(Payload {..}) ->
          throwM $ userError "not enough tries"
        )

      all isLeft xs `shouldBe` True

    runDB conn $ do
      getEnqueuedCountDB schemaName `shouldReturn` 0
      getFailedCountDB schemaName `shouldReturn` 1

  it "selects the oldest first" $ \conn -> do
    runDB conn $ do
      payloadId0 <- enqueueDB schemaName $ String "Hello"
      liftIO $ threadDelay 1000000

      payloadId1 <- enqueueDB schemaName $ String "Hi"

      getEnqueuedCountDB schemaName `shouldReturn` 2

      either throwM return =<< withPayloadDB schemaName 8 (\(Payload {..}) -> do
        pId `shouldBe` payloadId0
        pValue `shouldBe` String "Hello"
        )

      either throwM return =<< withPayloadDB schemaName 8 (\(Payload {..}) -> do
        pId `shouldBe` payloadId1
        pValue `shouldBe` String "Hi"
        )

    runDB conn $ getEnqueuedCountDB schemaName `shouldReturn` 0

  it "enqueues and dequeues concurrently withPayload" $ \testDB -> do
    let withPool' = withPool testDB
        elementCount = 1000 :: Int
        expected = [0 .. elementCount - 1]

    ref <- newTVarIO []

    loopThreads <- replicateM 10 $ async $ fix $ \next -> do
      lastCount <- either throwM return =<< withPool' (\c -> withPayload schemaName c 0 $ \(Payload {..}) -> do
        atomically $ do
          xs <- readTVar ref
          writeTVar ref $ pValue : xs
          return $ length xs + 1
        )

      when (lastCount < elementCount) next

    forM_ (chunksOf (elementCount `div` 40) expected) $ \xs -> forkIO $ void $ withPool' $ \c ->
       forM_ xs $ \i -> enqueue schemaName c $ toJSON i


    waitAnyCancel loopThreads
    xs <- atomically $ readTVar ref
    let Just decoded = mapM (decode . encode) xs
    sort decoded `shouldBe` sort expected

  --  threadDelay maxBound
